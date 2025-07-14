package client

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/draganm/datas3t/tarindex"
	"golang.org/x/sync/errgroup"
)

// Additional progress phases for aggregation
const (
	PhaseStartingAggregate   ProgressPhase = "starting_aggregate"
	PhaseDownloadingSources  ProgressPhase = "downloading_sources"
	PhaseMergingTars         ProgressPhase = "merging_tars"
	PhaseUploadingAggregate  ProgressPhase = "uploading_aggregate"
	PhaseCompletingAggregate ProgressPhase = "completing_aggregate"
)

// AggregateOptions configures the aggregation behavior
type AggregateOptions struct {
	MaxParallelism   int              // Maximum number of concurrent downloads/uploads (default: 4)
	MaxRetries       int              // Maximum number of retry attempts per operation (default: 3)
	ProgressCallback ProgressCallback // Optional progress callback
	TempDir          string           // Directory for temporary files (default: os.TempDir())
}

// DefaultAggregateOptions returns sensible default options for aggregation
func DefaultAggregateOptions() *AggregateOptions {
	return &AggregateOptions{
		MaxParallelism: 4,
		MaxRetries:     3,
		TempDir:        os.TempDir(),
	}
}

// AggregateDataRanges combines multiple existing dataranges into a single aggregate datarange
func (c *Client) AggregateDataRanges(ctx context.Context, datas3tName string, firstDatapointIndex, lastDatapointIndex uint64, opts *AggregateOptions) (err error) {
	if opts == nil {
		opts = DefaultAggregateOptions()
	}

	// Set default temp directory if not specified
	if opts.TempDir == "" {
		opts.TempDir = os.TempDir()
	}

	// Phase 1: Start aggregate operation
	startReq := &StartAggregateRequest{
		Datas3tName:         datas3tName,
		FirstDatapointIndex: firstDatapointIndex,
		LastDatapointIndex:  lastDatapointIndex,
	}

	// Create progress tracker - we'll estimate total size after getting source info
	tracker := newProgressTracker(opts.ProgressCallback, 0)
	tracker.reportProgress(PhaseStartingAggregate, "Starting aggregate operation", 0)

	aggregateResp, err := c.StartAggregate(ctx, startReq)
	if err != nil {
		return fmt.Errorf("failed to start aggregate: %w", err)
	}

	// Set up cleanup on failure
	defer func() {
		if err != nil {
			// Best effort cleanup - cancel the aggregate if it was started
			cancelReq := &CancelAggregateRequest{
				AggregateUploadID: aggregateResp.AggregateUploadID,
			}
			c.CancelAggregate(context.Background(), cancelReq) // Use background context for cleanup
		}
	}()

	// Calculate total estimated size from source dataranges
	var totalSourceSize int64
	for _, source := range aggregateResp.SourceDatarangeDownloadURLs {
		totalSourceSize += source.SizeBytes
	}
	// Estimate total bytes for progress tracking: download + upload
	// We use source size * 2 as an estimate (download + upload approximately same size)
	tracker.totalBytes = totalSourceSize * 2
	tracker.nextStep()

	// Phase 2: Download all source dataranges
	tracker.reportProgress(PhaseDownloadingSources, "Downloading source dataranges", 0)
	sourceData, err := c.downloadSourceDataranges(ctx, aggregateResp.SourceDatarangeDownloadURLs, opts, tracker)
	if err != nil {
		return fmt.Errorf("failed to download source dataranges: %w", err)
	}
	tracker.nextStep()

	// Phase 3: Merge TAR files and create aggregate index
	tracker.reportProgress(PhaseMergingTars, "Merging TAR files", 0)
	aggregatedTarFile, aggregatedIndex, err := c.mergeTarFiles(sourceData, opts.TempDir, tracker)
	if err != nil {
		return fmt.Errorf("failed to merge TAR files: %w", err)
	}
	defer os.Remove(aggregatedTarFile.Name()) // Clean up temporary file
	defer aggregatedTarFile.Close()

	// Update total bytes with actual aggregated file size for more accurate progress
	if fileInfo, err := aggregatedTarFile.Stat(); err == nil {
		actualUploadSize := fileInfo.Size()
		// Adjust total: already downloaded source size + actual upload size + index size estimate
		tracker.totalBytes = totalSourceSize + actualUploadSize + int64(len(aggregatedIndex))
	}

	tracker.nextStep()

	// Phase 4: Upload aggregated data
	tracker.reportProgress(PhaseUploadingAggregate, "Uploading aggregated data", 0)
	var uploadIDs []string
	if aggregateResp.UseDirectPut {
		// Direct PUT for small aggregates
		err = c.uploadAggregateDataDirectPutFromFile(ctx, aggregateResp.PresignedDataPutURL, aggregatedTarFile, opts.MaxRetries, tracker)
		if err != nil {
			return fmt.Errorf("failed to upload aggregate data: %w", err)
		}
	} else {
		// Multipart upload for large aggregates
		uploadIDs, err = c.uploadAggregateDataMultipartFromFile(ctx, aggregateResp.PresignedMultipartUploadPutURLs, aggregatedTarFile, opts, tracker)
		if err != nil {
			return fmt.Errorf("failed to upload aggregate data: %w", err)
		}
	}

	// Upload aggregate index
	err = uploadIndexWithRetry(ctx, aggregateResp.PresignedIndexPutURL, aggregatedIndex, opts.MaxRetries, tracker)
	if err != nil {
		return fmt.Errorf("failed to upload aggregate index: %w", err)
	}
	tracker.nextStep()

	// Phase 5: Complete aggregate
	tracker.reportProgress(PhaseCompletingAggregate, "Completing aggregate", 0)
	completeReq := &CompleteAggregateRequest{
		AggregateUploadID: aggregateResp.AggregateUploadID,
		UploadIDs:         uploadIDs,
	}

	err = c.CompleteAggregate(ctx, completeReq)
	if err != nil {
		return fmt.Errorf("failed to complete aggregate: %w", err)
	}
	tracker.nextStep()

	// Final progress report
	tracker.reportProgress(PhaseCompletingAggregate, "Aggregate completed successfully", 0)
	return nil
}

// sourceDataInfo holds downloaded source datarange information
type sourceDataInfo struct {
	DatarangeID  int64
	MinDatapoint int64
	MaxDatapoint int64
	Data         []byte
	Index        []byte
}

// downloadSourceDataranges downloads all source dataranges in parallel
func (c *Client) downloadSourceDataranges(ctx context.Context, sources []DatarangeDownloadURL, opts *AggregateOptions, tracker *progressTracker) ([]sourceDataInfo, error) {
	results := make([]sourceDataInfo, len(sources))

	// Use errgroup with parallelism limit
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.MaxParallelism)

	var mu sync.Mutex
	var completedBytes int64

	for i, source := range sources {
		i, source := i, source // capture loop variables
		g.Go(func() error {
			// Download data
			dataBytes, err := c.downloadWithRetry(ctx, source.PresignedDataURL, opts.MaxRetries)
			if err != nil {
				return fmt.Errorf("failed to download data for datarange %d: %w", source.DatarangeID, err)
			}

			// Download index
			indexBytes, err := c.downloadWithRetry(ctx, source.PresignedIndexURL, opts.MaxRetries)
			if err != nil {
				return fmt.Errorf("failed to download index for datarange %d: %w", source.DatarangeID, err)
			}

			results[i] = sourceDataInfo{
				DatarangeID:  source.DatarangeID,
				MinDatapoint: source.MinDatapointKey,
				MaxDatapoint: source.MaxDatapointKey,
				Data:         dataBytes,
				Index:        indexBytes,
			}

			// Update progress
			mu.Lock()
			completedBytes += source.SizeBytes
			stepInfo := fmt.Sprintf("Downloaded %d of %d dataranges", i+1, len(sources))
			tracker.reportProgress(PhaseDownloadingSources, stepInfo, source.SizeBytes)
			mu.Unlock()

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, fmt.Errorf("failed to download source dataranges: %w", err)
	}

	// Sort results by min datapoint to ensure correct order
	sort.Slice(results, func(i, j int) bool {
		return results[i].MinDatapoint < results[j].MinDatapoint
	})

	return results, nil
}

// downloadWithRetry downloads data from a URL with retry logic
func (c *Client) downloadWithRetry(ctx context.Context, url string, maxRetries int) ([]byte, error) {
	var data []byte

	operation := func() error {
		resp, err := http.Get(url)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			data, err = io.ReadAll(resp.Body)
			return err
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		// Non-retryable error
		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return nil, fmt.Errorf("download failed: %w", err)
	}

	return data, nil
}

// mergeTarFiles combines multiple TAR files into a single TAR with continuous datapoint indices
func (c *Client) mergeTarFiles(sources []sourceDataInfo, tempDir string, tracker *progressTracker) (*os.File, []byte, error) {
	// Create temporary file for aggregated tar
	tempFile, err := os.CreateTemp(tempDir, "aggregate-*.tar")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create temporary file: %w", err)
	}

	tw := tar.NewWriter(tempFile)

	currentDatapoint := sources[0].MinDatapoint

	// Process each source TAR file
	for _, source := range sources {
		reader := bytes.NewReader(source.Data)
		tr := tar.NewReader(reader)

		for {
			header, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read tar entry from datarange %d: %w", source.DatarangeID, err)
			}

			// Skip directories and other non-regular files
			if header.Typeflag != tar.TypeReg {
				continue
			}

			// Extract original datapoint key from filename for validation
			_, err = extractDatapointKeyFromFileName(header.Name)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid filename in source TAR: %w", err)
			}

			// Create new filename with continuous datapoint index
			newFilename := fmt.Sprintf("%020d.txt", currentDatapoint)
			header.Name = newFilename

			// Write header to aggregate TAR
			err = tw.WriteHeader(header)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to write tar header: %w", err)
			}

			// Copy file content
			_, err = io.Copy(tw, tr)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to copy file content: %w", err)
			}

			currentDatapoint++
		}
	}

	// Close the TAR writer
	err = tw.Close()
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, nil, fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Seek to beginning for index generation
	_, err = tempFile.Seek(0, 0)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, nil, fmt.Errorf("failed to seek to beginning of file: %w", err)
	}

	// Generate index for the aggregated TAR
	indexData, err := tarindex.IndexTar(tempFile)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, nil, fmt.Errorf("failed to create aggregate index: %w", err)
	}

	// Seek back to beginning for upload
	_, err = tempFile.Seek(0, 0)
	if err != nil {
		tempFile.Close()
		os.Remove(tempFile.Name())
		return nil, nil, fmt.Errorf("failed to seek to beginning of file: %w", err)
	}

	tracker.reportProgress(PhaseMergingTars, "TAR merging completed", 0)
	return tempFile, indexData, nil
}

// uploadAggregateDataDirectPutFromFile uploads aggregate data from a file using direct PUT
func (c *Client) uploadAggregateDataDirectPutFromFile(ctx context.Context, url string, file *os.File, maxRetries int, tracker *progressTracker) error {
	operation := func() error {
		// Get file size for content length
		fileInfo, err := file.Stat()
		if err != nil {
			return err
		}

		// Seek to beginning
		_, err = file.Seek(0, 0)
		if err != nil {
			return err
		}

		req, err := http.NewRequestWithContext(ctx, "PUT", url, file)
		if err != nil {
			return err
		}
		req.ContentLength = fileInfo.Size()

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			tracker.reportProgress(PhaseUploadingAggregate, "Direct upload completed", fileInfo.Size())
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	return backoff.Retry(operation, backoff.WithContext(b, ctx))
}

// uploadAggregateDataMultipartFromFile uploads aggregate data from a file using multipart upload
func (c *Client) uploadAggregateDataMultipartFromFile(ctx context.Context, urls []string, file *os.File, opts *AggregateOptions, tracker *progressTracker) ([]string, error) {
	numParts := len(urls)
	if numParts == 0 {
		return nil, fmt.Errorf("no upload URLs provided")
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// Calculate part sizes (minimum 5MB except for last part)
	const minPartSize = 5 * 1024 * 1024
	standardPartSize := int64(minPartSize)

	// Use errgroup with parallelism limit
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.MaxParallelism)

	etags := make([]string, numParts)
	for i, url := range urls {
		i, url := i, url // capture loop variables
		g.Go(func() error {
			// Calculate this part's boundaries
			offset := int64(i) * standardPartSize
			partSize := standardPartSize

			// Handle the last part - it gets all remaining data
			if i == numParts-1 {
				partSize = fileSize - offset
			}

			// Safety check
			if offset+partSize > fileSize {
				partSize = fileSize - offset
			}

			// Upload chunk with retry
			etag, err := c.uploadChunkFromFileWithRetry(ctx, url, file, offset, partSize, opts.MaxRetries, tracker, i+1, numParts)
			if err != nil {
				return fmt.Errorf("failed to upload part %d: %w", i+1, err)
			}
			etags[i] = etag
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		return nil, fmt.Errorf("multipart upload failed: %w", err)
	}

	return etags, nil
}

// uploadChunkFromFileWithRetry uploads a single chunk from a file with retry logic for aggregate data
func (c *Client) uploadChunkFromFileWithRetry(ctx context.Context, url string, file *os.File, offset, size int64, maxRetries int, tracker *progressTracker, partNum, totalParts int) (string, error) {
	var etag string

	operation := func() error {
		// Create a section reader for this chunk
		sectionReader := io.NewSectionReader(file, offset, size)

		req, err := http.NewRequestWithContext(ctx, "PUT", url, sectionReader)
		if err != nil {
			return err
		}
		req.ContentLength = size

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			etag = resp.Header.Get("ETag")
			stepInfo := fmt.Sprintf("Uploading aggregate part %d of %d", partNum, totalParts)
			tracker.reportProgress(PhaseUploadingAggregate, stepInfo, size)
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return "", fmt.Errorf("chunk upload failed: %w", err)
	}

	return etag, nil
}
