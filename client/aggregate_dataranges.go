package client

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/draganm/datas3t/server/dataranges"
	"github.com/draganm/datas3t/tarindex"
	"golang.org/x/sync/errgroup"
)

// Additional progress phases for aggregation
const (
	PhaseStartingAggregate   ProgressPhase = "starting_aggregate"
	PhaseDownloadingSources  ProgressPhase = "downloading_sources"
	PhaseMergingTars        ProgressPhase = "merging_tars"
	PhaseUploadingAggregate ProgressPhase = "uploading_aggregate"
	PhaseCompletingAggregate ProgressPhase = "completing_aggregate"
)

// AggregateOptions configures the aggregation behavior
type AggregateOptions struct {
	MaxParallelism   int              // Maximum number of concurrent downloads/uploads (default: 4)
	MaxRetries       int              // Maximum number of retry attempts per operation (default: 3)
	ProgressCallback ProgressCallback // Optional progress callback
}

// DefaultAggregateOptions returns sensible default options for aggregation
func DefaultAggregateOptions() *AggregateOptions {
	return &AggregateOptions{
		MaxParallelism: 4,
		MaxRetries:     3,
	}
}

// AggregateDataRanges combines multiple existing dataranges into a single aggregate datarange
func (c *Client) AggregateDataRanges(ctx context.Context, datas3tName string, firstDatapointIndex, lastDatapointIndex uint64, opts *AggregateOptions) error {
	if opts == nil {
		opts = DefaultAggregateOptions()
	}

	// Phase 1: Start aggregate operation
	startReq := &dataranges.StartAggregateRequest{
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
			cancelReq := &dataranges.CancelAggregateRequest{
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
	tracker.totalBytes = totalSourceSize
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
	aggregatedTar, aggregatedIndex, err := c.mergeTarFiles(sourceData, tracker)
	if err != nil {
		return fmt.Errorf("failed to merge TAR files: %w", err)
	}
	tracker.nextStep()

	// Phase 4: Upload aggregated data
	tracker.reportProgress(PhaseUploadingAggregate, "Uploading aggregated data", 0)
	var uploadIDs []string
	if aggregateResp.UseDirectPut {
		// Direct PUT for small aggregates
		err = c.uploadAggregateDataDirectPut(ctx, aggregateResp.PresignedDataPutURL, aggregatedTar, opts.MaxRetries, tracker)
		if err != nil {
			return fmt.Errorf("failed to upload aggregate data: %w", err)
		}
	} else {
		// Multipart upload for large aggregates
		uploadIDs, err = c.uploadAggregateDataMultipart(ctx, aggregateResp.PresignedMultipartUploadPutURLs, aggregatedTar, opts, tracker)
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
	completeReq := &dataranges.CompleteAggregateRequest{
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
	DatarangeID    int64
	MinDatapoint   int64
	MaxDatapoint   int64
	Data           []byte
	Index          []byte
}

// downloadSourceDataranges downloads all source dataranges in parallel
func (c *Client) downloadSourceDataranges(ctx context.Context, sources []dataranges.DatarangeDownloadURL, opts *AggregateOptions, tracker *progressTracker) ([]sourceDataInfo, error) {
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
func (c *Client) mergeTarFiles(sources []sourceDataInfo, tracker *progressTracker) ([]byte, []byte, error) {
	var aggregatedTar bytes.Buffer
	tw := tar.NewWriter(&aggregatedTar)

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
	err := tw.Close()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to close tar writer: %w", err)
	}

	// Generate index for the aggregated TAR
	aggregatedData := aggregatedTar.Bytes()
	reader := bytes.NewReader(aggregatedData)
	indexData, err := tarindex.IndexTar(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create aggregate index: %w", err)
	}

	tracker.reportProgress(PhaseMergingTars, "TAR merging completed", 0)
	return aggregatedData, indexData, nil
}

// uploadAggregateDataDirectPut uploads aggregate data using direct PUT
func (c *Client) uploadAggregateDataDirectPut(ctx context.Context, url string, data []byte, maxRetries int, tracker *progressTracker) error {
	operation := func() error {
		req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.ContentLength = int64(len(data))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			tracker.reportProgress(PhaseUploadingAggregate, "Direct upload completed", int64(len(data)))
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

// uploadAggregateDataMultipart uploads aggregate data using multipart upload
func (c *Client) uploadAggregateDataMultipart(ctx context.Context, urls []string, data []byte, opts *AggregateOptions, tracker *progressTracker) ([]string, error) {
	numParts := len(urls)
	if numParts == 0 {
		return nil, fmt.Errorf("no upload URLs provided")
	}

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
				partSize = int64(len(data)) - offset
			}

			// Safety check
			if offset+partSize > int64(len(data)) {
				partSize = int64(len(data)) - offset
			}

			// Extract chunk data
			chunkData := data[offset : offset+partSize]

			// Upload chunk with retry
			etag, err := c.uploadChunkWithRetry(ctx, url, chunkData, opts.MaxRetries, tracker, i+1, numParts)
			if err != nil {
				return fmt.Errorf("failed to upload part %d: %w", i+1, err)
			}
			etags[i] = etag
			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return nil, fmt.Errorf("multipart upload failed: %w", err)
	}

	return etags, nil
}

// uploadChunkWithRetry uploads a single chunk with retry logic for aggregate data
func (c *Client) uploadChunkWithRetry(ctx context.Context, url string, data []byte, maxRetries int, tracker *progressTracker, partNum, totalParts int) (string, error) {
	var etag string

	operation := func() error {
		req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(data))
		if err != nil {
			return err
		}
		req.ContentLength = int64(len(data))

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			etag = resp.Header.Get("ETag")
			stepInfo := fmt.Sprintf("Uploading aggregate part %d of %d", partNum, totalParts)
			tracker.reportProgress(PhaseUploadingAggregate, stepInfo, int64(len(data)))
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