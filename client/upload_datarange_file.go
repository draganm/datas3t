package client

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/draganm/datas3t/tarindex"
	"golang.org/x/sync/errgroup"
)

// ProgressPhase represents the current phase of the upload process
type ProgressPhase string

const (
	PhaseAnalyzing      ProgressPhase = "analyzing"
	PhaseIndexing       ProgressPhase = "indexing"
	PhaseStarting       ProgressPhase = "starting"
	PhaseUploading      ProgressPhase = "uploading"
	PhaseUploadingIndex ProgressPhase = "uploading_index"
	PhaseCompleting     ProgressPhase = "completing"
)

// ProgressInfo contains information about the upload progress
type ProgressInfo struct {
	Phase           ProgressPhase
	TotalBytes      int64
	CompletedBytes  int64
	PercentComplete float64
	CurrentStep     string
	TotalSteps      int
	CompletedSteps  int
	EstimatedETA    time.Duration
	Speed           float64 // bytes per second
	StartTime       time.Time
}

// ProgressCallback is called to report upload progress
type ProgressCallback func(info ProgressInfo)

// UploadOptions configures the upload behavior
type UploadOptions struct {
	MaxParallelism   int              // Maximum number of concurrent uploads (default: 4)
	MaxRetries       int              // Maximum number of retry attempts per chunk (default: 3)
	ProgressCallback ProgressCallback // Optional progress callback
}

// DefaultUploadOptions returns sensible default options
func DefaultUploadOptions() *UploadOptions {
	return &UploadOptions{
		MaxParallelism: 4,
		MaxRetries:     3,
	}
}

// createBackoffConfig creates a backoff configuration with the specified max retries
func createBackoffConfig(maxRetries int) backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Second
	expBackoff.MaxInterval = 30 * time.Second
	expBackoff.Multiplier = 2.0
	expBackoff.RandomizationFactor = 0.5

	return backoff.WithMaxRetries(expBackoff, uint64(maxRetries))
}

// progressTracker helps track upload progress across phases
type progressTracker struct {
	callback       ProgressCallback
	totalBytes     int64
	completedBytes int64
	startTime      time.Time
	totalSteps     int
	completedSteps int
}

// newProgressTracker creates a new progress tracker
func newProgressTracker(callback ProgressCallback, totalBytes int64) *progressTracker {
	return &progressTracker{
		callback:   callback,
		totalBytes: totalBytes,
		startTime:  time.Now(),
		totalSteps: 6, // Total phases: analyze, index, start, upload, upload_index, complete
	}
}

// reportProgress reports current progress
func (pt *progressTracker) reportProgress(phase ProgressPhase, currentStep string, additionalBytes int64) {
	if pt.callback == nil {
		return
	}

	pt.completedBytes += additionalBytes

	var percentComplete float64
	if pt.totalBytes > 0 {
		percentComplete = float64(pt.completedBytes) / float64(pt.totalBytes) * 100
	}

	elapsed := time.Since(pt.startTime)
	var speed float64
	var eta time.Duration

	if elapsed > 0 && pt.completedBytes > 0 {
		speed = float64(pt.completedBytes) / elapsed.Seconds()
		if speed > 0 {
			remainingBytes := pt.totalBytes - pt.completedBytes
			eta = time.Duration(float64(remainingBytes) / speed * float64(time.Second))
		}
	}

	info := ProgressInfo{
		Phase:           phase,
		TotalBytes:      pt.totalBytes,
		CompletedBytes:  pt.completedBytes,
		PercentComplete: percentComplete,
		CurrentStep:     currentStep,
		TotalSteps:      pt.totalSteps,
		CompletedSteps:  pt.completedSteps,
		EstimatedETA:    eta,
		Speed:           speed,
		StartTime:       pt.startTime,
	}

	pt.callback(info)
}

// nextStep advances to the next step
func (pt *progressTracker) nextStep() {
	pt.completedSteps++
}

func (c *Client) UploadDataRangeFile(ctx context.Context, datas3tName string, file io.ReaderAt, size int64, opts *UploadOptions) error {
	if opts == nil {
		opts = DefaultUploadOptions()
	}

	// Create progress tracker
	tracker := newProgressTracker(opts.ProgressCallback, size)

	// Phase 1: Analyze TAR file to extract datapoint information
	tracker.reportProgress(PhaseAnalyzing, "Analyzing TAR file structure", 0)
	tarInfo, err := analyzeTarFile(file, size)
	if err != nil {
		return fmt.Errorf("failed to analyze tar file: %w", err)
	}
	tracker.nextStep()

	// Phase 2: Generate TAR index
	tracker.reportProgress(PhaseIndexing, "Generating TAR index", 0)
	indexData, err := generateTarIndex(file, size)
	if err != nil {
		return fmt.Errorf("failed to generate tar index: %w", err)
	}
	tracker.nextStep()

	// Phase 3: Start upload
	tracker.reportProgress(PhaseStarting, "Starting upload session", 0)
	uploadReq := &UploadDatarangeRequest{
		Datas3tName:         datas3tName,
		DataSize:            uint64(size),
		NumberOfDatapoints:  uint64(tarInfo.NumDatapoints),
		FirstDatapointIndex: uint64(tarInfo.FirstDatapointIndex),
	}

	uploadResp, err := c.StartDatarangeUpload(ctx, uploadReq)
	if err != nil {
		return fmt.Errorf("failed to start upload: %w", err)
	}
	tracker.nextStep()

	// Phase 4: Upload data
	tracker.reportProgress(PhaseUploading, "Uploading data", 0)
	var uploadIDs []string
	if uploadResp.UseDirectPut {
		// Direct PUT for small files
		err = uploadDataDirectPut(ctx, uploadResp.PresignedDataPutURL, file, size, opts.MaxRetries, tracker)
		if err != nil {
			// Cancel upload on failure
			cancelReq := &CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}
			c.CancelDatarangeUpload(ctx, cancelReq) // Best effort, ignore error
			return fmt.Errorf("failed to upload data: %w", err)
		}
	} else {
		// Multipart upload for large files
		uploadIDs, err = uploadDataMultipart(ctx, uploadResp.PresignedMultipartUploadPutURLs, file, size, opts, tracker)
		if err != nil {
			// Cancel upload on failure
			cancelReq := &CancelUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}
			c.CancelDatarangeUpload(ctx, cancelReq) // Best effort, ignore error
			return fmt.Errorf("failed to upload data: %w", err)
		}
	}
	tracker.nextStep()

	// Phase 5: Upload index
	tracker.reportProgress(PhaseUploadingIndex, "Uploading index", 0)
	err = uploadIndexWithRetry(ctx, uploadResp.PresignedIndexPutURL, indexData, opts.MaxRetries, tracker)
	if err != nil {
		// Cancel upload on failure
		cancelReq := &CancelUploadRequest{
			DatarangeUploadID: uploadResp.DatarangeID,
		}
		c.CancelDatarangeUpload(ctx, cancelReq) // Best effort, ignore error
		return fmt.Errorf("failed to upload index: %w", err)
	}
	tracker.nextStep()

	// Phase 6: Complete upload
	tracker.reportProgress(PhaseCompleting, "Completing upload", 0)
	completeReq := &CompleteUploadRequest{
		DatarangeUploadID: uploadResp.DatarangeID,
		UploadIDs:         uploadIDs, // ETags for multipart, empty for direct PUT
	}

	err = c.CompleteDatarangeUpload(ctx, completeReq)
	if err != nil {
		return fmt.Errorf("failed to complete upload: %w", err)
	}
	tracker.nextStep()

	// Final progress report
	tracker.reportProgress(PhaseCompleting, "Upload completed successfully", 0)
	return nil
}

// TarInfo contains metadata extracted from analyzing the TAR file
type TarInfo struct {
	FirstDatapointIndex int64
	NumDatapoints       int
}

// analyzeTarFile reads the TAR file to extract datapoint information and validate naming convention
func analyzeTarFile(file io.ReaderAt, size int64) (*TarInfo, error) {
	reader := io.NewSectionReader(file, 0, size)
	tr := tar.NewReader(reader)

	var datapointKeys []int64

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar entry: %w", err)
		}

		// Skip directories and other non-regular files
		if header.Typeflag != tar.TypeReg {
			continue
		}

		// Validate filename format and extract datapoint key
		datapointKey, err := extractDatapointKeyFromFileName(header.Name)
		if err != nil {
			return nil, fmt.Errorf("invalid filename '%s': %w", header.Name, err)
		}

		datapointKeys = append(datapointKeys, datapointKey)

		// Skip file content
		_, err = io.Copy(io.Discard, tr)
		if err != nil {
			return nil, fmt.Errorf("failed to skip file content: %w", err)
		}
	}

	if len(datapointKeys) == 0 {
		return nil, fmt.Errorf("no valid datapoint files found in tar archive")
	}

	// Sort the keys to ensure they're in order
	sort.Slice(datapointKeys, func(i, j int) bool {
		return datapointKeys[i] < datapointKeys[j]
	})

	// Validate there are no gaps in the sequence
	firstKey := datapointKeys[0]
	for i, key := range datapointKeys {
		expectedKey := firstKey + int64(i)
		if key != expectedKey {
			return nil, fmt.Errorf("gap in datapoint sequence: expected %d, found %d", expectedKey, key)
		}
	}

	return &TarInfo{
		FirstDatapointIndex: firstKey,
		NumDatapoints:       len(datapointKeys),
	}, nil
}

// isValidFileName checks if the filename matches the pattern %020d.<extension>
func isValidFileName(fileName string) bool {
	// Find the first dot
	dotIndex := strings.Index(fileName, ".")
	if dotIndex == -1 || dotIndex == 0 {
		return false // No extension or starts with dot
	}

	// Check if the part before the extension is exactly 20 digits
	namepart := fileName[:dotIndex]
	if len(namepart) != 20 {
		return false
	}

	// Check if all characters are digits
	for _, r := range namepart {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

// extractDatapointKeyFromFileName extracts the numeric datapoint key from a filename
func extractDatapointKeyFromFileName(fileName string) (int64, error) {
	if !isValidFileName(fileName) {
		return 0, fmt.Errorf("filename doesn't match pattern %%020d.<extension>")
	}

	dotIndex := strings.Index(fileName, ".")
	namepart := fileName[:dotIndex]

	key, err := strconv.ParseInt(namepart, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse numeric part: %w", err)
	}

	return key, nil
}

// generateTarIndex creates a tar index from the file
func generateTarIndex(file io.ReaderAt, size int64) ([]byte, error) {
	reader := io.NewSectionReader(file, 0, size)
	return tarindex.IndexTar(reader)
}

// uploadDataDirectPut handles direct PUT upload for small files
func uploadDataDirectPut(ctx context.Context, url string, file io.ReaderAt, size int64, maxRetries int, tracker *progressTracker) error {
	operation := func() error {
		// Create reader for entire file
		reader := io.NewSectionReader(file, 0, size)

		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "PUT", url, reader)
		if err != nil {
			return err
		}
		req.ContentLength = size

		// Execute request
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			// Report progress for the entire file
			tracker.reportProgress(PhaseUploading, "Direct upload completed", size)
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		// Non-retryable error - wrap with Permanent to stop retrying
		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return fmt.Errorf("direct PUT failed: %w", err)
	}

	return nil
}

// uploadDataMultipart handles multipart upload for large files
func uploadDataMultipart(ctx context.Context, urls []string, file io.ReaderAt, size int64, opts *UploadOptions, tracker *progressTracker) ([]string, error) {
	numParts := len(urls)
	if numParts == 0 {
		return nil, fmt.Errorf("no upload URLs provided")
	}

	// Calculate part sizes respecting S3 minimum part size requirements
	// All parts except the last must be at least 5MB
	const minPartSize = 5 * 1024 * 1024 // 5MB

	// Use the minimum part size for all parts except the last
	// The last part can be smaller and will contain any remainder
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
				partSize = size - offset
			}

			// Safety check: ensure we don't exceed file boundaries
			if offset+partSize > size {
				partSize = size - offset
			}

			// Upload chunk with retry
			etag, err := uploadChunkWithRetry(ctx, url, file, offset, partSize, opts.MaxRetries, tracker, i+1, numParts)
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

// uploadChunkWithRetry uploads a single chunk with exponential backoff retry
func uploadChunkWithRetry(ctx context.Context, url string, file io.ReaderAt, offset, size int64, maxRetries int, tracker *progressTracker, partNum, totalParts int) (string, error) {
	var etag string

	operation := func() error {
		// Create section reader for this chunk
		reader := io.NewSectionReader(file, offset, size)

		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "PUT", url, reader)
		if err != nil {
			return err
		}
		req.ContentLength = size

		// Execute request
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			etag = resp.Header.Get("ETag")
			// Report progress for this chunk
			stepInfo := fmt.Sprintf("Uploading part %d of %d", partNum, totalParts)
			tracker.reportProgress(PhaseUploading, stepInfo, size)
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		// Non-retryable error - wrap with Permanent to stop retrying
		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return "", fmt.Errorf("chunk upload failed: %w", err)
	}

	return etag, nil
}

// uploadIndexWithRetry uploads the tar index with retry logic
func uploadIndexWithRetry(ctx context.Context, url string, indexData []byte, maxRetries int, tracker *progressTracker) error {
	operation := func() error {
		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewReader(indexData))
		if err != nil {
			return err
		}
		req.ContentLength = int64(len(indexData))

		// Execute request
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			// Report progress for index upload
			tracker.reportProgress(PhaseUploadingIndex, "Index upload completed", 0)
			return nil
		}

		// Handle retryable errors
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			return fmt.Errorf("HTTP %d", resp.StatusCode)
		}

		// Non-retryable error - wrap with Permanent to stop retrying
		return backoff.Permanent(fmt.Errorf("HTTP %d", resp.StatusCode))
	}

	b := createBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return fmt.Errorf("index upload failed: %w", err)
	}

	return nil
}
