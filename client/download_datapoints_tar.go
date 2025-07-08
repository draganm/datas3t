package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"golang.org/x/sync/errgroup"
)

const (
	// DefaultChunkSize is the default chunk size for downloads (5MB)
	DefaultChunkSize = 5 * 1024 * 1024
)

// downloadChunk represents a chunk to be downloaded
type downloadChunk struct {
	URL        string
	StartByte  int64
	EndByte    int64
	FileOffset int64
	Size       int64
}

// DownloadOptions configures the download behavior
type DownloadOptions struct {
	MaxParallelism int   // Maximum number of concurrent downloads (default: 4)
	MaxRetries     int   // Maximum number of retry attempts per chunk (default: 3)
	ChunkSize      int64 // Size of each chunk in bytes (default: 5MB)
}

// DefaultDownloadOptions returns sensible default options
func DefaultDownloadOptions() *DownloadOptions {
	return &DownloadOptions{
		MaxParallelism: 4,
		MaxRetries:     3,
		ChunkSize:      DefaultChunkSize,
	}
}

// createDownloadBackoffConfig creates a backoff configuration with the specified max retries
func createDownloadBackoffConfig(maxRetries int) backoff.BackOff {
	expBackoff := backoff.NewExponentialBackOff()
	expBackoff.InitialInterval = 1 * time.Second
	expBackoff.MaxInterval = 30 * time.Second
	expBackoff.Multiplier = 2.0
	expBackoff.RandomizationFactor = 0.5

	return backoff.WithMaxRetries(expBackoff, uint64(maxRetries))
}

// parseSegmentSize parses the byte range to determine segment size
func (c *Client) parseSegmentSize(rangeHeader string) (int64, error) {
	// Range format: "bytes=start-end"
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, fmt.Errorf("invalid range format: %s", rangeHeader)
	}

	rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid range format: %s", rangeHeader)
	}

	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid start byte: %w", err)
	}

	end, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid end byte: %w", err)
	}

	return end - start + 1, nil
}

// createChunksForSegments splits all segments into smaller chunks for parallel download
func (c *Client) createChunksForSegments(segments []DownloadSegment, chunkSize int64) ([]downloadChunk, error) {
	var chunks []downloadChunk
	var currentFileOffset int64

	for _, segment := range segments {
		segmentSize, err := c.parseSegmentSize(segment.Range)
		if err != nil {
			return nil, fmt.Errorf("failed to parse segment range: %w", err)
		}

		// Parse the original range to get start and end bytes
		rangeStr := strings.TrimPrefix(segment.Range, "bytes=")
		parts := strings.Split(rangeStr, "-")
		segmentStartByte, _ := strconv.ParseInt(parts[0], 10, 64)
		segmentEndByte, _ := strconv.ParseInt(parts[1], 10, 64)

		// Split segment into chunks
		currentByte := segmentStartByte
		currentChunkFileOffset := currentFileOffset

		for currentByte <= segmentEndByte {
			chunkEndByte := currentByte + chunkSize - 1
			if chunkEndByte > segmentEndByte {
				chunkEndByte = segmentEndByte
			}

			actualChunkSize := chunkEndByte - currentByte + 1

			chunks = append(chunks, downloadChunk{
				URL:        segment.PresignedURL,
				StartByte:  currentByte,
				EndByte:    chunkEndByte,
				FileOffset: currentChunkFileOffset,
				Size:       actualChunkSize,
			})

			currentByte = chunkEndByte + 1
			currentChunkFileOffset += actualChunkSize
		}

		currentFileOffset += segmentSize
	}

	return chunks, nil
}

// DownloadDatapointsTar downloads a range of datapoints as a TAR file and saves it to the specified path
func (c *Client) DownloadDatapointsTar(ctx context.Context, datas3tName string, firstDatapoint, lastDatapoint uint64, outputPath string) error {
	return c.DownloadDatapointsTarWithOptions(ctx, datas3tName, firstDatapoint, lastDatapoint, outputPath, nil)
}

// DownloadDatapointsTarWithOptions downloads a range of datapoints as a TAR file with configurable options
func (c *Client) DownloadDatapointsTarWithOptions(ctx context.Context, datas3tName string, firstDatapoint, lastDatapoint uint64, outputPath string, opts *DownloadOptions) error {
	if opts == nil {
		opts = DefaultDownloadOptions()
	}

	// 1. Get presigned download URLs for the datapoints
	req := &PreSignDownloadForDatapointsRequest{
		Datas3tName:    datas3tName,
		FirstDatapoint: firstDatapoint,
		LastDatapoint:  lastDatapoint,
	}

	resp, err := c.PreSignDownloadForDatapoints(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to get presigned download URLs: %w", err)
	}

	if len(resp.DownloadSegments) == 0 {
		return fmt.Errorf("no download segments available for datapoints %d-%d in datas3t %s", firstDatapoint, lastDatapoint, datas3tName)
	}

	// 2. Create chunks from all segments
	chunks, err := c.createChunksForSegments(resp.DownloadSegments, opts.ChunkSize)
	if err != nil {
		return fmt.Errorf("failed to create chunks: %w", err)
	}

	// 3. Calculate total size for file pre-allocation
	var totalSize int64
	for _, chunk := range chunks {
		totalSize += chunk.Size
	}

	// 4. Create output file and pre-allocate space (including termination blocks)
	finalSize := totalSize + 1024 // Add space for TAR termination blocks
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", outputPath, err)
	}
	defer outputFile.Close()

	// Pre-allocate file size
	err = outputFile.Truncate(finalSize)
	if err != nil {
		return fmt.Errorf("failed to pre-allocate file size: %w", err)
	}

	// 5. Download all chunks in parallel using errgroup
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.MaxParallelism)

	for _, chunk := range chunks {
		chunk := chunk // capture loop variable

		g.Go(func() error {
			return c.downloadChunkWithRetry(ctx, chunk, outputFile, opts.MaxRetries)
		})
	}

	// Wait for all downloads to complete
	err = g.Wait()
	if err != nil {
		return fmt.Errorf("failed to download chunks: %w", err)
	}

	// 6. Add TAR termination blocks (two 512-byte zero blocks) at the end
	terminationBlocks := make([]byte, 1024) // Two 512-byte zero blocks
	_, err = outputFile.WriteAt(terminationBlocks, totalSize)
	if err != nil {
		return fmt.Errorf("failed to write TAR termination blocks: %w", err)
	}

	return nil
}

// downloadChunkWithRetry downloads a single chunk with exponential backoff retry
func (c *Client) downloadChunkWithRetry(ctx context.Context, chunk downloadChunk, outputFile *os.File, maxRetries int) error {
	operation := func() error {
		req, err := http.NewRequestWithContext(ctx, "GET", chunk.URL, nil)
		if err != nil {
			return err
		}

		// Set the Range header for this chunk
		rangeHeader := fmt.Sprintf("bytes=%d-%d", chunk.StartByte, chunk.EndByte)
		req.Header.Set("Range", rangeHeader)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// Check for successful response (206 Partial Content for range requests)
		if resp.StatusCode != http.StatusPartialContent {
			// Handle retryable errors
			if resp.StatusCode >= 500 || resp.StatusCode == 429 {
				return fmt.Errorf("HTTP %d", resp.StatusCode)
			}
			// Non-retryable error - wrap with Permanent to stop retrying
			body, _ := io.ReadAll(resp.Body)
			return backoff.Permanent(fmt.Errorf("unexpected HTTP status: %s, body: %s", resp.Status, string(body)))
		}

		// Read the chunk data
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		if int64(len(data)) != chunk.Size {
			return fmt.Errorf("expected %d bytes, got %d bytes", chunk.Size, len(data))
		}

		// Write to the correct position in the file using WriteAt
		_, err = outputFile.WriteAt(data, chunk.FileOffset)
		if err != nil {
			return backoff.Permanent(fmt.Errorf("failed to write chunk data: %w", err))
		}

		return nil
	}

	b := createDownloadBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return fmt.Errorf("chunk download failed (offset %d, size %d): %w", chunk.FileOffset, chunk.Size, err)
	}

	return nil
}
