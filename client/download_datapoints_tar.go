package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/draganm/datas3t/server/download"
)

const (
	// DefaultChunkSize is the default chunk size for downloads (5MB)
	DefaultChunkSize = 5 * 1024 * 1024
)

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
	req := &download.PreSignDownloadForDatapointsRequest{
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

	// 2. Create output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file %s: %w", outputPath, err)
	}
	defer outputFile.Close()

	// 3. Download and stitch segments together in order
	for i, segment := range resp.DownloadSegments {
		segmentData, err := c.downloadSegment(ctx, segment, opts.MaxRetries)
		if err != nil {
			return fmt.Errorf("failed to download segment %d: %w", i, err)
		}

		// Write segment data directly to output file
		_, err = outputFile.Write(segmentData)
		if err != nil {
			return fmt.Errorf("failed to write segment %d to output file: %w", i, err)
		}
	}

	// 4. Add TAR termination blocks (two 512-byte zero blocks) to make it a valid TAR file
	terminationBlocks := make([]byte, 1024) // Two 512-byte zero blocks
	_, err = outputFile.Write(terminationBlocks)
	if err != nil {
		return fmt.Errorf("failed to write TAR termination blocks: %w", err)
	}

	return nil
}

// downloadSegment downloads a single segment and returns its data
func (c *Client) downloadSegment(ctx context.Context, segment download.DownloadSegment, maxRetries int) ([]byte, error) {
	var segmentData []byte

	operation := func() error {
		req, err := http.NewRequestWithContext(ctx, "GET", segment.PresignedURL, nil)
		if err != nil {
			return err
		}

		// Set the Range header
		req.Header.Set("Range", segment.Range)

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

		// Read the segment data
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		// Store the data in the closure variable
		segmentData = data
		return nil
	}

	b := createDownloadBackoffConfig(maxRetries)
	err := backoff.Retry(operation, backoff.WithContext(b, ctx))
	if err != nil {
		return nil, fmt.Errorf("segment download failed: %w", err)
	}

	return segmentData, nil
}
