package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"sync"

	"golang.org/x/sync/errgroup"
)

// ProgressCallback is a function type that reports progress during multipart uploads
type ProgressCallback func(partNumber, totalParts int, partBytes, uploadedBytes, totalBytes int64)

// InitiateMultipartUpload starts a new multipart upload for a dataset
func (c *Client) InitiateMultipartUpload(ctx context.Context, datasetID string) (*InitiateMultipartUploadResponse, error) {
	endpointPath := fmt.Sprintf("/api/v1/datas3t/%s/multipart", datasetID)
	endpointURL, err := url.Parse(endpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint path: %w", err)
	}

	requestURL := c.baseURL.ResolveReference(endpointURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("server returned non-OK status code"),
		}
	}

	var response InitiateMultipartUploadResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// UploadPart uploads a single part of a multipart upload
func (c *Client) UploadPart(ctx context.Context, datasetID, uploadID string, partNumber int, partData []byte) (*UploadPartResponse, error) {
	endpointPath := fmt.Sprintf("/api/v1/datas3t/%s/multipart/%s/%d", datasetID, uploadID, partNumber)
	endpointURL, err := url.Parse(endpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint path: %w", err)
	}

	requestURL := c.baseURL.ResolveReference(endpointURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, requestURL.String(), bytes.NewReader(partData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("server returned non-OK status code"),
		}
	}

	var response UploadPartResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// CompleteMultipartUpload finalizes a multipart upload
func (c *Client) CompleteMultipartUpload(ctx context.Context, datasetID, uploadID string, partIDs []string) (*CompleteMultipartUploadResponse, error) {
	endpointPath := fmt.Sprintf("/api/v1/datas3t/%s/multipart/%s", datasetID, uploadID)
	endpointURL, err := url.Parse(endpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint path: %w", err)
	}

	requestURL := c.baseURL.ResolveReference(endpointURL)

	request := CompleteMultipartUploadRequest{
		PartIDs: partIDs,
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL.String(), bytes.NewReader(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("server returned non-OK status code"),
		}
	}

	var response CompleteMultipartUploadResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// CancelMultipartUpload cancels a multipart upload and cleans up resources
func (c *Client) CancelMultipartUpload(ctx context.Context, datasetID, uploadID string) error {
	endpointPath := fmt.Sprintf("/api/v1/datas3t/%s/multipart/%s", datasetID, uploadID)
	endpointURL, err := url.Parse(endpointPath)
	if err != nil {
		return fmt.Errorf("failed to parse endpoint path: %w", err)
	}

	requestURL := c.baseURL.ResolveReference(endpointURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, requestURL.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("server returned non-OK status code"),
		}
	}

	return nil
}

// GetMultipartUploadStatus retrieves the status of a multipart upload
func (c *Client) GetMultipartUploadStatus(ctx context.Context, datasetID, uploadID string) (*MultipartUploadStatus, error) {
	endpointPath := fmt.Sprintf("/api/v1/datas3t/%s/multipart/%s/status", datasetID, uploadID)
	endpointURL, err := url.Parse(endpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint path: %w", err)
	}

	requestURL := c.baseURL.ResolveReference(endpointURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("server returned non-OK status code"),
		}
	}

	var status MultipartUploadStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &status, nil
}

// ListMultipartUploads lists all active multipart uploads for a dataset
func (c *Client) ListMultipartUploads(ctx context.Context, datasetID string) (*ListMultipartUploadsResponse, error) {
	endpointPath := fmt.Sprintf("/api/v1/datas3t/%s/multipart", datasetID)
	endpointURL, err := url.Parse(endpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint path: %w", err)
	}

	requestURL := c.baseURL.ResolveReference(endpointURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("server returned non-OK status code"),
		}
	}

	var response ListMultipartUploadsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// UploadFileWithMultipart uploads a file using multipart upload with chunking and parallelization
func (c *Client) UploadFileWithMultipart(ctx context.Context, datasetID, filePath string, progressCallback ProgressCallback) (*CompleteMultipartUploadResponse, error) {
	const (
		chunkSize      = 5 * 1024 * 1024 // 5 MB chunks
		maxConcurrency = 5               // Maximum number of parallel uploads
	)

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}
	fileSize := fileInfo.Size()

	// Calculate number of parts
	numParts := int(math.Ceil(float64(fileSize) / float64(chunkSize)))
	if numParts == 0 {
		numParts = 1 // Ensure at least one part for empty files
	}

	// Initiate multipart upload
	upload, err := c.InitiateMultipartUpload(ctx, datasetID)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate multipart upload: %w", err)
	}

	// Set up error handling for cleanup in case of failure
	var uploadErr error
	defer func() {
		if uploadErr != nil {
			// Attempt to cancel the upload if there was an error
			_ = c.CancelMultipartUpload(context.Background(), datasetID, upload.UploadID)
		}
	}()

	// Use mutex to protect access to partIDs slice and progress tracking
	var mu sync.Mutex
	partIDs := make([]string, numParts)
	var totalBytesUploaded int64

	// Create a new errgroup with limited concurrency
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxConcurrency)

	// Start part uploads
	for i := 0; i < numParts; i++ {
		partNumber := i // Capture loop variable

		g.Go(func() error {
			// Calculate part size and offset
			partOffset := int64(partNumber * chunkSize)
			currentPartSize := int64(chunkSize)
			if partOffset+currentPartSize > fileSize {
				currentPartSize = fileSize - partOffset
			}

			// Read part data
			partData := make([]byte, currentPartSize)
			_, err := file.ReadAt(partData, partOffset)
			if err != nil && !errors.Is(err, io.EOF) {
				return fmt.Errorf("failed to read part %d: %w", partNumber, err)
			}

			// Upload the part
			partResp, err := c.UploadPart(gctx, datasetID, upload.UploadID, partNumber+1, partData)
			if err != nil {
				return fmt.Errorf("failed to upload part %d: %w", partNumber+1, err)
			}

			// Store the part ID and update progress
			mu.Lock()
			partIDs[partNumber] = partResp.PartID
			totalBytesUploaded += currentPartSize

			// Call progress callback if provided
			if progressCallback != nil {
				progressCallback(
					partNumber+1,       // 1-indexed part number
					numParts,           // total number of parts
					currentPartSize,    // bytes uploaded in this part
					totalBytesUploaded, // total bytes uploaded so far
					fileSize,           // total bytes to upload
				)
			}
			mu.Unlock()

			return nil
		})
	}

	// Wait for all uploads to complete and collect errors
	if err = g.Wait(); err != nil {
		uploadErr = err
		return nil, err
	}

	// Complete the multipart upload
	completeResp, err := c.CompleteMultipartUpload(ctx, datasetID, upload.UploadID, partIDs)
	if err != nil {
		uploadErr = fmt.Errorf("failed to complete multipart upload: %w", err)
		return nil, uploadErr
	}

	return completeResp, nil
}

// UploadFileWithMultipartNoProgress is a convenience method that calls UploadFileWithMultipart without a progress callback
func (c *Client) UploadFileWithMultipartNoProgress(ctx context.Context, datasetID, filePath string) (*CompleteMultipartUploadResponse, error) {
	return c.UploadFileWithMultipart(ctx, datasetID, filePath, nil)
}
