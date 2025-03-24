package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// DataRange represents a range of data in a dataset
type DataRange struct {
	ObjectKey       string `json:"object_key"`
	MinDatapointKey uint64 `json:"min_datapoint_key"`
	MaxDatapointKey uint64 `json:"max_datapoint_key"`
	SizeBytes       uint64 `json:"size_bytes"`
}

// AggregateResponse represents the response from the aggregate datarange endpoint
type AggregateResponse struct {
	DatasetID      string `json:"dataset_id"`
	StartKey       uint64 `json:"start_key"`
	EndKey         uint64 `json:"end_key"`
	RangesReplaced uint64 `json:"ranges_replaced"`
	NewObjectKey   string `json:"new_object_key"`
	SizeBytes      uint64 `json:"size_bytes"`
}

// StatusError represents an error with an HTTP status code
type StatusError struct {
	StatusCode int
	Body       string
	Err        error
}

// Error implements the error interface
func (e *StatusError) Error() string {
	return fmt.Sprintf("unexpected status code: %d, body: %s, error: %v", e.StatusCode, e.Body, e.Err)
}

// Client represents an HTTP client for interacting with the datas3t server
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

// NewClient creates a new datas3t client
func NewClient(baseURL string) (*Client, error) {
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	return &Client{
		baseURL:    parsedURL,
		httpClient: &http.Client{},
	}, nil
}

// GetStatusCode returns the HTTP status code from an error if it is a StatusError.
// Returns 0 if the error is not a StatusError or if the error is nil.
func GetStatusCode(err error) int {
	if err == nil {
		return 0
	}

	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode
	}

	return 0
}

// AggregateDatarange aggregates multiple dataranges within the specified range into a single datarange
func (c *Client) AggregateDatarange(ctx context.Context, datasetID string, startKey, endKey uint64) (*AggregateResponse, error) {
	// Build the URL
	endpointPath := fmt.Sprintf("/api/v1/datas3t/%s/aggregate/%d/%d", datasetID, startKey, endKey)
	endpointURL, err := url.Parse(endpointPath)
	if err != nil {
		return nil, fmt.Errorf("failed to parse endpoint path: %w", err)
	}

	requestURL := c.baseURL.ResolveReference(endpointURL)

	// Create a new POST request
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, requestURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Send the request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Check if the response status code indicates an error
	if resp.StatusCode != http.StatusOK {
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("server returned non-OK status code"),
		}
	}

	// Decode the response
	var aggregateResponse AggregateResponse
	if err := json.Unmarshal(body, &aggregateResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &aggregateResponse, nil
}
