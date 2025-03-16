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
	MinDatapointKey int64  `json:"min_datapoint_key"`
	MaxDatapointKey int64  `json:"max_datapoint_key"`
	SizeBytes       int64  `json:"size_bytes"`
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

// CreateDataset creates a new dataset with the given ID
func (c *Client) CreateDataset(ctx context.Context, id string) error {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id)

	req, err := http.NewRequestWithContext(ctx, "PUT", endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("create dataset failed"),
		}
	}

	return nil
}

// GetDataset retrieves dataset information by ID
func (c *Client) GetDataset(ctx context.Context, id string) ([]byte, error) {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("get dataset failed"),
		}
	}

	return io.ReadAll(resp.Body)
}

// UploadDatarange uploads data to a dataset
func (c *Client) UploadDatarange(ctx context.Context, id string, data io.Reader) error {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id)

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint.String(), data)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("upload datarange failed"),
		}
	}

	return nil
}

// GetDataranges retrieves the data ranges for a dataset
func (c *Client) GetDataranges(ctx context.Context, id string) ([]DataRange, error) {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id, "dataranges")

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("get dataranges failed"),
		}
	}

	var dataRanges []DataRange
	if err := json.NewDecoder(resp.Body).Decode(&dataRanges); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return dataRanges, nil
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
