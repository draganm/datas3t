package client

import (
	"errors"
	"fmt"
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
