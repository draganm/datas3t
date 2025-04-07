package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// GetMissingRanges retrieves the missing ranges for a given dataset
func (c *Client) GetMissingRanges(ctx context.Context, datasetID string) (*MissingRangesResponse, error) {
	// Build the URL
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", datasetID, "missing-ranges")

	// Create a new GET request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
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
	var missingRangesResponse MissingRangesResponse
	if err := json.Unmarshal(body, &missingRangesResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &missingRangesResponse, nil
}
