package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// WaitDatasets waits for datasets to reach the specified datapoints.
// It returns the current maximum datapoint for each dataset, and an error if any occurred.
// If the context is canceled, it returns the current state of the datasets.
// It will continue polling if the server returns a 202 Accepted status, and will only
// terminate if the context is canceled or the server returns an error status code (>299).
func (c *Client) WaitDatasets(ctx context.Context, datasets map[string]uint64) (*WaitDatasetsResponse, error) {
	// Build request URL
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", "wait")

	// Create request body
	reqBody := WaitDatasetsRequest{
		Datasets: datasets,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("operation canceled: %w", ctx.Err())
		default:
			// Create POST request
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(bodyBytes))
			if err != nil {
				return nil, fmt.Errorf("failed to create request: %w", err)
			}
			req.Header.Set("Content-Type", "application/json")

			// Execute request
			resp, err := c.httpClient.Do(req)
			if err != nil {
				return nil, fmt.Errorf("failed to execute request: %w", err)
			}

			// Read response body
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, fmt.Errorf("failed to read response body: %w", err)
			}

			// Handle status codes
			if resp.StatusCode > 299 {
				return nil, &StatusError{
					StatusCode: resp.StatusCode,
					Body:       string(body),
					Err:        fmt.Errorf("wait datasets failed"),
				}
			}

			// If status is OK, parse and return the response
			if resp.StatusCode == http.StatusOK {
				var response WaitDatasetsResponse
				if err := json.Unmarshal(body, &response); err != nil {
					return nil, fmt.Errorf("failed to unmarshal response: %w", err)
				}
				return &response, nil
			}

			// If we get 202 Accepted, continue polling
			if resp.StatusCode == http.StatusAccepted {
				// Brief pause to avoid hammering the server
				select {
				case <-ctx.Done():
					return nil, fmt.Errorf("operation canceled: %w", ctx.Err())
				case <-time.After(500 * time.Millisecond):
					// Continue polling
				}
				continue
			}
		}
	}
}
