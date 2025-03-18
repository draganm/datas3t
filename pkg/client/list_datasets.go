package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Dataset represents a dataset returned from the API
type Dataset struct {
	ID             string `json:"id"`
	DatarangeCount int64  `json:"datarange_count"`
	TotalSizeBytes int64  `json:"total_size_bytes"`
}

// ListDatasets retrieves all datasets from the server
func (c *Client) ListDatasets(ctx context.Context) ([]Dataset, error) {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t")

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
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read error response: %w", err)
		}
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("list datasets failed"),
		}
	}

	var datasets []Dataset
	err = json.NewDecoder(resp.Body).Decode(&datasets)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return datasets, nil
}
