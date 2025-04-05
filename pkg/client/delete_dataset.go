package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

// DeleteDataset deletes a dataset and schedules its objects for deletion
func (c *Client) DeleteDataset(ctx context.Context, id string) error {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id)

	req, err := http.NewRequestWithContext(ctx, "DELETE", endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read error response: %w", err)
		}
		return &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("delete dataset failed"),
		}
	}

	return nil
}
