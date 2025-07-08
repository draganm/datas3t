package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

func (c *Client) CompleteAggregate(ctx context.Context, r *CompleteAggregateRequest) error {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "aggregate", "complete")
	if err != nil {
		return fmt.Errorf("failed to join path: %w", err)
	}

	body, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", ur, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to complete aggregate: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to complete aggregate: %s: %s", resp.Status, string(body))
	}

	return nil
}