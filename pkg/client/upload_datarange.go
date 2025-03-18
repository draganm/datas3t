package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
)

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
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read error response: %w", err)
		}
		return &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("upload datarange failed"),
		}
	}

	return nil
}
