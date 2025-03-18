package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

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
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read error response: %w", err)
		}
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("get dataranges failed"),
		}
	}

	var dataRanges []DataRange
	err = json.NewDecoder(resp.Body).Decode(&dataRanges)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return dataRanges, nil
}
