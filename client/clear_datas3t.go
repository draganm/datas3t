package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

func (c *Client) ClearDatas3t(ctx context.Context, req *ClearDatas3tRequest) (*ClearDatas3tResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	ur, err := url.JoinPath(c.baseURL, "api", "v1", "datas3ts", "clear")
	if err != nil {
		return nil, fmt.Errorf("failed to join path: %w", err)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal clear request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", ur, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to clear datas3t: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to clear datas3t: %s", resp.Status)
	}

	var response ClearDatas3tResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("failed to decode clear response: %w", err)
	}

	return &response, nil
}