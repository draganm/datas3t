package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

func (c *Client) ImportDatas3t(ctx context.Context, req *ImportDatas3tRequest) (*ImportDatas3tResponse, error) {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "datas3ts", "import")
	if err != nil {
		return nil, fmt.Errorf("failed to join path: %w", err)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal import request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", ur, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to import datas3t: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to import datas3t: %s", resp.Status)
	}

	var response ImportDatas3tResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("failed to decode import response: %w", err)
	}

	return &response, nil
}