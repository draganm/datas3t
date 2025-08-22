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

type DeleteDatas3tRequest struct {
	Name string `json:"name"`
}

type DeleteDatas3tResponse struct{}

func (r *DeleteDatas3tRequest) Validate() error {
	if r.Name == "" {
		return fmt.Errorf("name is required")
	}
	return nil
}

func (c *Client) DeleteDatas3t(ctx context.Context, req *DeleteDatas3tRequest) (*DeleteDatas3tResponse, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	ur, err := url.JoinPath(c.baseURL, "api", "v1", "datas3ts")
	if err != nil {
		return nil, fmt.Errorf("failed to join path: %w", err)
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal delete request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "DELETE", ur, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to delete datas3t: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("cannot delete datas3t: %s", string(bodyBytes))
	}

	if resp.StatusCode == http.StatusNotFound {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("datas3t not found: %s", string(bodyBytes))
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to delete datas3t (status %d): %s", resp.StatusCode, string(bodyBytes))
	}

	var response DeleteDatas3tResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("failed to decode delete response: %w", err)
	}

	return &response, nil
}