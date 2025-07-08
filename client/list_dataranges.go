package client

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

func (c *Client) ListDataranges(ctx context.Context, datas3tName string) ([]DatarangeInfo, error) {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "dataranges")
	if err != nil {
		return nil, fmt.Errorf("failed to join path: %w", err)
	}

	// Add query parameter
	u, err := url.Parse(ur)
	if err != nil {
		return nil, fmt.Errorf("failed to parse URL: %w", err)
	}

	q := u.Query()
	q.Set("datas3t_name", datas3tName)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list dataranges: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to list dataranges: %s", resp.Status)
	}

	var response ListDatarangesResponse
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return response.Dataranges, nil
}