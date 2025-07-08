package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

func (c *Client) DeleteDatarange(ctx context.Context, r *DeleteDatarangeRequest) error {
	body, err := json.Marshal(r)
	if err != nil {
		return err
	}

	u, err := url.JoinPath(c.baseURL, "api", "v1", "datarange", "delete")
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewBuffer(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to delete datarange: %s", resp.Status)
	}

	return nil
}
