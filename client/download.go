package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

func (c *DataS3tClient) Download(ctx context.Context, dbName string, id uint64, cb func(r io.Reader) error) error {
	ur, err := c.GetDownloadURL(ctx, dbName, id)
	if err != nil {
		return fmt.Errorf("could not get upload url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", ur, nil)
	if err != nil {
		return fmt.Errorf("could not create upload request: %w", err)
	}

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("could not perform upload request: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		d, _ := io.ReadAll(res.Body)
		return fmt.Errorf("unexpected status %s: %s", res.Status, string(d))
	}

	return cb(res.Body)

}

type downloadURLResponse struct {
	URL string `json:"url"`
}

func (c *DataS3tClient) GetDownloadURL(ctx context.Context, dbName string, id uint64) (ur string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("GetDownloadURL: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "db", dbName, "downloadUrl", strconv.FormatUint(id, 10))
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("could not create request: %w", err)
	}

	c.addAPIToken(req)

	res, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("could not perform request: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		d, _ := io.ReadAll(res.Body)
		return "", fmt.Errorf("got unexpected status %s: %s", res.Status, string(d))
	}

	du := &downloadURLResponse{}
	err = json.NewDecoder(res.Body).Decode(du)
	if err != nil {
		return "", fmt.Errorf("could not decode response: %w", err)
	}

	return du.URL, nil

}
