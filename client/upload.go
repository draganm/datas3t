package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

func (c *DataS3tClient) Upload(ctx context.Context, dbName string, id uint64, data []byte) error {
	ur, err := c.GetUploadURL(ctx, dbName, id)
	if err != nil {
		return fmt.Errorf("could not get upload url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "PUT", ur, bytes.NewReader(data))
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

	return nil
}
