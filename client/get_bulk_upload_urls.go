package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
)

func (c *DataS3tClient) GetBulkUploaddURLs(ctx context.Context, dbName string, fromID, toID uint64) (ur []string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("GetDownloadURL: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "db", dbName, "bulkUploadUrls", strconv.FormatUint(fromID, 10), strconv.FormatUint(toID, 10))
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("could not create request: %w", err)
	}

	c.addAPIToken(req)

	res, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not perform request: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		d, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("got unexpected status %s: %s", res.Status, string(d))
	}

	dec := json.NewDecoder(res.Body)

	urls := []string{}

	for {
		du := &bulkURLsResponse{}
		err = dec.Decode(du)
		if err == io.EOF {
			return urls, nil
		}

		if err != nil {
			return nil, fmt.Errorf("could not decode response: %w", err)
		}

		urls = append(urls, du.URL)
	}

}
