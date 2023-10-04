package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type AdminClient struct {
	u  *url.URL
	hc *http.Client
}

func NewAdminClient(baseURL string) (*AdminClient, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("could not parse base URL: %w", err)
	}

	return &AdminClient{u: u, hc: http.DefaultClient}, nil
}

func (c *AdminClient) CreateDB(ctx context.Context, name string) (err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("CreateDB: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "database", name)
	req, err := http.NewRequestWithContext(ctx, "PUT", u.String(), nil)
	if err != nil {
		return fmt.Errorf("could not create request: %w", err)
	}

	res, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("could not perform request: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusCreated {
		d, _ := io.ReadAll(res.Body)
		return fmt.Errorf("got unexpected status %s: %s", res.Status, string(d))
	}

	return nil

}
