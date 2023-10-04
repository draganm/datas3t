package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
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

var ErrAlreadyExists = errors.New("already exists")

func (c *AdminClient) CreateDB(ctx context.Context, name string) (err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("CreateDB: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "db", name)
	req, err := http.NewRequestWithContext(ctx, "PUT", u.String(), nil)
	if err != nil {
		return fmt.Errorf("could not create request: %w", err)
	}

	res, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("could not perform request: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode == http.StatusConflict {
		return ErrAlreadyExists
	}

	if res.StatusCode != http.StatusCreated {
		d, _ := io.ReadAll(res.Body)
		return fmt.Errorf("got unexpected status %s: %s", res.Status, string(d))
	}

	return nil

}

func (c *AdminClient) ListDBs(ctx context.Context) (dbs []string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("ListDBs: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "db")
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("could not create request: %w", err)
	}

	res, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not perform request: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		d, _ := io.ReadAll(res.Body)
		return nil, fmt.Errorf("got unexpected status %s: %s", res.Status, string(d))
	}

	err = json.NewDecoder(res.Body).Decode(&dbs)
	if err != nil {
		return nil, fmt.Errorf("could not decode response: %w", err)
	}

	return dbs, nil

}

func (c *AdminClient) GetUploadURL(ctx context.Context, dbName string, id uint64) (ur string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("GetUploadURL: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "db", dbName, "uploadUrl", strconv.FormatUint(id, 10))
	req, err := http.NewRequestWithContext(ctx, "POST", u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("could not create request: %w", err)
	}

	res, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("could not perform request: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		d, _ := io.ReadAll(res.Body)
		return "", fmt.Errorf("got unexpected status %s: %s", res.Status, string(d))
	}

	d, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("could not read whole URL: %w", err)
	}

	return string(d), nil

}
