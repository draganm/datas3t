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

type DataS3tClient struct {
	u       *url.URL
	hc      *http.Client
	options Options
}

type Options struct {
	APIToken      string
	AdminAPIToken string
}

func NewClient(baseURL string, options Options) (*DataS3tClient, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("could not parse base URL: %w", err)
	}

	return &DataS3tClient{
		u:       u,
		hc:      http.DefaultClient,
		options: options,
	}, nil
}

var ErrAlreadyExists = errors.New("already exists")

func (c *DataS3tClient) addAdminAPIToken(r *http.Request) {
	if c.options.AdminAPIToken != "" {
		r.Header.Set("authorization", fmt.Sprintf("Bearer %s", c.options.AdminAPIToken))
	}
}

func (c *DataS3tClient) addAPIToken(r *http.Request) {
	if c.options.APIToken != "" {
		r.Header.Set("authorization", fmt.Sprintf("Bearer %s", c.options.APIToken))
	}
}

func (c *DataS3tClient) CreateDB(ctx context.Context, name string) (err error) {

	defer func() {
		if err != nil {
			err = fmt.Errorf("CreateDB: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "admin", "db", name)
	req, err := http.NewRequestWithContext(ctx, "PUT", u.String(), nil)
	if err != nil {
		return fmt.Errorf("could not create request: %w", err)
	}

	c.addAdminAPIToken(req)

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

func (c *DataS3tClient) ListDBs(ctx context.Context) (dbs []string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("ListDBs: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "admin", "db")
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("could not create request: %w", err)
	}

	c.addAdminAPIToken(req)

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

func (c *DataS3tClient) GetLastID(ctx context.Context, dbName string) (lastID uint64, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("GetLastID: %w", err)
		}
	}()

	u := c.u.JoinPath("api", "db", dbName, "lastId")
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return 0, fmt.Errorf("could not create request: %w", err)
	}

	c.addAPIToken(req)

	res, err := c.hc.Do(req)
	if err != nil {
		return 0, fmt.Errorf("could not perform request: %w", err)
	}

	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		d, _ := io.ReadAll(res.Body)
		return 0, fmt.Errorf("got unexpected status %s: %s", res.Status, string(d))
	}

	d, err := io.ReadAll(res.Body)
	if err != nil {
		return 0, fmt.Errorf("could not read whole URL: %w", err)
	}

	lastID, err = strconv.ParseUint(string(d), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse last ID %s: %w", string(d), err)
	}

	return lastID, nil

}
