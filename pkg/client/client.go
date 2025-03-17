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
	"strings"

	"golang.org/x/sync/errgroup"
)

// DataRange represents a range of data in a dataset
type DataRange struct {
	ObjectKey       string `json:"object_key"`
	MinDatapointKey int64  `json:"min_datapoint_key"`
	MaxDatapointKey int64  `json:"max_datapoint_key"`
	SizeBytes       int64  `json:"size_bytes"`
}

// StatusError represents an error with an HTTP status code
type StatusError struct {
	StatusCode int
	Body       string
	Err        error
}

// Error implements the error interface
func (e *StatusError) Error() string {
	return fmt.Sprintf("unexpected status code: %d, body: %s, error: %v", e.StatusCode, e.Body, e.Err)
}

// Client represents an HTTP client for interacting with the datas3t server
type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
}

// NewClient creates a new datas3t client
func NewClient(baseURL string) (*Client, error) {
	parsedURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %w", err)
	}

	return &Client{
		baseURL:    parsedURL,
		httpClient: &http.Client{},
	}, nil
}

// CreateDataset creates a new dataset with the given ID
func (c *Client) CreateDataset(ctx context.Context, id string) error {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id)

	req, err := http.NewRequestWithContext(ctx, "PUT", endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read error response: %w", err)
		}
		return &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("create dataset failed"),
		}
	}

	return nil
}

// GetDataset retrieves dataset information by ID
func (c *Client) GetDataset(ctx context.Context, id string) ([]byte, error) {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id)

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read error response: %w", err)
		}
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("get dataset failed"),
		}
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	return body, nil
}

// UploadDatarange uploads data to a dataset
func (c *Client) UploadDatarange(ctx context.Context, id string, data io.Reader) error {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id)

	req, err := http.NewRequestWithContext(ctx, "POST", endpoint.String(), data)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read error response: %w", err)
		}
		return &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("upload datarange failed"),
		}
	}

	return nil
}

// GetDataranges retrieves the data ranges for a dataset
func (c *Client) GetDataranges(ctx context.Context, id string) ([]DataRange, error) {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id, "dataranges")

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read error response: %w", err)
		}
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       string(body),
			Err:        fmt.Errorf("get dataranges failed"),
		}
	}

	var dataRanges []DataRange
	err = json.NewDecoder(resp.Body).Decode(&dataRanges)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return dataRanges, nil
}

// ObjectAndRange represents a presigned URL and its associated range
type ObjectAndRange struct {
	GETURL string `json:"get_url"`
	Start  uint64 `json:"start"`
	End    uint64 `json:"end"`
}

// toDownload represents the information needed to download a range of data
type toDownload struct {
	url              string
	localFileOffset  uint64
	remoteRangeStart uint64
	remoteRangeEnd   uint64
}

// GetDatarange downloads data from a dataset within the specified range and writes it to the given file.
// It uses errgroup to parallelize the downloads and adds two empty blocks (2x512 zero bytes) at the end.
func (c *Client) GetDatarange(ctx context.Context, id string, start, end uint64, file io.WriterAt) error {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id, "data", strconv.FormatUint(start, 10), strconv.FormatUint(end, 10))

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint.String(), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var sb strings.Builder
		_, err = io.Copy(&sb, resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read error response: %w", err)
		}
		return &StatusError{
			StatusCode: resp.StatusCode,
			Body:       sb.String(),
			Err:        fmt.Errorf("get datarange failed"),
		}
	}

	var ranges []ObjectAndRange
	err = json.NewDecoder(resp.Body).Decode(&ranges)
	if err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	// Transform ranges into toDownload slice
	downloads := make([]toDownload, len(ranges))
	localFileOffset := uint64(0)
	for i, r := range ranges {
		downloads[i] = toDownload{
			url:              r.GETURL,
			localFileOffset:  localFileOffset,
			remoteRangeStart: r.Start,
			remoteRangeEnd:   r.End,
		}
		localFileOffset += r.End - r.Start + 1
	}

	// Create errgroup for parallel downloads
	g, ctx := errgroup.WithContext(ctx)

	for _, d := range downloads {
		g.Go(func() error {
			// Download the data from the presigned URL
			resp, err := http.Get(d.url)
			if err != nil {
				return fmt.Errorf("failed to download from presigned URL: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				var sb strings.Builder
				_, err = io.Copy(&sb, resp.Body)
				if err != nil {
					return fmt.Errorf("failed to read error response: %w", err)
				}
				return fmt.Errorf("failed to download data: status %d, body: %s", resp.StatusCode, sb.String())
			}

			// Create an offset writer for this range
			writer := io.NewOffsetWriter(file, int64(d.localFileOffset))

			// Copy the data directly to the file at the correct offset
			_, err = io.Copy(writer, resp.Body)
			if err != nil {
				return fmt.Errorf("failed to copy data to file: %w", err)
			}

			return nil
		})
	}

	// Wait for all downloads to complete
	err = g.Wait()
	if err != nil {
		return fmt.Errorf("failed to download data: %w", err)
	}

	// Add two empty blocks (2x512 zero bytes) at the end
	emptyBlock := make([]byte, 512)
	_, err = file.WriteAt(emptyBlock, int64(end-start+1))
	if err != nil {
		return fmt.Errorf("failed to write first empty block: %w", err)
	}
	_, err = file.WriteAt(emptyBlock, int64(end-start+513))
	if err != nil {
		return fmt.Errorf("failed to write second empty block: %w", err)
	}

	return nil
}

// GetStatusCode returns the HTTP status code from an error if it is a StatusError.
// Returns 0 if the error is not a StatusError or if the error is nil.
func GetStatusCode(err error) int {
	if err == nil {
		return 0
	}

	var statusErr *StatusError
	if errors.As(err, &statusErr) {
		return statusErr.StatusCode
	}

	return 0
}
