package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/RoaringBitmap/roaring/roaring64"
)

func (c *Client) GetDatapointsBitmap(ctx context.Context, datas3tName string) (*roaring64.Bitmap, error) {
	ur, err := url.JoinPath(c.baseURL, "api", "v1", "datapoints-bitmap")
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
		return nil, fmt.Errorf("failed to get datapoints bitmap: %w", err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to get datapoints bitmap: %s", resp.Status)
	}

	// Read raw bitmap bytes
	bitmapBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read bitmap data: %w", err)
	}

	// Deserialize roaring bitmap from bytes
	bitmap := roaring64.New()
	err = bitmap.UnmarshalBinary(bitmapBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize bitmap: %w", err)
	}

	return bitmap, nil
}
