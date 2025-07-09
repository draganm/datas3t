package client

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"iter"
)

const (
	// MaxChunkSize is the maximum chunk size for streaming downloads (5MB)
	MaxChunkSize = 5 * 1024 * 1024
)

// DatapointIterator creates an iterator that progressively loads chunks of max 5MB
// and yields individual datapoint file contents from the tar stream
func (c *Client) DatapointIterator(ctx context.Context, datas3tName string, firstDatapoint, lastDatapoint uint64) iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		// Get presigned download URLs for the datapoints
		req := &PreSignDownloadForDatapointsRequest{
			Datas3tName:    datas3tName,
			FirstDatapoint: firstDatapoint,
			LastDatapoint:  lastDatapoint,
		}

		resp, err := c.PreSignDownloadForDatapoints(ctx, req)
		if err != nil {
			yield(nil, fmt.Errorf("failed to get presigned download URLs: %w", err))
			return
		}

		if len(resp.DownloadSegments) == 0 {
			// No download segments means empty datas3t - complete normally without yielding anything
			return
		}

		// Pass download segments directly to newDatarangeReader
		r := newDatarangeReader(ctx, MaxChunkSize, resp.DownloadSegments)
		tr := tar.NewReader(r)

		for {
			_, err := tr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				yield(nil, err)
				return
			}
			data, err := io.ReadAll(tr)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(data, nil) {
				return
			}
		}
	}
}
