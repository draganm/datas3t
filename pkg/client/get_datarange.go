package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"golang.org/x/sync/errgroup"
)

type ObjectAndRange struct {
	GETURL string `json:"get_url"`
	Start  uint64 `json:"start"`
	End    uint64 `json:"end"`
}

type toDownload struct {
	url              string
	localFileOffset  uint64
	remoteRangeStart uint64
	remoteRangeEnd   uint64
}

func (c *Client) GetDatarange(ctx context.Context, id string, start, end uint64) ([]ObjectAndRange, error) {
	endpoint := c.baseURL.JoinPath("api", "v1", "datas3t", id, "datarange", strconv.FormatUint(start, 10), strconv.FormatUint(end, 10))

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
		var sb strings.Builder
		_, err = io.Copy(&sb, resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read error response: %w", err)
		}
		return nil, &StatusError{
			StatusCode: resp.StatusCode,
			Body:       sb.String(),
			Err:        fmt.Errorf("get datarange failed"),
		}
	}

	var ranges []ObjectAndRange
	err = json.NewDecoder(resp.Body).Decode(&ranges)
	if err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return ranges, nil
}

func (c *Client) DownloadDataranges(ctx context.Context, ranges []ObjectAndRange, file io.WriterAt) error {
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

	g, ctx := errgroup.WithContext(ctx)

	for _, d := range downloads {
		g.Go(func() error {
			req, err := http.NewRequestWithContext(ctx, "GET", d.url, nil)
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}

			rangeHeader := fmt.Sprintf("bytes=%d-%d", d.remoteRangeStart, d.remoteRangeEnd)
			req.Header.Set("Range", rangeHeader)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return fmt.Errorf("failed to download from presigned URL: %w", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
				var sb strings.Builder
				_, err = io.Copy(&sb, resp.Body)
				if err != nil {
					return fmt.Errorf("failed to read error response: %w", err)
				}
				return fmt.Errorf("failed to download data: status %d, body: %s", resp.StatusCode, sb.String())
			}

			writer := io.NewOffsetWriter(file, int64(d.localFileOffset))
			_, err = io.Copy(writer, resp.Body)
			if err != nil {
				return fmt.Errorf("failed to copy data to file: %w", err)
			}

			return nil
		})
	}

	err := g.Wait()
	if err != nil {
		return fmt.Errorf("failed to download data: %w", err)
	}

	emptyBlock := make([]byte, 512)
	_, err = file.WriteAt(emptyBlock, int64(localFileOffset))
	if err != nil {
		return fmt.Errorf("failed to write first empty block: %w", err)
	}
	_, err = file.WriteAt(emptyBlock, int64(localFileOffset+512))
	if err != nil {
		return fmt.Errorf("failed to write second empty block: %w", err)
	}

	return nil
}
