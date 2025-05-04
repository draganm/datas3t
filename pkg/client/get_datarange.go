package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"iter"

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

type DownloadRanges []ObjectAndRange

type downloadSegment struct {
	objectAndRange  ObjectAndRange
	localFileOffset uint64
}

func (r DownloadRanges) Iterate(maxChunkSize uint64) iter.Seq[downloadSegment] {

	return func(yield func(downloadSegment) bool) {

		localFileOffset := uint64(0)

		for _, item := range r {

			currentItemStart := item.Start

			for item.End-currentItemStart+1 > maxChunkSize {

				cnt := yield(downloadSegment{
					objectAndRange: ObjectAndRange{
						GETURL: item.GETURL,
						Start:  currentItemStart,
						End:    currentItemStart + maxChunkSize - 1,
					},
					localFileOffset: localFileOffset,
				})
				if !cnt {
					return
				}

				localFileOffset += maxChunkSize
				currentItemStart += maxChunkSize

			}

			cnt := yield(downloadSegment{
				objectAndRange: ObjectAndRange{
					GETURL: item.GETURL,
					Start:  currentItemStart,
					End:    item.End,
				},
				localFileOffset: localFileOffset,
			})
			if !cnt {
				return
			}

			localFileOffset += item.End - currentItemStart + 1

		}
	}
}

func DownloadDataranges(ctx context.Context, ranges DownloadRanges, file io.WriterAt) error {

	g, grCtx := errgroup.WithContext(ctx)

	g.SetLimit(10)

	lastByteOffset := uint64(0)
	for d := range ranges.Iterate(5 * 1024 * 1024) {
		lastByteOffset = max(lastByteOffset, d.localFileOffset+d.objectAndRange.End-d.objectAndRange.Start+1)
		g.Go(func() error {
			req, err := http.NewRequestWithContext(grCtx, "GET", d.objectAndRange.GETURL, nil)
			if err != nil {
				return fmt.Errorf("failed to create request: %w", err)
			}

			rangeHeader := fmt.Sprintf("bytes=%d-%d", d.objectAndRange.Start, d.objectAndRange.End)
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

	emptyTwoBlocks := make([]byte, 512*2)
	_, err = file.WriteAt(emptyTwoBlocks, int64(lastByteOffset))
	if err != nil {
		return fmt.Errorf("failed to write first empty block: %w", err)
	}

	return nil
}
