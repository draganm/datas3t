package client

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
)

type datarangeReader func(p []byte) (n int, err error)

func (dr datarangeReader) Read(p []byte) (n int, err error) {
	return dr(p)
}

func newDatarangeReader(ctx context.Context, chunkSize uint64, segments []DownloadSegment) io.Reader {
	remainingSegments := slices.Clone(segments)
	readAheadBuffer := []byte{}

	fillBuffer := func() error {
		if len(remainingSegments) == 0 {
			return io.EOF
		}

		currentSegment := &remainingSegments[0]

		// Parse the current segment's range
		start, end, err := parseRangeHeader(currentSegment.Range)
		if err != nil {
			return fmt.Errorf("failed to parse range header %s: %w", currentSegment.Range, err)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", currentSegment.PresignedURL, nil)
		if err != nil {
			return err
		}

		if (end - start + 1) > chunkSize {
			readAheadBuffer = make([]byte, chunkSize)
			// Update the segment's range for the next read
			newStart := start + chunkSize
			currentSegment.Range = fmt.Sprintf("bytes=%d-%d", newStart, end)
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, start+chunkSize-1))
		} else {
			readAheadBuffer = make([]byte, end-start+1)
			// Remove this segment since we're reading it completely
			remainingSegments = remainingSegments[1:]
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}

		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusPartialContent:
		case http.StatusOK:
		default:
			return fmt.Errorf("expected status code %d, got %d", http.StatusPartialContent, resp.StatusCode)
		}

		n, err := io.ReadFull(resp.Body, readAheadBuffer)
		if err != nil {
			return err
		}

		if n != len(readAheadBuffer) {
			return fmt.Errorf("expected to read %d bytes, got %d", len(readAheadBuffer), n)
		}

		return nil
	}

	return datarangeReader(func(p []byte) (n int, err error) {
		if len(readAheadBuffer) == 0 {
			err := fillBuffer()
			if err != nil {
				return 0, err
			}
		}

		toCopy := min(len(p), len(readAheadBuffer))

		copy(p, readAheadBuffer[:toCopy])
		readAheadBuffer = readAheadBuffer[toCopy:]

		return toCopy, nil
	})
}

// parseRangeHeader parses a "bytes=start-end" header and returns start and end values
func parseRangeHeader(rangeHeader string) (start, end uint64, err error) {
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, fmt.Errorf("invalid range format: %s", rangeHeader)
	}

	rangeStr := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeStr, "-")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid range format: %s", rangeHeader)
	}

	startInt, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid start byte: %w", err)
	}

	endInt, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid end byte: %w", err)
	}

	return startInt, endInt, nil
}
