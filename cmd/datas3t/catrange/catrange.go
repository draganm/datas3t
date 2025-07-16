package catrange

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/draganm/datas3t/client"
	"github.com/klauspost/compress/zstd"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "cat-range",
		Usage: "Print contents of datapoints in a range",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server-url",
				Value:   "http://localhost:8765",
				Usage:   "Server URL",
				EnvVars: []string{"DATAS3T_SERVER_URL"},
			},
		},
		ArgsUsage: "<datas3t-name> <first-datapoint> <last-datapoint>",
		Action:    catRangeAction,
	}
}

func catRangeAction(c *cli.Context) error {
	if c.NArg() != 3 {
		return fmt.Errorf("expected 3 arguments: <datas3t-name> <first-datapoint> <last-datapoint>")
	}

	datas3tName := c.Args().Get(0)
	firstStr := c.Args().Get(1)
	lastStr := c.Args().Get(2)

	first, err := strconv.ParseUint(firstStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid first datapoint: %w", err)
	}

	last, err := strconv.ParseUint(lastStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid last datapoint: %w", err)
	}

	if first > last {
		return fmt.Errorf("first datapoint (%d) cannot be greater than last datapoint (%d)", first, last)
	}

	client := client.NewClient(c.String("server-url"))

	// Create custom iterator that gives us filenames and content
	for filename, content := range datapointIteratorWithFilenames(client, context.Background(), datas3tName, first, last) {
		decompressed, err := decompressContent(content, filename)
		if err != nil {
			return fmt.Errorf("failed to decompress file %s: %w", filename, err)
		}

		os.Stdout.Write(decompressed)
		os.Stdout.WriteString("\n")
	}

	return nil
}

// datapointIteratorWithFilenames creates an iterator that yields filename and content
func datapointIteratorWithFilenames(c *client.Client, ctx context.Context, datas3tName string, firstDatapoint, lastDatapoint uint64) iter.Seq2[string, []byte] {
	return func(yield func(string, []byte) bool) {
		// Get presigned download URLs for the datapoints
		req := &client.PreSignDownloadForDatapointsRequest{
			Datas3tName:    datas3tName,
			FirstDatapoint: firstDatapoint,
			LastDatapoint:  lastDatapoint,
		}

		resp, err := c.PreSignDownloadForDatapoints(ctx, req)
		if err != nil {
			return // Can't yield error in this iterator pattern, will handle in caller
		}

		if len(resp.DownloadSegments) == 0 {
			// No download segments means empty datas3t - complete normally without yielding anything
			return
		}

		// Create a reader from the download segments
		reader := createSimpleReader(ctx, resp.DownloadSegments)
		tarReader := tar.NewReader(reader)

		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return // Can't yield error in this iterator pattern
			}
			
			if header.Typeflag != tar.TypeReg {
				continue
			}

			filename := filepath.Base(header.Name)
			data, err := io.ReadAll(tarReader)
			if err != nil {
				return // Can't yield error in this iterator pattern
			}
			if !yield(filename, data) {
				return
			}
		}
	}
}

// createSimpleReader creates a simple sequential reader for download segments
func createSimpleReader(ctx context.Context, segments []client.DownloadSegment) io.Reader {
	readers := make([]io.Reader, len(segments))
	for i, seg := range segments {
		readers[i] = &segmentReader{ctx: ctx, segment: seg}
	}
	return io.MultiReader(readers...)
}

type segmentReader struct {
	ctx     context.Context
	segment client.DownloadSegment
	reader  io.Reader
	read    bool
}

func (sr *segmentReader) Read(p []byte) (n int, err error) {
	if !sr.read {
		req, err := http.NewRequestWithContext(sr.ctx, "GET", sr.segment.PresignedURL, nil)
		if err != nil {
			return 0, err
		}
		
		if sr.segment.Range != "" {
			req.Header.Set("Range", sr.segment.Range)
		}
		
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0, err
		}
		defer resp.Body.Close()
		
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return 0, err
		}
		
		sr.reader = strings.NewReader(string(data))
		sr.read = true
	}
	
	if sr.reader == nil {
		return 0, io.EOF
	}
	
	return sr.reader.Read(p)
}

func decompressContent(content []byte, filename string) ([]byte, error) {
	ext := strings.ToLower(filepath.Ext(filename))
	
	switch ext {
	case ".gz":
		reader, err := gzip.NewReader(strings.NewReader(string(content)))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer reader.Close()
		
		decompressed, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress gzip: %w", err)
		}
		return decompressed, nil
		
	case ".zst", ".zstd":
		decoder, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
		}
		defer decoder.Close()
		
		decompressed, err := decoder.DecodeAll(content, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress zstd: %w", err)
		}
		return decompressed, nil
		
	default:
		return content, nil
	}
}