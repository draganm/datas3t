package client

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDownloadDataranges(t *testing.T) {
	// Create a test server to simulate S3 presigned URLs
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Expect Range header
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintln(w, "Range header is required")
			return
		}

		// Parse the range header
		var start, end int
		_, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "Invalid range header: %s", rangeHeader)
			return
		}

		// Get the test data from the URL path (small, medium, large, error)
		dataType := r.URL.Path[1:] // Skip leading "/"

		switch dataType {
		case "error":
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintln(w, "Simulated error")
			return
		case "timeout":
			// Simulate a timeout
			time.Sleep(500 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			return
		default:
			// Generate data based on the range
			size := end - start + 1
			data := make([]byte, size)

			// Fill with deterministic pattern based on start position
			for i := 0; i < size; i++ {
				data[i] = byte((start + i) % 256)
			}

			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
			w.WriteHeader(http.StatusPartialContent)
			w.Write(data)
		}
	}))
	defer server.Close()

	tests := []struct {
		name           string
		ranges         []ObjectAndRange
		expectError    bool
		errorContains  string
		expectedSize   int
		timeout        time.Duration
		verifyContents bool
	}{
		{
			name: "small single range",
			ranges: []ObjectAndRange{
				{
					GETURL: server.URL + "/small",
					Start:  0,
					End:    1023, // 1KB
				},
			},
			expectedSize:   1024,
			verifyContents: true,
		},
		{
			name: "multiple small ranges",
			ranges: []ObjectAndRange{
				{
					GETURL: server.URL + "/small1",
					Start:  0,
					End:    1023, // 1KB
				},
				{
					GETURL: server.URL + "/small2",
					Start:  1024,
					End:    2047, // 1KB
				},
				{
					GETURL: server.URL + "/small3",
					Start:  2048,
					End:    3071, // 1KB
				},
			},
			expectedSize:   3072,
			verifyContents: true,
		},
		{
			name: "medium range",
			ranges: []ObjectAndRange{
				{
					GETURL: server.URL + "/medium",
					Start:  0,
					End:    1048575, // 1MB
				},
			},
			expectedSize: 1048576,
		},
		{
			name: "one large range (10MB)",
			ranges: []ObjectAndRange{
				{
					GETURL: server.URL + "/large",
					Start:  0,
					End:    10485759, // 10MB
				},
			},
			expectedSize: 10485760,
		},
		{
			name: "multiple ranges including large",
			ranges: []ObjectAndRange{
				{
					GETURL: server.URL + "/small",
					Start:  0,
					End:    1023, // 1KB
				},
				{
					GETURL: server.URL + "/large",
					Start:  1024,
					End:    10486783, // 10MB
				},
				{
					GETURL: server.URL + "/medium",
					Start:  10486784,
					End:    11535359, // 1MB
				},
			},
			expectedSize: 11535360,
		},
		{
			name: "error in one range",
			ranges: []ObjectAndRange{
				{
					GETURL: server.URL + "/small",
					Start:  0,
					End:    1023, // 1KB
				},
				{
					GETURL: server.URL + "/error",
					Start:  1024,
					End:    2047, // 1KB
				},
			},
			expectError:   true,
			errorContains: "failed to download data: status 500",
		},
		{
			name: "context timeout",
			ranges: []ObjectAndRange{
				{
					GETURL: server.URL + "/timeout",
					Start:  0,
					End:    1023, // 1KB
				},
			},
			timeout:       50 * time.Millisecond,
			expectError:   true,
			errorContains: "context deadline exceeded",
		},
		{
			name: "very large parallel downloads",
			ranges: func() []ObjectAndRange {
				// Create 20 ranges of 1MB each
				var ranges []ObjectAndRange
				for i := 0; i < 20; i++ {
					ranges = append(ranges, ObjectAndRange{
						GETURL: server.URL + fmt.Sprintf("/large%d", i),
						Start:  uint64(i * 1048576),
						End:    uint64((i+1)*1048576 - 1),
					})
				}
				return ranges
			}(),
			expectedSize: 20 * 1048576, // 20MB
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a buffer to store the downloaded data
			buf := &bytes.Buffer{}
			file := &testWriterAt{buf: buf}

			// Create context with timeout if specified
			ctx := context.Background()
			if tt.timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, tt.timeout)
				defer cancel()
			}

			// Call the function under test
			err := DownloadDataranges(ctx, tt.ranges, file)

			// Check error expectations
			if tt.expectError {
				require.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
				return
			}

			require.NoError(t, err)

			// Verify the size of the downloaded data
			assert.Equal(t, tt.expectedSize+1024, buf.Len(), "Downloaded data size mismatch (including empty blocks)")

			// For small datasets, verify content integrity
			if tt.verifyContents {
				data := buf.Bytes()

				// Skip the empty blocks at the end (512 bytes each, 1024 total)
				data = data[:len(data)-1024]

				// Verify each range's data
				var offset int
				for _, r := range tt.ranges {
					size := int(r.End - r.Start + 1)
					for i := 0; i < size; i++ {
						expected := byte((int(r.Start) + i) % 256)
						if data[offset+i] != expected {
							t.Errorf("Data mismatch at offset %d (range start %d + %d): got %d, want %d",
								offset+i, r.Start, i, data[offset+i], expected)
							// Only report the first few errors
							if i > 10 {
								break
							}
						}
					}
					offset += size
				}
			}
		})
	}
}

// testWriterAt implements io.WriterAt for testing
type testWriterAt struct {
	buf *bytes.Buffer
	mu  sync.Mutex
}

func (t *testWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// If we need to extend the buffer
	if int(off) > t.buf.Len() {
		padding := make([]byte, int(off)-t.buf.Len())
		t.buf.Write(padding)
	} else if int(off) < t.buf.Len() {
		// Overwrite existing content
		// Copy current buffer
		data := t.buf.Bytes()
		// Create new buffer with updated content
		newBuf := bytes.NewBuffer(nil)
		newBuf.Write(data[:off])
		newBuf.Write(p)
		if int(off)+len(p) < len(data) {
			newBuf.Write(data[int(off)+len(p):])
		}
		// Replace old buffer with new one
		*t.buf = *newBuf
		return len(p), nil
	}

	return t.buf.Write(p)
}

// TestParallelDownloadPerformance tests the performance of parallel downloads with different sizes
func TestParallelDownloadPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	// Create a test server that generates random data of requested size
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rangeHeader := r.Header.Get("Range")
		if rangeHeader == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		var start, end int
		_, err := fmt.Sscanf(rangeHeader, "bytes=%d-%d", &start, &end)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		size := end - start + 1

		// For performance testing, we'll generate random data
		// but we don't need to return all of it - we can simulate the time
		// it takes to transfer by sleeping proportionally to size

		// Sleep time: ~5ms per MB to simulate network transfer
		transferTime := time.Duration(size/1048576*5) * time.Millisecond
		time.Sleep(transferTime)

		// Generate a small sample of the data for headers
		sample := make([]byte, min(size, 1024))
		rand.Read(sample)

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", size))
		w.WriteHeader(http.StatusPartialContent)

		// Write the sample data
		w.Write(sample)

		// For the rest, just write zeros in chunks to simulate the transfer
		chunkSize := 64 * 1024
		zeros := make([]byte, chunkSize)
		remaining := size - len(sample)

		for remaining > 0 {
			writeSize := min(remaining, chunkSize)
			w.Write(zeros[:writeSize])
			remaining -= writeSize

			// Add small jitter to simulate network variability
			if rand.Intn(10) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}))
	defer server.Close()

	performanceTests := []struct {
		name        string
		rangeCount  int
		rangeSize   int
		totalSize   int
		description string
	}{
		{
			name:        "many_small_ranges",
			rangeCount:  100,
			rangeSize:   100 * 1024,       // 100KB each
			totalSize:   10 * 1024 * 1024, // ~10MB total
			description: "Many small ranges (100 ranges of 100KB)",
		},
		{
			name:        "few_large_ranges",
			rangeCount:  5,
			rangeSize:   2 * 1024 * 1024,  // 2MB each
			totalSize:   10 * 1024 * 1024, // 10MB total
			description: "Few large ranges (5 ranges of 2MB)",
		},
		{
			name:        "mixed_ranges",
			rangeCount:  20,
			rangeSize:   512 * 1024,       // 512KB each
			totalSize:   10 * 1024 * 1024, // ~10MB total
			description: "Mixed-size ranges (20 ranges of 512KB)",
		},
	}

	for _, tt := range performanceTests {
		t.Run(tt.name, func(t *testing.T) {
			// Create the ranges
			var ranges []ObjectAndRange
			currentStart := uint64(0)

			for i := 0; i < tt.rangeCount; i++ {
				end := currentStart + uint64(tt.rangeSize) - 1
				ranges = append(ranges, ObjectAndRange{
					GETURL: server.URL + fmt.Sprintf("/perf%d", i),
					Start:  currentStart,
					End:    end,
				})
				currentStart = end + 1
			}

			// Create a discard writer for performance testing
			discardWriter := &discardWriterAt{}

			// Measure download time
			start := time.Now()
			err := DownloadDataranges(context.Background(), ranges, discardWriter)
			duration := time.Since(start)

			require.NoError(t, err)

			// Log performance metrics
			throughputMBps := float64(tt.totalSize) / 1024 / 1024 / duration.Seconds()
			t.Logf("Download performance - %s:\n", tt.description)
			t.Logf("  - Time: %v\n", duration)
			t.Logf("  - Throughput: %.2f MB/s\n", throughputMBps)
			t.Logf("  - Ranges: %d\n", tt.rangeCount)
			t.Logf("  - Total size: %.2f MB\n", float64(tt.totalSize)/1024/1024)
		})
	}
}

// discardWriterAt is a io.WriterAt that discards all writes
type discardWriterAt struct{}

func (d *discardWriterAt) WriteAt(p []byte, off int64) (n int, err error) {
	return len(p), nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestDownloadRangesIterate(t *testing.T) {
	tests := []struct {
		name           string
		ranges         DownloadRanges
		maxChunkSize   uint64
		expectedChunks []struct {
			objectURL       string
			start           uint64
			end             uint64
			localFileOffset uint64
		}
		expectedTotalBytes uint64
	}{
		{
			name: "single range smaller than chunk size",
			ranges: DownloadRanges{
				{
					GETURL: "http://example.com/obj1",
					Start:  0,
					End:    999, // 1000 bytes
				},
			},
			maxChunkSize: 2000,
			expectedChunks: []struct {
				objectURL       string
				start           uint64
				end             uint64
				localFileOffset uint64
			}{
				{
					objectURL:       "http://example.com/obj1",
					start:           0,
					end:             999,
					localFileOffset: 0,
				},
			},
			expectedTotalBytes: 1000,
		},
		{
			name: "single range larger than chunk size",
			ranges: DownloadRanges{
				{
					GETURL: "http://example.com/obj1",
					Start:  0,
					End:    2999, // 3000 bytes
				},
			},
			maxChunkSize: 1000,
			expectedChunks: []struct {
				objectURL       string
				start           uint64
				end             uint64
				localFileOffset uint64
			}{
				{
					objectURL:       "http://example.com/obj1",
					start:           0,
					end:             999, // First chunk of exactly 1000 bytes
					localFileOffset: 0,
				},
				{
					objectURL:       "http://example.com/obj1",
					start:           1000,
					end:             1999, // Second chunk of exactly 1000 bytes
					localFileOffset: 1000,
				},
				{
					objectURL:       "http://example.com/obj1",
					start:           2000,
					end:             2999, // Last chunk of exactly 1000 bytes
					localFileOffset: 2000,
				},
			},
			expectedTotalBytes: 3000,
		},
		{
			name: "single range with exact multiple of chunk size",
			ranges: DownloadRanges{
				{
					GETURL: "http://example.com/obj1",
					Start:  0,
					End:    1999, // 2000 bytes
				},
			},
			maxChunkSize: 1000,
			expectedChunks: []struct {
				objectURL       string
				start           uint64
				end             uint64
				localFileOffset uint64
			}{
				{
					objectURL:       "http://example.com/obj1",
					start:           0,
					end:             999,
					localFileOffset: 0,
				},
				{
					objectURL:       "http://example.com/obj1",
					start:           1000,
					end:             1999,
					localFileOffset: 1000,
				},
			},
			expectedTotalBytes: 2000,
		},
		{
			name: "multiple ranges with various sizes",
			ranges: DownloadRanges{
				{
					GETURL: "http://example.com/obj1",
					Start:  100,
					End:    2099, // 2000 bytes
				},
				{
					GETURL: "http://example.com/obj2",
					Start:  0,
					End:    499, // 500 bytes
				},
				{
					GETURL: "http://example.com/obj3",
					Start:  200,
					End:    1699, // 1500 bytes
				},
			},
			maxChunkSize: 1000,
			expectedChunks: []struct {
				objectURL       string
				start           uint64
				end             uint64
				localFileOffset uint64
			}{
				{
					objectURL:       "http://example.com/obj1",
					start:           100,
					end:             1099, // First chunk of exactly 1000 bytes
					localFileOffset: 0,
				},
				{
					objectURL:       "http://example.com/obj1",
					start:           1100,
					end:             2099, // Second chunk of exactly 1000 bytes
					localFileOffset: 1000,
				},
				{
					objectURL:       "http://example.com/obj2",
					start:           0,
					end:             499, // Third chunk of 500 bytes
					localFileOffset: 2000,
				},
				{
					objectURL:       "http://example.com/obj3",
					start:           200,
					end:             1199, // Fourth chunk of exactly 1000 bytes
					localFileOffset: 2500,
				},
				{
					objectURL:       "http://example.com/obj3",
					start:           1200,
					end:             1699, // Fifth chunk of 500 bytes
					localFileOffset: 3500,
				},
			},
			expectedTotalBytes: 4000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Collect all chunks generated by the Iterate method
			var chunks []downloadSegment
			for chunk := range tt.ranges.Iterate(tt.maxChunkSize) {
				chunks = append(chunks, chunk)
			}

			// Verify number of chunks
			assert.Equal(t, len(tt.expectedChunks), len(chunks), "Incorrect number of chunks generated")

			// Track total bytes for verification
			var totalBytes uint64 = 0

			// Verify each chunk's properties
			for i, expected := range tt.expectedChunks {
				// Skip if we don't have enough chunks
				if i >= len(chunks) {
					break
				}

				chunk := chunks[i]

				// Check URL
				assert.Equal(t, expected.objectURL, chunk.objectAndRange.GETURL,
					"Chunk %d URL mismatch", i)

				// Check start position
				assert.Equal(t, expected.start, chunk.objectAndRange.Start,
					"Chunk %d start position mismatch", i)

				// Check end position
				assert.Equal(t, expected.end, chunk.objectAndRange.End,
					"Chunk %d end position mismatch", i)

				// Check local file offset
				assert.Equal(t, expected.localFileOffset, chunk.localFileOffset,
					"Chunk %d local file offset mismatch", i)

				// Check chunk size is correct (especially for non-final chunks)
				chunkSize := chunk.objectAndRange.End - chunk.objectAndRange.Start + 1

				// Non-final chunks of a range should be exactly maxChunkSize
				if i < len(tt.expectedChunks)-1 &&
					chunks[i].objectAndRange.GETURL == chunks[i+1].objectAndRange.GETURL &&
					chunks[i+1].objectAndRange.Start == chunks[i].objectAndRange.End+1 {
					assert.Equal(t, tt.maxChunkSize, chunkSize,
						"Non-final chunk %d should be exactly maxChunkSize bytes", i)
				}

				// Check chunk boundaries connect properly
				// If this isn't the last chunk of this object, the next chunk should start at end+1
				if i < len(chunks)-1 && chunks[i].objectAndRange.GETURL == chunks[i+1].objectAndRange.GETURL {
					assert.Equal(t, chunks[i].objectAndRange.End+1, chunks[i+1].objectAndRange.Start,
						"Chunk boundaries don't connect properly between chunks %d and %d", i, i+1)
				}

				totalBytes += chunkSize
			}

			// Verify total byte count
			assert.Equal(t, tt.expectedTotalBytes, totalBytes, "Total bytes downloaded doesn't match expected")
		})
	}
}
