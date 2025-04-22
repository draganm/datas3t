package upload

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
		file      string
	}{}

	return &cli.Command{
		Name:      "upload",
		Usage:     "Upload datarange to a dataset",
		ArgsUsage: "DATASET_ID",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "file",
				Required:    true,
				Usage:       "File to upload",
				Destination: &cfg.file,
				EnvVars:     []string{"DATAS3T_FILE"},
			},
			&cli.StringFlag{
				Name:        "server-url",
				Required:    true,
				Usage:       "URL of the Datas3t server",
				Destination: &cfg.serverURL,
				EnvVars:     []string{"DATAS3T_SERVER_URL"},
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("expected exactly one argument: DATASET_ID")
			}

			datasetID := c.Args().Get(0)

			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			// Check if file exists
			fileInfo, err := os.Stat(cfg.file)
			if err != nil {
				return fmt.Errorf("failed to access file: %w", err)
			}

			totalSize := fileInfo.Size()
			log.Info("Starting multipart upload", "file", cfg.file, "size", formatSize(totalSize))

			// Create a progress callback that displays progress
			lastLogTime := time.Now()
			var lastBytes int64
			startTime := time.Now()

			progressCallback := func(partNum, totalParts int, partBytes, uploadedBytes, totalBytes int64) {
				// Only update progress every 500ms to avoid excessive logging
				now := time.Now()
				if now.Sub(lastLogTime) < 500*time.Millisecond && uploadedBytes != totalBytes {
					return
				}

				// Calculate speed
				elapsed := now.Sub(startTime).Seconds()
				bytesPerSec := float64(uploadedBytes) / elapsed

				// Calculate ETA
				var eta string
				if bytesPerSec > 0 {
					etaSeconds := float64(totalBytes-uploadedBytes) / bytesPerSec
					eta = formatDuration(time.Duration(etaSeconds) * time.Second)
				} else {
					eta = "calculating..."
				}

				// Calculate progress percentage
				percentage := float64(uploadedBytes) / float64(totalBytes) * 100

				// Calculate instantaneous transfer rate
				instantBytesPerSec := float64(uploadedBytes-lastBytes) / now.Sub(lastLogTime).Seconds()

				// Update last values for next calculation
				lastLogTime = now
				lastBytes = uploadedBytes

				// Display progress
				log.Info(fmt.Sprintf("Progress: %.1f%%", percentage),
					"part", fmt.Sprintf("%d/%d", partNum, totalParts),
					"bytes", fmt.Sprintf("%s/%s", formatSize(uploadedBytes), formatSize(totalBytes)),
					"speed", fmt.Sprintf("%s/s", formatSize(int64(instantBytesPerSec))),
					"eta", eta,
				)
			}

			// Upload file using multipart upload
			response, err := cl.UploadFileWithMultipart(c.Context, datasetID, cfg.file, progressCallback)
			if err != nil {
				return fmt.Errorf("failed to upload file: %w", err)
			}

			// Display upload completion information
			totalTime := time.Since(startTime)
			avgSpeed := float64(totalSize) / totalTime.Seconds()

			log.Info("Upload complete",
				"dataset", datasetID,
				"file", cfg.file,
				"size", formatSize(totalSize),
				"datapoints", response.NumDataPoints,
				"time", formatDuration(totalTime),
				"avg_speed", fmt.Sprintf("%s/s", formatSize(int64(avgSpeed))),
			)

			return nil
		},
	}
}

// formatSize formats a byte count as a human-readable string (KB, MB, GB, etc.)
func formatSize(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatDuration formats a duration in a more human-readable format
func formatDuration(d time.Duration) string {
	if d.Hours() >= 1 {
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		return fmt.Sprintf("%dh %dm", h, m)
	} else if d.Minutes() >= 1 {
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		return fmt.Sprintf("%dm %ds", m, s)
	}
	return fmt.Sprintf("%.1fs", d.Seconds())
}
