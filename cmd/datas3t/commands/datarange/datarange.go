package datarange

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

// progressBar implements a simple terminal progress bar
type progressBar struct {
	mu            sync.Mutex
	width         int
	lastPrintTime time.Time
	lastOutput    string
}

// newProgressBar creates a new progress bar with the specified width
func newProgressBar(width int) *progressBar {
	if width < 20 {
		width = 60 // default width
	}
	return &progressBar{
		width: width,
	}
}

// update updates the progress bar display
func (pb *progressBar) update(info client.ProgressInfo) {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Throttle updates to avoid screen flicker (max 10 updates per second)
	now := time.Now()
	if now.Sub(pb.lastPrintTime) < 100*time.Millisecond && info.PercentComplete < 100 {
		return
	}
	pb.lastPrintTime = now

	// Create progress bar visualization
	progressWidth := pb.width - 20 // Leave space for percentage and other info
	filled := int(info.PercentComplete * float64(progressWidth) / 100.0)
	if filled > progressWidth {
		filled = progressWidth
	}

	bar := strings.Repeat("█", filled) + strings.Repeat("▒", progressWidth-filled)

	// Format data sizes
	totalMB := float64(info.TotalBytes) / (1024 * 1024)
	completedMB := float64(info.CompletedBytes) / (1024 * 1024)

	// Format speed
	var speedStr string
	if info.Speed > 0 {
		speedMB := info.Speed / (1024 * 1024)
		speedStr = fmt.Sprintf("%.1f MB/s", speedMB)
	} else {
		speedStr = "--.- MB/s"
	}

	// Format ETA
	var etaStr string
	if info.EstimatedETA > 0 {
		if info.EstimatedETA < time.Minute {
			etaStr = fmt.Sprintf("%ds", int(info.EstimatedETA.Seconds()))
		} else if info.EstimatedETA < time.Hour {
			etaStr = fmt.Sprintf("%dm%ds", int(info.EstimatedETA.Minutes()), int(info.EstimatedETA.Seconds())%60)
		} else {
			etaStr = fmt.Sprintf("%dh%dm", int(info.EstimatedETA.Hours()), int(info.EstimatedETA.Minutes())%60)
		}
	} else {
		etaStr = "--:--"
	}

	// Format phase-specific message
	var phaseMsg string
	switch info.Phase {
	case client.PhaseAnalyzing:
		phaseMsg = "Analyzing TAR file..."
	case client.PhaseIndexing:
		phaseMsg = "Generating index..."
	case client.PhaseStarting:
		phaseMsg = "Starting upload session..."
	case client.PhaseUploading:
		phaseMsg = info.CurrentStep
	case client.PhaseUploadingIndex:
		phaseMsg = "Uploading index..."
	case client.PhaseCompleting:
		phaseMsg = "Completing upload..."
	}

	// Build the output line
	var output string
	if info.Phase == client.PhaseUploading || info.PercentComplete > 0 {
		output = fmt.Sprintf("\r[%s] %5.1f%% (%.1f/%.1f MB) %s ETA: %s - %s",
			bar, info.PercentComplete, completedMB, totalMB, speedStr, etaStr, phaseMsg)
	} else {
		output = fmt.Sprintf("\r%s", phaseMsg)
	}

	// Clear previous line if new output is shorter
	if len(output) < len(pb.lastOutput) {
		fmt.Print("\r" + strings.Repeat(" ", len(pb.lastOutput)) + "\r")
	}

	fmt.Print(output)
	pb.lastOutput = output
}

// finish completes the progress bar
func (pb *progressBar) finish() {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	fmt.Println() // Move to next line
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "datarange",
		Usage: "Manage datarange operations",
		Subcommands: []*cli.Command{
			{
				Name:  "upload-tar",
				Usage: "Upload a TAR file as a datarange",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "server-url",
						Value:   "http://localhost:8765",
						Usage:   "Server URL",
						EnvVars: []string{"DATAS3T_SERVER_URL"},
					},
					&cli.StringFlag{
						Name:     "datas3t",
						Usage:    "Datas3t name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "file",
						Usage:    "Path to TAR file to upload",
						Required: true,
					},
					&cli.IntFlag{
						Name:  "max-parallelism",
						Usage: "Maximum number of concurrent uploads",
						Value: 6,
					},
					&cli.IntFlag{
						Name:  "max-retries",
						Usage: "Maximum number of retry attempts per chunk",
						Value: 3,
					},
				},
				Action: uploadTarAction,
			},
			{
				Name:  "download-tar",
				Usage: "Download a range of datapoints as a TAR file",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "server-url",
						Value:   "http://localhost:8765",
						Usage:   "Server URL",
						EnvVars: []string{"DATAS3T_SERVER_URL"},
					},
					&cli.StringFlag{
						Name:     "datas3t",
						Usage:    "Datas3t name",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "first-datapoint",
						Usage:    "First datapoint to download",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "last-datapoint",
						Usage:    "Last datapoint to download",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "output",
						Usage:    "Output TAR file path",
						Required: true,
					},
					&cli.IntFlag{
						Name:  "max-parallelism",
						Usage: "Maximum number of concurrent downloads",
						Value: 4,
					},
					&cli.IntFlag{
						Name:  "max-retries",
						Usage: "Maximum number of retry attempts per chunk",
						Value: 3,
					},
					&cli.Int64Flag{
						Name:  "chunk-size",
						Usage: "Size of each download chunk in bytes",
						Value: 5 * 1024 * 1024, // 5MB
					},
				},
				Action: downloadTarAction,
			},
		},
	}
}

func uploadTarAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))

	filePath := c.String("file")
	datas3tName := c.String("datas3t")

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file '%s': %w", filePath, err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %w", err)
	}

	fmt.Printf("Uploading '%s' to datas3t '%s' (size: %.1f MB)...\n",
		filePath, datas3tName, float64(fileInfo.Size())/(1024*1024))

	// Create progress bar
	progressBar := newProgressBar(80)

	// Set up upload options with progress callback
	opts := &client.UploadOptions{
		MaxParallelism:   c.Int("max-parallelism"),
		MaxRetries:       c.Int("max-retries"),
		ProgressCallback: progressBar.update,
	}

	// Start upload with progress tracking
	err = clientInstance.UploadDataRangeFile(context.Background(), datas3tName, file, fileInfo.Size(), opts)

	// Finish progress bar
	progressBar.finish()

	if err != nil {
		return fmt.Errorf("failed to upload datarange: %w", err)
	}

	fmt.Printf("Successfully uploaded datarange to datas3t '%s'\n", datas3tName)
	return nil
}

func downloadTarAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))

	datas3tName := c.String("datas3t")
	outputPath := c.String("output")

	// Parse datapoint range
	firstDatapointStr := c.String("first-datapoint")
	lastDatapointStr := c.String("last-datapoint")

	firstDatapoint, err := strconv.ParseUint(firstDatapointStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid first-datapoint '%s': %w", firstDatapointStr, err)
	}

	lastDatapoint, err := strconv.ParseUint(lastDatapointStr, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid last-datapoint '%s': %w", lastDatapointStr, err)
	}

	if firstDatapoint > lastDatapoint {
		return fmt.Errorf("first-datapoint (%d) cannot be greater than last-datapoint (%d)", firstDatapoint, lastDatapoint)
	}

	fmt.Printf("Downloading datapoints %d-%d from datas3t '%s' to '%s'...\n", firstDatapoint, lastDatapoint, datas3tName, outputPath)

	err = clientInstance.DownloadDatapointsTarWithOptions(context.Background(), datas3tName, firstDatapoint, lastDatapoint, outputPath, nil)
	if err != nil {
		return fmt.Errorf("failed to download datapoints: %w", err)
	}

	fmt.Printf("Successfully downloaded datapoints to '%s'\n", outputPath)
	return nil
}
