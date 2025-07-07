package aggregate

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

// progressBar implements a simple terminal progress bar for aggregation
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

// update updates the progress bar display for aggregation operations
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
	currentMB := float64(info.CompletedBytes) / (1024 * 1024)

	// Format phase message
	var phaseMsg string
	switch info.Phase {
	case client.PhaseStartingAggregate:
		phaseMsg = "Starting aggregation..."
	case client.PhaseDownloadingSources:
		phaseMsg = "Downloading source dataranges..."
	case client.PhaseMergingTars:
		phaseMsg = "Merging TAR files..."
	case client.PhaseUploadingAggregate:
		phaseMsg = "Uploading aggregate..."
	case client.PhaseCompletingAggregate:
		phaseMsg = "Completing aggregation..."
	default:
		phaseMsg = string(info.Phase)
	}

	// Build the output line
	var output string
	if info.TotalBytes > 0 {
		output = fmt.Sprintf("\r[%s] %5.1f%% (%.1f/%.1f MB) - %s",
			bar, info.PercentComplete, currentMB, totalMB, phaseMsg)
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
		Name:  "aggregate",
		Usage: "Aggregate multiple dataranges into a single larger datarange",
		Description: `Combine multiple small dataranges into a single larger datarange for improved efficiency.

This operation:
- Downloads all source dataranges in the specified range
- Merges them into a single TAR archive with continuous datapoint numbering
- Uploads the merged archive to S3
- Atomically replaces the original dataranges with the new aggregate

The operation validates that the datapoint range is fully covered by existing dataranges
with no gaps before proceeding.`,
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
				Usage:    "First datapoint index to include in aggregate",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "last-datapoint",
				Usage:    "Last datapoint index to include in aggregate",
				Required: true,
			},
			&cli.IntFlag{
				Name:  "max-parallelism",
				Usage: "Maximum number of concurrent operations",
				Value: 4,
			},
			&cli.IntFlag{
				Name:  "max-retries",
				Usage: "Maximum number of retry attempts per operation",
				Value: 3,
			},
		},
		Action: aggregateAction,
	}
}

func aggregateAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))

	datas3tName := c.String("datas3t")

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

	fmt.Printf("Aggregating datapoints %d-%d in datas3t '%s'...\n", firstDatapoint, lastDatapoint, datas3tName)

	// Create progress bar
	progressBar := newProgressBar(80)

	// Set up aggregation options with progress callback
	opts := &client.AggregateOptions{
		MaxParallelism: c.Int("max-parallelism"),
		MaxRetries:     c.Int("max-retries"),
		ProgressCallback: progressBar.update,
	}

	// Start aggregation with progress tracking
	err = clientInstance.AggregateDataRanges(context.Background(), datas3tName, firstDatapoint, lastDatapoint, opts)

	// Finish progress bar
	progressBar.finish()

	if err != nil {
		return fmt.Errorf("failed to aggregate dataranges: %w", err)
	}

	fmt.Printf("Successfully aggregated datapoints %d-%d in datas3t '%s'\n", firstDatapoint, lastDatapoint, datas3tName)
	return nil
}