package optimize

import (
	"context"
	"fmt"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

// progressBar implements a simple terminal progress bar for optimization operations
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

// update updates the progress bar display for optimization operations
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
		Name:  "optimize",
		Usage: "Automatically optimize datarange storage by aggregating files",
		Description: `Analyze datarange layout and automatically perform optimal aggregation operations.

This command uses greedy optimization rules in priority order:
1. Aggregate small dataranges (<100 datapoints) when at least 10 can be combined
2. Aggregate <10MB dataranges into 10-100MB range
3. Aggregate 10-100MB dataranges into >100MB range  
4. Aggregate 100MB-1GB dataranges into 1-5GB range`,
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
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Show optimization recommendations without executing them",
			},
		},
		Action: func(c *cli.Context) error {

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			clientInstance := client.NewClient(c.String("server-url"))
			datas3tName := c.String("datas3t")
			isDryRun := c.Bool("dry-run")

			fmt.Printf("Analyzing dataranges for optimization opportunities in datas3t '%s'...\n", datas3tName)

			// Get current dataranges
			dataranges, err := clientInstance.ListDataranges(ctx, datas3tName)
			if err != nil {
				return fmt.Errorf("failed to list dataranges: %w", err)
			}

			if len(dataranges) == 0 {
				// Check if the datas3t exists by trying to list all datas3ts
				datas3ts, err := clientInstance.ListDatas3ts(ctx)
				if err != nil {
					return fmt.Errorf("failed to verify datas3t existence: %w", err)
				}

				// Check if our datas3t exists in the list
				found := false
				for _, d := range datas3ts {
					if d.Datas3tName == datas3tName {
						found = true
						break
					}
				}

				if !found {
					return fmt.Errorf("datas3t '%s' not found", datas3tName)
				}

				fmt.Println("No dataranges found to optimize.")
				return nil
			}

			fmt.Printf("Found %d dataranges to analyze.\n", len(dataranges))

			// Create optimizer
			optimizer := NewOptimizer(dataranges)

			// Find the best optimization
			operation := optimizer.FindBestOptimization()
			if operation == nil {
				fmt.Println("No beneficial optimization operations found.")
				return nil
			}

			// Display recommendation
			fmt.Printf("\nOptimization found:\n")
			fmt.Printf("  Type: %s\n", operation.Type)
			fmt.Printf("  Range: %d-%d (%d datapoints)\n",
				operation.FirstDatapoint, operation.LastDatapoint,
				operation.LastDatapoint-operation.FirstDatapoint+1)
			fmt.Printf("  Files: %d → 1\n", len(operation.DatarangeIDs))
			fmt.Printf("  Total size: %s\n", formatBytes(operation.TotalSize))
			fmt.Printf("  Reason: %s\n", operation.Reason)
			fmt.Println()

			if isDryRun {
				fmt.Println("Dry run complete. No aggregations were performed.")
				return nil
			}

			// Execute aggregation
			fmt.Printf("Executing optimization...\n\n")

			// Create progress bar for this operation
			progressBar := newProgressBar(80)
			defer progressBar.finish()

			aggregateOpts := &client.AggregateOptions{
				MaxParallelism:   4,
				MaxRetries:       3,
				ProgressCallback: progressBar.update,
			}

			err = clientInstance.AggregateDataRanges(
				ctx,
				datas3tName,
				operation.FirstDatapoint,
				operation.LastDatapoint,
				aggregateOpts,
			)
			if err != nil {
				return fmt.Errorf("failed to aggregate data ranges: %w", err)
			}

			fmt.Printf("✅ Success: Aggregated %d files covering datapoints %d-%d\n",
				len(operation.DatarangeIDs), operation.FirstDatapoint, operation.LastDatapoint)

			return nil
		},
	}
}

func formatBytes(bytes int64) string {
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
