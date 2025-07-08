package optimize

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/draganm/datas3t/client"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "optimize",
		Usage: "Automatically optimize datarange storage by aggregating files based on size and adjacency",
		Description: `Analyze datarange layout and automatically perform optimal aggregation operations.

This command:
- Analyzes existing dataranges to identify optimization opportunities
- Uses an Aggregation Value Score (AVS) algorithm to prioritize operations
- Automatically performs beneficial aggregations using the existing aggregate functionality
- Supports both one-time optimization and continuous monitoring modes

The optimization algorithm considers:
- Small file aggregation (combine many small files)
- Adjacent ID range aggregation (merge consecutive datapoint ranges)
- Size bucket aggregation (group similarly sized files)

Each potential aggregation is scored based on:
- Objects reduced (fewer files to manage)
- Size efficiency (approach target size)
- Consecutive bonus (bonus for adjacent datapoint ranges)
- Operation cost (download/upload overhead)`,
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
			&cli.BoolFlag{
				Name:  "daemon",
				Usage: "Run continuously, monitoring for optimization opportunities",
			},
			&cli.DurationFlag{
				Name:  "interval",
				Usage: "Interval between optimization checks in daemon mode",
				Value: 5 * time.Minute,
			},
			&cli.Float64Flag{
				Name:  "min-score",
				Usage: "Minimum AVS score required to perform aggregation",
				Value: 1.0,
			},
			&cli.StringFlag{
				Name:  "target-size",
				Usage: "Target size for aggregated files (e.g., 1GB, 2GB)",
				Value: "1GB",
			},
			&cli.StringFlag{
				Name:  "max-aggregate-size",
				Usage: "Maximum size for aggregated files (e.g., 5GB)",
				Value: "5GB",
			},
			&cli.IntFlag{
				Name:  "max-operations",
				Usage: "Maximum number of aggregation operations to perform in one run",
				Value: 10,
			},
			&cli.IntFlag{
				Name:  "max-parallelism",
				Usage: "Maximum number of concurrent operations for each aggregation",
				Value: 4,
			},
			&cli.IntFlag{
				Name:  "max-retries",
				Usage: "Maximum number of retry attempts per operation",
				Value: 3,
			},
		},
		Action: optimizeAction,
	}
}

func optimizeAction(c *cli.Context) error {
	clientInstance := client.NewClient(c.String("server-url"))
	datas3tName := c.String("datas3t")
	isDryRun := c.Bool("dry-run")
	isDaemon := c.Bool("daemon")
	interval := c.Duration("interval")
	minScore := c.Float64("min-score")
	maxOperations := c.Int("max-operations")

	// Parse size parameters
	targetSize, err := parseSize(c.String("target-size"))
	if err != nil {
		return fmt.Errorf("invalid target-size: %w", err)
	}

	maxAggregateSize, err := parseSize(c.String("max-aggregate-size"))
	if err != nil {
		return fmt.Errorf("invalid max-aggregate-size: %w", err)
	}

	if isDaemon {
		return runDaemonMode(clientInstance, datas3tName, isDryRun, interval, minScore, targetSize, maxAggregateSize, maxOperations, c)
	}

	return runSingleOptimization(clientInstance, datas3tName, isDryRun, minScore, targetSize, maxAggregateSize, maxOperations, c)
}

func runSingleOptimization(clientInstance *client.Client, datas3tName string, isDryRun bool, minScore float64, targetSize, maxAggregateSize int64, maxOperations int, c *cli.Context) error {
	fmt.Printf("Analyzing dataranges for optimization opportunities in datas3t '%s'...\n", datas3tName)

	// Get current dataranges
	dataranges, err := clientInstance.ListDataranges(context.Background(), datas3tName)
	if err != nil {
		return fmt.Errorf("failed to list dataranges: %w", err)
	}

	if len(dataranges) == 0 {
		// Check if the datas3t exists by trying to list all datas3ts
		datas3ts, err := clientInstance.ListDatas3ts(context.Background())
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

	// Convert to optimizer format
	tarFiles := ConvertFromDatarangeInfo(dataranges)

	// Create optimizer
	optimizer := NewAggregationOptimizer(tarFiles)
	optimizer.SetThresholds(minScore, targetSize, maxAggregateSize)

	// Find optimization opportunities
	operations := optimizer.FindAllBeneficialAggregations()

	if len(operations) == 0 {
		fmt.Println("No beneficial aggregation operations found.")
		return nil
	}

	// Limit operations
	if len(operations) > maxOperations {
		operations = operations[:maxOperations]
	}

	// Display recommendations
	fmt.Printf("\nFound %d beneficial aggregation operations:\n\n", len(operations))
	for i, op := range operations {
		fmt.Printf("Operation %d (Score: %.2f):\n", i+1, op.Score)
		fmt.Printf("  Range: %d-%d (%d datapoints)\n", op.FirstDatapoint, op.LastDatapoint, op.LastDatapoint-op.FirstDatapoint+1)
		fmt.Printf("  Files: %d → 1 (reduces %d objects)\n", len(op.Files), len(op.Files)-1)
		
		totalSize := int64(0)
		for _, f := range op.Files {
			totalSize += f.Size
		}
		fmt.Printf("  Total size: %s\n", formatBytes(totalSize))
		fmt.Printf("  Files to aggregate:\n")
		for _, f := range op.Files {
			fmt.Printf("    - %s: %d-%d (%s)\n", f.ID, f.MinID, f.MaxID, formatBytes(f.Size))
		}
		fmt.Println()
	}

	if isDryRun {
		fmt.Println("Dry run complete. No aggregations were performed.")
		return nil
	}

	// Execute aggregations
	fmt.Println("Executing aggregation operations...")
	
	aggregateOpts := &client.AggregateOptions{
		MaxParallelism: c.Int("max-parallelism"),
		MaxRetries:     c.Int("max-retries"),
	}

	successful := 0
	for i, op := range operations {
		fmt.Printf("\nExecuting operation %d/%d (Score: %.2f)...\n", i+1, len(operations), op.Score)
		
		err := clientInstance.AggregateDataRanges(
			context.Background(),
			datas3tName,
			op.FirstDatapoint,
			op.LastDatapoint,
			aggregateOpts,
		)
		
		if err != nil {
			fmt.Printf("  ❌ Failed: %v\n", err)
			continue
		}
		
		successful++
		fmt.Printf("  ✅ Success: Aggregated %d files covering datapoints %d-%d\n", 
			len(op.Files), op.FirstDatapoint, op.LastDatapoint)
	}

	fmt.Printf("\nOptimization complete: %d/%d operations successful\n", successful, len(operations))
	return nil
}

func runDaemonMode(clientInstance *client.Client, datas3tName string, isDryRun bool, interval time.Duration, minScore float64, targetSize, maxAggregateSize int64, maxOperations int, c *cli.Context) error {
	fmt.Printf("Starting optimization daemon for datas3t '%s' (interval: %v)...\n", datas3tName, interval)
	
	if isDryRun {
		fmt.Println("Running in dry-run mode - no aggregations will be performed.")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run initial optimization
	err := runSingleOptimization(clientInstance, datas3tName, isDryRun, minScore, targetSize, maxAggregateSize, maxOperations, c)
	if err != nil {
		fmt.Printf("Initial optimization failed: %v\n", err)
	}

	for {
		select {
		case <-ticker.C:
			fmt.Printf("\n--- Optimization check at %s ---\n", time.Now().Format(time.RFC3339))
			err := runSingleOptimization(clientInstance, datas3tName, isDryRun, minScore, targetSize, maxAggregateSize, maxOperations, c)
			if err != nil {
				fmt.Printf("Optimization failed: %v\n", err)
			}
		}
	}
}

func parseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, fmt.Errorf("empty size string")
	}

	// Handle common size suffixes
	multiplier := int64(1)
	if len(sizeStr) > 2 {
		suffix := sizeStr[len(sizeStr)-2:]
		switch suffix {
		case "GB":
			multiplier = 1024 * 1024 * 1024
			sizeStr = sizeStr[:len(sizeStr)-2]
		case "MB":
			multiplier = 1024 * 1024
			sizeStr = sizeStr[:len(sizeStr)-2]
		case "KB":
			multiplier = 1024
			sizeStr = sizeStr[:len(sizeStr)-2]
		}
	}

	value, err := strconv.ParseFloat(sizeStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size value: %w", err)
	}

	return int64(value * float64(multiplier)), nil
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