package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/draganm/datas3t/cmd/aggregator/optimizer"
	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

const (
	// Parameters for aggregation planning
	defaultTargetDatarangeSize = 100 * 1024 * 1024 // 100MB
	defaultInterval            = 30 * time.Minute  // Default interval
)

// ContinuousRange represents a range with datapoint keys
type ContinuousRange struct {
	FromDatapointKey uint64
	ToDatapointKey   uint64
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := struct {
		serverURL string
		interval  time.Duration
	}{}

	app := &cli.App{
		Name:  "aggregator",
		Usage: "Datas3t datarange aggregator utility",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server-url",
				Usage:       "Datas3t server URL",
				Value:       "http://localhost:8080",
				Destination: &cfg.serverURL,
				EnvVars:     []string{"DATAS3T_SERVER_URL"},
			},
			&cli.DurationFlag{
				Name:        "interval",
				Usage:       "Aggregation interval (e.g. 30m, 1h, 2h30m)",
				Value:       defaultInterval,
				Destination: &cfg.interval,
				EnvVars:     []string{"DATAS3T_AGGREGATION_INTERVAL"},
			},
		},
		Action: func(c *cli.Context) error {
			log.Info("starting aggregator",
				"server_url", cfg.serverURL,
				"interval", cfg.interval,
			)

			// Create client
			client, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			// Create context that can be cancelled
			ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt)
			defer cancel()

			// Run the aggregator
			return runAggregator(
				ctx,
				client,
				log,
				cfg.interval,
			)
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("terminated", "error", err)
		os.Exit(1)
	}
}

func runAggregator(
	ctx context.Context,
	c *client.Client,
	log *slog.Logger,
	interval time.Duration,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run once immediately
	err := performAggregation(ctx, c, log)
	if err != nil {
		log.Error("aggregation failed", "error", err)
	}

	// Then run periodically
	for {
		select {
		case <-ticker.C:
			err := performAggregation(ctx, c, log)
			if err != nil {
				log.Error("aggregation failed", "error", err)
			}
		case <-ctx.Done():
			log.Info("context cancelled, stopping aggregator")
			return nil
		}
	}
}

func performAggregation(
	ctx context.Context,
	c *client.Client,
	log *slog.Logger,
) error {
	log.Info("starting aggregation cycle")

	// List all datasets
	datasets, err := c.ListDatasets(ctx)
	if err != nil {
		return fmt.Errorf("failed to list datasets: %w", err)
	}

	log.Info("found datasets", "count", len(datasets))

	// Process each dataset
	for _, dataset := range datasets {
		log.Info("processing dataset", "id", dataset.ID)

		err := processDataset(ctx, c, dataset.ID, log)
		if err != nil {
			log.Error("failed to process dataset", "id", dataset.ID, "error", err)
			// Continue with the next dataset
			continue
		}
	}

	log.Info("aggregation cycle completed")
	return nil
}

func processDataset(
	ctx context.Context,
	c *client.Client,
	datasetID string,
	log *slog.Logger,
) error {
	// Get dataranges for the dataset
	dataranges, err := c.GetDataranges(ctx, datasetID)
	if err != nil {
		return fmt.Errorf("failed to get dataranges: %w", err)
	}

	log.Info("found dataranges", "dataset", datasetID, "count", len(dataranges))

	// Skip if there are not enough dataranges to aggregate
	if len(dataranges) <= 1 {
		log.Info("not enough dataranges to aggregate", "dataset", datasetID)
		return nil
	}

	// Iteratively apply optimization steps until there's nothing more to optimize
	optimizationRounds := 0
	totalMergesApplied := 0

	for {
		// Create optimizer with current dataranges
		opt := optimizer.NewDataRangeMergeOptimizer(dataranges)

		// Get the next best merge proposal
		proposal := opt.ProposeNextMerge()
		if proposal == nil {
			log.Info("optimization complete - no more merges possible",
				"dataset", datasetID,
				"final_ranges", len(dataranges),
				"rounds", optimizationRounds,
				"total_merges", totalMergesApplied)
			break
		}

		optimizationRounds++

		log.Info("applying optimization",
			"dataset", datasetID,
			"round", optimizationRounds,
			"merging_ranges", len(proposal.DataRangeIndices),
			"result_size_mb", float64(proposal.ResultSize)/(1024*1024),
			"efficiency", proposal.Efficiency)

		// Convert the merge proposal to continuous range format for executeAggregationPlan
		// First, find the min and max datapoint keys from the ranges to be merged
		var minKey, maxKey uint64
		for i, rangeIdx := range proposal.DataRangeIndices {
			rangeToMerge := dataranges[rangeIdx]
			if i == 0 {
				minKey = rangeToMerge.MinDatapointKey
				maxKey = rangeToMerge.MaxDatapointKey
			} else {
				if rangeToMerge.MinDatapointKey < minKey {
					minKey = rangeToMerge.MinDatapointKey
				}
				if rangeToMerge.MaxDatapointKey > maxKey {
					maxKey = rangeToMerge.MaxDatapointKey
				}
			}
		}

		// Create a ContinuousRange structure for executeAggregationPlan
		plan := ContinuousRange{
			FromDatapointKey: minKey,
			ToDatapointKey:   maxKey,
		}

		// Execute the aggregation
		err := executeAggregationPlan(ctx, c, datasetID, plan, log)
		if err != nil {
			log.Error("failed to execute optimization",
				"dataset", datasetID,
				"round", optimizationRounds,
				"error", err)
			// Stop optimization on error
			return fmt.Errorf("failed to execute optimization round %d: %w", optimizationRounds, err)
		}

		totalMergesApplied++

		// Get updated dataranges after the merge
		dataranges, err = c.GetDataranges(ctx, datasetID)
		if err != nil {
			return fmt.Errorf("failed to get updated dataranges after optimization: %w", err)
		}

		log.Info("optimization applied successfully",
			"dataset", datasetID,
			"round", optimizationRounds,
			"new_range_count", len(dataranges))

		// Safety check to prevent infinite loops
		if optimizationRounds > 1000 {
			log.Warn("stopping optimization due to maximum rounds limit",
				"dataset", datasetID,
				"rounds", optimizationRounds)
			break
		}
	}

	return nil
}

func executeAggregationPlan(ctx context.Context, c *client.Client, datasetID string, plan ContinuousRange, log *slog.Logger) error {
	log.Info("executing aggregation plan",
		"dataset", datasetID,
		"start_key", plan.FromDatapointKey,
		"end_key", plan.ToDatapointKey,
	)

	// Call the aggregate endpoint
	resp, err := c.AggregateDatarange(ctx, datasetID, plan.FromDatapointKey, plan.ToDatapointKey)
	if err != nil {
		return fmt.Errorf("failed to aggregate dataranges: %w", err)
	}

	log.Info("successfully aggregated dataranges",
		"dataset", datasetID,
		"start_key", resp.StartKey,
		"end_key", resp.EndKey,
		"ranges_replaced", resp.RangesReplaced,
		"new_object_key", resp.NewObjectKey,
		"size_bytes", resp.SizeBytes)

	return nil
}
