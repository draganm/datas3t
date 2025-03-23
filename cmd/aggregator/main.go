package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"time"

	"github.com/draganm/datas3t/cmd/aggregator/planner"
	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

const (
	// Parameters for aggregation planning
	defaultTargetDatarangeSize = 100 * 1024 * 1024 // 100MB
	defaultInterval            = 30 * time.Minute  // Default interval
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg := struct {
		serverURL           string
		interval            time.Duration
		targetDatarangeSize int64
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
			&cli.Int64Flag{
				Name:        "target-size",
				Usage:       "Target datarange size in bytes",
				Value:       defaultTargetDatarangeSize,
				Destination: &cfg.targetDatarangeSize,
				EnvVars:     []string{"DATAS3T_TARGET_DATARANGE_SIZE"},
			},
		},
		Action: func(c *cli.Context) error {
			log.Info("starting aggregator",
				"server_url", cfg.serverURL,
				"interval", cfg.interval,
				"min_datarange_size", cfg.targetDatarangeSize,
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
				cfg.targetDatarangeSize,
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
	targetDatarangeSize int64,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Run once immediately
	err := performAggregation(ctx, c, log, targetDatarangeSize)
	if err != nil {
		log.Error("aggregation failed", "error", err)
	}

	// Then run periodically
	for {
		select {
		case <-ticker.C:
			err := performAggregation(ctx, c, log, targetDatarangeSize)
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
	targetDatarangeSize int64,
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

		err := processDataset(ctx, c, dataset.ID, log, targetDatarangeSize)
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
	targetDatarangeSize int64,
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

	// Create aggregation plan using the planner package
	plans := planner.CreatePlans(dataranges, log, targetDatarangeSize)
	log.Info("created aggregation plans", "dataset", datasetID, "plan_count", len(plans))

	// Execute each plan
	for _, plan := range plans {
		err := executeAggregationPlan(ctx, c, datasetID, plan, log)
		if err != nil {
			log.Error("failed to execute aggregation plan",
				"dataset", datasetID,
				"start_key", plan.StartKey,
				"end_key", plan.EndKey,
				"error", err)
			// Continue with the next plan
			continue
		}
	}

	return nil
}

func executeAggregationPlan(ctx context.Context, c *client.Client, datasetID string, plan planner.AggregationPlan, log *slog.Logger) error {
	log.Info("executing aggregation plan",
		"dataset", datasetID,
		"start_key", plan.StartKey,
		"end_key", plan.EndKey,
		"range_count", len(plan.Ranges))

	// Call the aggregate endpoint
	resp, err := c.AggregateDatarange(ctx, datasetID, plan.StartKey, plan.EndKey)
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
