package optimizeall

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/draganm/datas3t/client"
	"github.com/draganm/datas3t/cmd/datas3t/optimize"
	"github.com/urfave/cli/v2"
)

// progressLogger implements periodic logging of aggregation progress
type progressLogger struct {
	mu           sync.Mutex
	logger       *slog.Logger
	datas3tName  string
	lastLogTime  time.Time
	logInterval  time.Duration
	startTime    time.Time
	lastProgress client.ProgressInfo
}

// newProgressLogger creates a new progress logger
func newProgressLogger(logger *slog.Logger, datas3tName string, logInterval time.Duration) *progressLogger {
	return &progressLogger{
		logger:      logger,
		datas3tName: datas3tName,
		logInterval: logInterval,
		startTime:   time.Now(),
	}
}

// update handles progress updates from the aggregation operation
func (pl *progressLogger) update(info client.ProgressInfo) {
	pl.mu.Lock()
	defer pl.mu.Unlock()

	pl.lastProgress = info
	now := time.Now()

	// Log immediately on phase changes or completion
	shouldLog := false
	if info.Phase != pl.lastProgress.Phase || info.PercentComplete >= 100 {
		shouldLog = true
	} else if now.Sub(pl.lastLogTime) >= pl.logInterval {
		// Log periodically based on interval
		shouldLog = true
	}

	if shouldLog {
		pl.lastLogTime = now

		// Convert phase to string
		phaseStr := string(info.Phase)
		switch info.Phase {
		case client.PhaseStartingAggregate:
			phaseStr = "starting"
		case client.PhaseDownloadingSources:
			phaseStr = "downloading"
		case client.PhaseMergingTars:
			phaseStr = "merging"
		case client.PhaseUploadingAggregate:
			phaseStr = "uploading"
		case client.PhaseCompletingAggregate:
			phaseStr = "completing"
		}

		pl.logger.Info("aggregation progress",
			"datas3t", pl.datas3tName,
			"phase", phaseStr,
			"percent", fmt.Sprintf("%.1f", info.PercentComplete),
			"bytes_completed", info.CompletedBytes,
			"bytes_total", info.TotalBytes,
			"elapsed_seconds", int(now.Sub(pl.startTime).Seconds()),
		)
	}
}

func Command() *cli.Command {
	return &cli.Command{
		Name:  "optimize-all",
		Usage: "Continuously optimize all datas3ts in a loop",
		Description: `Continuously cycle through all known datas3ts and optimize their datarange storage.

This command is designed to run indefinitely in a pod or container environment.
It will:
- List all datas3ts and attempt optimization for each
- If no optimizations are possible in a complete cycle, back off for 5 minutes
- Continue cycling until interrupted
- Log all operations in structured JSON format using slog
- Provide periodic progress updates during aggregation operations`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "server-url",
				Value:   "http://localhost:8765",
				Usage:   "Server URL",
				EnvVars: []string{"DATAS3T_SERVER_URL"},
			},
			&cli.StringFlag{
				Name:    "temp-dir",
				Value:   "",
				Usage:   "Directory for temporary downloads (default: system temp)",
				EnvVars: []string{"DATAS3T_TEMP_DIR"},
			},
			&cli.DurationFlag{
				Name:  "progress-interval",
				Value: 10 * time.Second,
				Usage: "Interval for progress logging updates",
			},
			&cli.DurationFlag{
				Name:  "backoff-duration",
				Value: 5 * time.Minute,
				Usage: "Duration to wait when no optimizations are possible",
			},
		},
		Action: func(c *cli.Context) error {
			// Setup structured JSON logging
			logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
				Level: slog.LevelInfo,
			}))
			slog.SetDefault(logger)

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			clientInstance := client.NewClient(c.String("server-url"))
			tempDir := c.String("temp-dir")
			if tempDir == "" {
				tempDir = os.TempDir()
			}
			progressInterval := c.Duration("progress-interval")
			backoffDuration := c.Duration("backoff-duration")

			logger.Info("starting continuous optimizer",
				"server_url", c.String("server-url"),
				"temp_dir", tempDir,
				"progress_interval", progressInterval.String(),
				"backoff_duration", backoffDuration.String(),
			)

			cycleNumber := 0
			for {
				select {
				case <-ctx.Done():
					logger.Info("shutting down continuous optimizer", "reason", ctx.Err().Error())
					return nil
				default:
				}

				cycleNumber++
				cycleStart := time.Now()
				optimizationsPerformed := 0

				logger.Info("starting optimization cycle", "cycle", cycleNumber)

				// List all datas3ts
				datas3ts, err := clientInstance.ListDatas3ts(ctx)
				if err != nil {
					logger.Error("failed to list datas3ts",
						"error", err.Error(),
						"cycle", cycleNumber,
					)
					// Wait a bit before retrying
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(30 * time.Second):
						continue
					}
				}

				logger.Info("found datas3ts",
					"count", len(datas3ts),
					"cycle", cycleNumber,
				)

				// Process each datas3t
				for _, ds3t := range datas3ts {
					select {
					case <-ctx.Done():
						logger.Info("optimization interrupted", "datas3t", ds3t.Datas3tName)
						return nil
					default:
					}

					logger.Info("analyzing datas3t",
						"datas3t", ds3t.Datas3tName,
						"datarange_count", ds3t.DatarangeCount,
						"total_datapoints", ds3t.TotalDatapoints,
						"total_bytes", ds3t.TotalBytes,
					)

					// Get dataranges for this datas3t
					dataranges, err := clientInstance.ListDataranges(ctx, ds3t.Datas3tName)
					if err != nil {
						logger.Error("failed to list dataranges",
							"datas3t", ds3t.Datas3tName,
							"error", err.Error(),
						)
						continue
					}

					if len(dataranges) == 0 {
						logger.Info("no dataranges to optimize",
							"datas3t", ds3t.Datas3tName,
						)
						continue
					}

					// Create optimizer and find best optimization
					optimizer := optimize.NewOptimizer(dataranges)
					operation := optimizer.FindBestOptimization()

					if operation == nil {
						logger.Info("no optimization needed",
							"datas3t", ds3t.Datas3tName,
							"datarange_count", len(dataranges),
						)
						continue
					}

					// Log the optimization found
					logger.Info("optimization found",
						"datas3t", ds3t.Datas3tName,
						"type", string(operation.Type),
						"first_datapoint", operation.FirstDatapoint,
						"last_datapoint", operation.LastDatapoint,
						"datapoint_count", operation.LastDatapoint-operation.FirstDatapoint+1,
						"files_to_merge", len(operation.DatarangeIDs),
						"total_bytes", operation.TotalSize,
						"reason", operation.Reason,
					)

					// Execute aggregation
					progressLogger := newProgressLogger(logger, ds3t.Datas3tName, progressInterval)
					aggregateOpts := &client.AggregateOptions{
						MaxParallelism:   4,
						MaxRetries:       3,
						TempDir:          tempDir,
						ProgressCallback: progressLogger.update,
					}

					aggregateStart := time.Now()
					err = clientInstance.AggregateDataRanges(
						ctx,
						ds3t.Datas3tName,
						operation.FirstDatapoint,
						operation.LastDatapoint,
						aggregateOpts,
					)
					aggregateDuration := time.Since(aggregateStart)

					if err != nil {
						logger.Error("aggregation failed",
							"datas3t", ds3t.Datas3tName,
							"error", err.Error(),
							"duration_seconds", int(aggregateDuration.Seconds()),
						)
						continue
					}

					logger.Info("optimization completed",
						"datas3t", ds3t.Datas3tName,
						"files_merged", len(operation.DatarangeIDs),
						"datapoints", operation.LastDatapoint-operation.FirstDatapoint+1,
						"bytes", operation.TotalSize,
						"duration_seconds", int(aggregateDuration.Seconds()),
					)
					optimizationsPerformed++
				}

				cycleDuration := time.Since(cycleStart)
				logger.Info("optimization cycle completed",
					"cycle", cycleNumber,
					"datas3ts_processed", len(datas3ts),
					"optimizations_performed", optimizationsPerformed,
					"duration_seconds", int(cycleDuration.Seconds()),
				)

				// If no optimizations were performed, back off
				if optimizationsPerformed == 0 {
					logger.Info("no optimizations performed, backing off",
						"backoff_duration", backoffDuration.String(),
					)
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(backoffDuration):
						// Continue to next cycle
					}
				} else {
					// Small delay before next cycle even when optimizations were performed
					select {
					case <-ctx.Done():
						return nil
					case <-time.After(10 * time.Second):
						// Continue to next cycle
					}
				}
			}
		},
	}
}
