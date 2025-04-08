package aggregate

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
		startKey  uint64
		endKey    uint64
	}{}

	return &cli.Command{
		Name:      "aggregate",
		Usage:     "Aggregate multiple dataranges into a single consolidated datarange",
		ArgsUsage: "DATASET_ID",
		Description: `This command consolidates multiple dataranges within a specified key range 
into a single datarange, optimizing storage and improving access performance.
The operation is atomic and will replace all affected dataranges with a single consolidated datarange.`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "server-url",
				Required:    true,
				Usage:       "URL of the Datas3t server",
				Destination: &cfg.serverURL,
				EnvVars:     []string{"DATAS3T_SERVER_URL"},
			},
			&cli.Uint64Flag{
				Name:        "start-key",
				Required:    true,
				Usage:       "Start key of the datapoint range to aggregate",
				Destination: &cfg.startKey,
			},
			&cli.Uint64Flag{
				Name:        "end-key",
				Required:    true,
				Usage:       "End key of the datapoint range to aggregate",
				Destination: &cfg.endKey,
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("expected exactly one argument: DATASET_ID")
			}

			datasetID := c.Args().Get(0)

			if cfg.startKey > cfg.endKey {
				return fmt.Errorf("start key (%d) must be less than or equal to end key (%d)", cfg.startKey, cfg.endKey)
			}

			// Create client
			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			// Call aggregate endpoint
			result, err := cl.AggregateDatarange(context.Background(), datasetID, cfg.startKey, cfg.endKey)
			if err != nil {
				return fmt.Errorf("failed to aggregate dataranges: %w", err)
			}

			// Log success
			log.Info("Successfully aggregated dataranges",
				"datasetID", result.DatasetID,
				"startKey", result.StartKey,
				"endKey", result.EndKey,
				"rangesReplaced", result.RangesReplaced,
				"newObjectKey", result.NewObjectKey,
				"sizeBytes", result.SizeBytes,
			)

			// Display results to stdout
			fmt.Printf("Successfully aggregated dataranges:\n")
			fmt.Printf("  Dataset ID:      %s\n", result.DatasetID)
			fmt.Printf("  Start Key:       %d\n", result.StartKey)
			fmt.Printf("  End Key:         %d\n", result.EndKey)
			fmt.Printf("  Ranges Replaced: %d\n", result.RangesReplaced)
			fmt.Printf("  New Object Key:  %s\n", result.NewObjectKey)
			fmt.Printf("  Size (bytes):    %d\n", result.SizeBytes)

			return nil
		},
	}
}
