package missing

import (
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
	}{}

	return &cli.Command{
		Name:      "missing-ranges",
		Usage:     "Show missing ranges in a dataset",
		ArgsUsage: "DATASET_ID",
		Flags: []cli.Flag{
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

			missingRanges, err := cl.GetMissingRanges(c.Context, datasetID)
			if err != nil {
				return fmt.Errorf("failed to get missing ranges: %w", err)
			}

			fmt.Printf("Dataset: %s\n", datasetID)
			if missingRanges.FirstDatapoint != nil {
				fmt.Printf("First datapoint: %d\n", *missingRanges.FirstDatapoint)
			} else {
				fmt.Println("First datapoint: none")
			}

			if missingRanges.LastDatapoint != nil {
				fmt.Printf("Last datapoint: %d\n", *missingRanges.LastDatapoint)
			} else {
				fmt.Println("Last datapoint: none")
			}

			if len(missingRanges.MissingRanges) == 0 {
				fmt.Println("No missing ranges found.")
			} else {
				fmt.Println("Missing ranges:")
				for i, r := range missingRanges.MissingRanges {
					fmt.Printf("  %d: %d to %d (%d datapoints)\n", i+1, r.Start, r.End, r.End-r.Start+1)
				}
			}

			return nil
		},
	}
}
