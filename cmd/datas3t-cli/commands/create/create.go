package create

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
		Name:      "create",
		Usage:     "Create a new dataset",
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

			err = cl.CreateDataset(c.Context, datasetID)
			if err != nil {
				return fmt.Errorf("failed to create dataset: %w", err)
			}

			log.Info("Dataset created successfully", "id", datasetID)
			return nil
		},
	}
}
