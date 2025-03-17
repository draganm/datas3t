package list

import (
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
		id        string
	}{}

	return &cli.Command{
		Name:  "list",
		Usage: "List data ranges for a dataset",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "id",
				Required:    true,
				Usage:       "Dataset ID",
				Destination: &cfg.id,
				EnvVars:     []string{"DATAS3T_DATASET_ID"},
			},
			&cli.StringFlag{
				Name:        "server-url",
				Required:    true,
				Usage:       "URL of the Datas3t server",
				Destination: &cfg.serverURL,
				EnvVars:     []string{"DATAS3T_SERVER_URL"},
			},
		},
		Action: func(c *cli.Context) error {
			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			ranges, err := cl.GetDataranges(c.Context, cfg.id)
			if err != nil {
				return fmt.Errorf("failed to get dataranges: %w", err)
			}

			for _, r := range ranges {
				fmt.Printf("Object Key: %s\n", r.ObjectKey)
				fmt.Printf("Min Datapoint Key: %d\n", r.MinDatapointKey)
				fmt.Printf("Max Datapoint Key: %d\n", r.MaxDatapointKey)
				fmt.Printf("Size (bytes): %d\n", r.SizeBytes)
				fmt.Println("---")
			}

			return nil
		},
	}
}
