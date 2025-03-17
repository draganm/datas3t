package get

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
		Name:  "get",
		Usage: "Get dataset information",
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

			data, err := cl.GetDataset(c.Context, cfg.id)
			if err != nil {
				return fmt.Errorf("failed to get dataset: %w", err)
			}

			fmt.Println(string(data))
			return nil
		},
	}
}
