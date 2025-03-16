package get

import (
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	return &cli.Command{
		Name:  "get-dataset",
		Usage: "Get dataset information",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "id",
				Required: true,
				Usage:    "Dataset ID",
			},
		},
		Action: func(c *cli.Context) error {
			serverURL := c.String("server-url")
			cl, err := client.NewClient(serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			id := c.String("id")
			data, err := cl.GetDataset(c.Context, id)
			if err != nil {
				return fmt.Errorf("failed to get dataset: %w", err)
			}

			fmt.Println(string(data))
			return nil
		},
	}
}
