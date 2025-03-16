package list

import (
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	return &cli.Command{
		Name:  "get-dataranges",
		Usage: "Get data ranges for a dataset",
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
			ranges, err := cl.GetDataranges(c.Context, id)
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
