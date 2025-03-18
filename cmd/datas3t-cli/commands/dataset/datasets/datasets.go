package datasets

import (
	"fmt"
	"log/slog"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/dustin/go-humanize"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
	}{}

	return &cli.Command{
		Name:  "list",
		Usage: "List all available datasets",
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
			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			datasets, err := cl.ListDatasets(c.Context)
			if err != nil {
				return fmt.Errorf("failed to list datasets: %w", err)
			}

			if len(datasets) == 0 {
				fmt.Println("No datasets found")
				return nil
			}

			fmt.Printf("%-20s %-15s %-15s\n", "ID", "DATARANGES", "SIZE")
			fmt.Println("-------------------------------------------------------")
			for _, ds := range datasets {
				fmt.Printf("%-20s %-15d %-15s\n",
					ds.ID,
					ds.DatarangeCount,
					humanize.Bytes(uint64(ds.TotalSizeBytes)),
				)
			}

			return nil
		},
	}
}
