package datarange

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	cfg := struct {
		serverURL string
		id        string
		start     uint64
		end       uint64
		output    string
	}{}

	return &cli.Command{
		Name:  "datarange",
		Usage: "Get dataset datarange and save it to a file",
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
			&cli.Uint64Flag{
				Name:        "start",
				Required:    true,
				Usage:       "Start offset of the datarange",
				Destination: &cfg.start,
			},
			&cli.Uint64Flag{
				Name:        "end",
				Required:    true,
				Usage:       "End offset of the datarange",
				Destination: &cfg.end,
			},
			&cli.StringFlag{
				Name:        "output",
				Required:    true,
				Usage:       "Output file path to save the datarange",
				Destination: &cfg.output,
			},
		},
		Action: func(c *cli.Context) error {
			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			file, err := os.Create(cfg.output)
			if err != nil {
				return fmt.Errorf("failed to create output file: %w", err)
			}
			defer file.Close()

			err = cl.GetDatarange(c.Context, cfg.id, cfg.start, cfg.end, file)
			if err != nil {
				return fmt.Errorf("failed to get datarange: %w", err)
			}

			log.Info("successfully saved datarange",
				"id", cfg.id,
				"start", cfg.start,
				"end", cfg.end,
				"output", cfg.output,
			)
			return nil
		},
	}
}
