package upload

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
		file      string
	}{}

	return &cli.Command{
		Name:      "upload",
		Usage:     "Upload datarange to a dataset",
		ArgsUsage: "DATASET_ID",
		Flags: []cli.Flag{
			&cli.PathFlag{
				Name:        "file",
				Required:    true,
				Usage:       "File to upload",
				Destination: &cfg.file,
				EnvVars:     []string{"DATAS3T_FILE"},
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
			if c.NArg() != 1 {
				return fmt.Errorf("expected exactly one argument: DATASET_ID")
			}

			datasetID := c.Args().Get(0)

			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			file, err := os.Open(cfg.file)
			if err != nil {
				return fmt.Errorf("failed to open file: %w", err)
			}
			defer file.Close()

			err = cl.UploadDatarange(c.Context, datasetID, file)
			if err != nil {
				return fmt.Errorf("failed to upload datarange: %w", err)
			}

			log.Info("Data uploaded successfully", "id", datasetID, "file", cfg.file)
			return nil
		},
	}
}
