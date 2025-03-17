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
		id        string
		file      string
	}{}

	return &cli.Command{
		Name:  "upload",
		Usage: "Upload data to a dataset",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "id",
				Required:    true,
				Usage:       "Dataset ID",
				Destination: &cfg.id,
				EnvVars:     []string{"DATAS3T_DATASET_ID"},
			},
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
			cl, err := client.NewClient(cfg.serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			file, err := os.Open(cfg.file)
			if err != nil {
				return fmt.Errorf("failed to open file: %w", err)
			}
			defer file.Close()

			err = cl.UploadDatarange(c.Context, cfg.id, file)
			if err != nil {
				return fmt.Errorf("failed to upload datarange: %w", err)
			}

			log.Info("Data uploaded successfully", "id", cfg.id, "file", cfg.file)
			return nil
		},
	}
}
