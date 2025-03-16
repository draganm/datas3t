package upload

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/urfave/cli/v2"
)

func Command(log *slog.Logger) *cli.Command {
	return &cli.Command{
		Name:  "upload-datarange",
		Usage: "Upload data to a dataset",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "id",
				Required: true,
				Usage:    "Dataset ID",
			},
			&cli.StringFlag{
				Name:     "file",
				Required: true,
				Usage:    "File to upload",
			},
		},
		Action: func(c *cli.Context) error {
			serverURL := c.String("server-url")
			cl, err := client.NewClient(serverURL)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}

			id := c.String("id")
			filePath := c.String("file")

			file, err := os.Open(filePath)
			if err != nil {
				return fmt.Errorf("failed to open file: %w", err)
			}
			defer file.Close()

			err = cl.UploadDatarange(c.Context, id, file)
			if err != nil {
				return fmt.Errorf("failed to upload datarange: %w", err)
			}

			log.Info("Data uploaded successfully", "id", id, "file", filePath)
			return nil
		},
	}
}
