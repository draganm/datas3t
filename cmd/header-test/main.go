package main

import (
	"archive/tar"
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"os/signal"

	"github.com/draganm/datas3t/pkg/client"
	tarmmap "github.com/draganm/tar-mmap-go"
	"github.com/urfave/cli/v2"
)

func main() {

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		datas3tURL  string
		headersFile string
	}{}

	app := &cli.App{
		Name:  "header-test",
		Usage: "upload headers to datas3t",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "datas3t-url",
				Usage:       "datas3t url",
				EnvVars:     []string{"DATAS3T_URL"},
				Destination: &cfg.datas3tURL,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "headers-file",
				Usage:       "headers file",
				EnvVars:     []string{"HEADERS_FILE"},
				Destination: &cfg.headersFile,
				Required:    true,
			},
		},

		Action: func(c *cli.Context) error {
			ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt)
			defer cancel()

			ds3tClient, err := client.NewClient(cfg.datas3tURL)
			if err != nil {
				return err
			}

			tm, err := tarmmap.Open(cfg.headersFile)
			if err != nil {
				return err
			}

			datasets, err := ds3tClient.ListDatasets(ctx)
			if err != nil {
				return err
			}

			ids := map[string]bool{}
			for _, dataset := range datasets {
				ids[dataset.ID] = true
			}

			if !ids["headers"] {
				err = ds3tClient.CreateDataset(ctx, "headers")
				if err != nil {
					return fmt.Errorf("failed to create headers dataset: %w", err)
				}
			}

			for _, s := range tm.Sections {
				buf := bytes.NewBuffer(nil)
				tw := tar.NewWriter(buf)
				err := tw.WriteHeader(s.Header)
				if err != nil {
					return fmt.Errorf("failed to write header: %w", err)
				}
				_, err = tw.Write(s.Data)
				if err != nil {
					return fmt.Errorf("failed to write data: %w", err)
				}

				err = tw.Close()
				if err != nil {
					return fmt.Errorf("failed to close writer: %w", err)
				}

				log.Info("uploading datarange", "dataset", "headers", "key", s.Header.Name)

				err = ds3tClient.UploadDatarange(ctx, "headers", buf)
				if err != nil {
					return fmt.Errorf("failed to upload datarange: %w", err)
				}

			}

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("failed to run app", "error", err)
		os.Exit(1)
	}
}
