package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	"github.com/draganm/datas3t/pkg/server"
	"github.com/urfave/cli/v2"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := struct {
		addr          string
		dbURL         string
		s3Endpoint    string
		s3Region      string
		s3AccessKeyID string
		s3SecretKey   string
		s3BucketName  string
		s3UseSSL      bool
	}{}

	app := &cli.App{
		Name:  "server",
		Usage: "Datas3t server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "db-url",
				Required:    true,
				Usage:       "Database URL",
				Destination: &cfg.dbURL,
			},
			&cli.StringFlag{
				Name:        "addr",
				Required:    true,
				Usage:       "Address to listen on",
				Destination: &cfg.addr,
			},
			&cli.StringFlag{
				Name:        "s3-endpoint",
				Required:    true,
				Usage:       "S3 endpoint URL",
				Destination: &cfg.s3Endpoint,
			},
			&cli.StringFlag{
				Name:        "s3-region",
				Required:    true,
				Usage:       "S3 region",
				Destination: &cfg.s3Region,
			},
			&cli.StringFlag{
				Name:        "s3-access-key-id",
				Required:    true,
				Usage:       "S3 access key ID",
				Destination: &cfg.s3AccessKeyID,
			},
			&cli.StringFlag{
				Name:        "s3-secret-key",
				Required:    true,
				Usage:       "S3 secret access key",
				Destination: &cfg.s3SecretKey,
			},
			&cli.StringFlag{
				Name:        "s3-bucket-name",
				Required:    true,
				Usage:       "S3 bucket name",
				Destination: &cfg.s3BucketName,
			},
			&cli.BoolFlag{
				Name:        "s3-use-ssl",
				Required:    true,
				Usage:       "Use SSL for S3 connection",
				Destination: &cfg.s3UseSSL,
			},
		},
		Action: func(c *cli.Context) error {
			log.Info("Starting datas3t")
			ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt)
			defer cancel()

			s3Config := &server.S3Config{
				Endpoint:        cfg.s3Endpoint,
				Region:          cfg.s3Region,
				AccessKeyID:     cfg.s3AccessKeyID,
				SecretAccessKey: cfg.s3SecretKey,
				BucketName:      cfg.s3BucketName,
				UseSSL:          cfg.s3UseSSL,
			}
			log.Info("S3 storage configured", "endpoint", cfg.s3Endpoint, "bucket", cfg.s3BucketName)

			s, err := server.CreateServer(
				ctx,
				log,
				cfg.dbURL,
				s3Config,
			)
			if err != nil {
				return err
			}

			hs := &http.Server{
				Addr:    cfg.addr,
				Handler: s,
			}

			context.AfterFunc(ctx, func() {
				hs.Close()
			})

			log.Info("Server listening", "address", cfg.addr)
			return hs.ListenAndServe()
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("terminated", "error", err)
		os.Exit(1)
	}
}
