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
		uploadsPath   string
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
				EnvVars:     []string{"DATAS3T_DB_URL"},
			},
			&cli.StringFlag{
				Name:        "addr",
				Required:    true,
				Usage:       "Address to listen on",
				Destination: &cfg.addr,
				EnvVars:     []string{"DATAS3T_ADDR"},
			},
			&cli.StringFlag{
				Name:        "s3-endpoint",
				Required:    true,
				Usage:       "S3 endpoint URL",
				Destination: &cfg.s3Endpoint,
				EnvVars:     []string{"DATAS3T_S3_ENDPOINT"},
			},
			&cli.StringFlag{
				Name:        "s3-region",
				Required:    true,
				Usage:       "S3 region",
				Destination: &cfg.s3Region,
				EnvVars:     []string{"DATAS3T_S3_REGION"},
			},
			&cli.StringFlag{
				Name:        "s3-access-key-id",
				Required:    true,
				Usage:       "S3 access key ID",
				Destination: &cfg.s3AccessKeyID,
				EnvVars:     []string{"DATAS3T_S3_ACCESS_KEY_ID"},
			},
			&cli.StringFlag{
				Name:        "s3-secret-key",
				Required:    true,
				Usage:       "S3 secret access key",
				Destination: &cfg.s3SecretKey,
				EnvVars:     []string{"DATAS3T_S3_SECRET_KEY"},
			},
			&cli.StringFlag{
				Name:        "s3-bucket-name",
				Required:    true,
				Usage:       "S3 bucket name",
				Destination: &cfg.s3BucketName,
				EnvVars:     []string{"DATAS3T_S3_BUCKET_NAME"},
			},
			&cli.BoolFlag{
				Name:        "s3-use-ssl",
				Required:    true,
				Usage:       "Use SSL for S3 connection",
				Destination: &cfg.s3UseSSL,
				EnvVars:     []string{"DATAS3T_S3_USE_SSL"},
			},
			&cli.StringFlag{
				Name:        "uploads-path",
				Required:    true,
				Usage:       "Path for temporary file uploads",
				Destination: &cfg.uploadsPath,
				EnvVars:     []string{"DATAS3T_UPLOADS_PATH"},
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
				cfg.uploadsPath,
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
