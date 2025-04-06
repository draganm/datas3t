package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/signal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/pkg/restore"
	"github.com/draganm/datas3t/pkg/server"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
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
		},
		Commands: []*cli.Command{
			{
				Name:  "run",
				Usage: "Run the Datas3t server",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:        "addr",
						Required:    true,
						Usage:       "Address to listen on",
						Destination: &cfg.addr,
						EnvVars:     []string{"DATAS3T_ADDR"},
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
			},
			{
				Name:  "restore",
				Usage: "Restore database from S3 storage if needed",
				Action: func(c *cli.Context) error {
					log.Info("Starting database restoration")
					ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt)
					defer cancel()

					// Initialize S3 client
					awsConfig := aws.Config{
						Region: cfg.s3Region,
						Credentials: credentials.NewStaticCredentialsProvider(
							cfg.s3AccessKeyID,
							cfg.s3SecretKey,
							"",
						),
					}

					s3Client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
						o.UsePathStyle = true

						switch cfg.s3UseSSL {
						case true:
							o.BaseEndpoint = aws.String(fmt.Sprintf("https://%s", cfg.s3Endpoint))
						case false:
							o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", cfg.s3Endpoint))
						}
					})

					// Open database
					db, err := sql.Open("sqlite3", cfg.dbURL)
					if err != nil {
						return fmt.Errorf("failed to open database: %w", err)
					}
					defer db.Close()

					// Enable foreign key constraints
					_, err = db.Exec("PRAGMA foreign_keys = ON;")
					if err != nil {
						return fmt.Errorf("failed to enable foreign key constraints: %w", err)
					}

					// Run migrations
					migrationFS, err := fs.Sub(server.MigrationsFS, "sqlitestore/migrations")
					if err != nil {
						return err
					}

					d, err := iofs.New(migrationFS, ".")
					if err != nil {
						return err
					}

					driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
					if err != nil {
						return err
					}

					m, err := migrate.NewWithInstance("iofs", d, "sqlite3", driver)
					if err != nil {
						return fmt.Errorf("failed to create migrator: %w", err)
					}

					err = m.Up()
					switch err {
					case nil:
						log.Info("Applied database migrations")
					case migrate.ErrNoChange:
						log.Info("No migrations applied")
					default:
						return fmt.Errorf("failed to run migrations: %w", err)
					}

					// Call RestoreIfNeeded
					err = restore.RestoreIfNeeded(ctx, restore.Config{
						Logger:   log,
						DB:       db,
						S3Client: s3Client,
						Bucket:   cfg.s3BucketName,
					})

					if err != nil {
						return fmt.Errorf("failed to restore database: %w", err)
					}

					log.Info("Database restoration completed successfully")
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Error("terminated", "error", err)
		os.Exit(1)
	}
}
