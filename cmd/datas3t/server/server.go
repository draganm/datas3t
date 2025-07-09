package server

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"github.com/draganm/datas3t/httpapi"
	"github.com/draganm/datas3t/postgresstore"
	"github.com/draganm/datas3t/server"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/urfave/cli/v2"
)

func Command() *cli.Command {
	return &cli.Command{
		Name:  "server",
		Usage: "Start the datas3t server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "addr",
				Value:   ":8765",
				Usage:   "Address to bind to",
				EnvVars: []string{"ADDR"},
			},
			&cli.StringFlag{
				Name:     "db-url",
				Value:    "postgres://postgres:postgres@localhost:5432/postgres",
				Usage:    "Database URL",
				EnvVars:  []string{"DB_URL"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "cache-dir",
				Usage:    "Cache directory",
				Required: true,
				EnvVars:  []string{"CACHE_DIR"},
			},
			&cli.Int64Flag{
				Name:    "max-cache-size",
				Value:   1024 * 1024 * 1024,
				Usage:   "Maximum cache size in bytes",
				EnvVars: []string{"MAX_CACHE_SIZE"},
			},
			&cli.StringFlag{
				Name:     "encryption-key",
				Usage:    "Base64-encoded encryption key for S3 credentials (32 bytes)",
				Required: true,
				EnvVars:  []string{"ENCRYPTION_KEY"},
			},
		},
		Action: serverAction,
	}
}

func serverAction(c *cli.Context) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	addr := c.String("addr")
	dbURL := c.String("db-url")
	cacheDir := c.String("cache-dir")
	maxCacheSize := c.Int64("max-cache-size")
	encryptionKey := c.String("encryption-key")

	ctx, cancel := signal.NotifyContext(c.Context, os.Interrupt, os.Kill)
	defer cancel()

	migrationFS, err := fs.Sub(postgresstore.MigrationsFS, "migrations")
	if err != nil {
		return err
	}

	d, err := iofs.New(migrationFS, ".")
	if err != nil {
		return err
	}

	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	m, err := migrate.NewWithSourceInstance("iofs", d, strings.Replace(dbURL, "postgresql:", "pgx5:", 1))
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}

	err = m.Up()
	switch err {
	case nil:
		logger.Info("Applied database migrations")
	case migrate.ErrNoChange:
		logger.Info("No migrations applied")
	default:
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		logger.Error("failed to listen", "error", err)
		return err
	}
	defer l.Close()
	logger.Info("server started", "addr", l.Addr())

	s, err := server.NewServer(db, cacheDir, maxCacheSize, encryptionKey)
	if err != nil {
		return fmt.Errorf("failed to create server: %w", err)
	}

	// Start the key deletion worker
	s.StartKeyDeletionWorker(ctx, logger)

	mux := httpapi.NewHTTPAPI(s, logger)

	srv := &http.Server{
		Handler: mux,
	}

	context.AfterFunc(ctx, func() {
		srv.Close()
	})

	return srv.Serve(l)
}
