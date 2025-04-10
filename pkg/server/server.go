package server

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/sync/errgroup"
)

//go:embed sqlitestore/migrations/*.sql
var migrationsFS embed.FS

// Export migrationsFS for use in other packages
var MigrationsFS = migrationsFS

// S3Config holds configuration for connecting to an S3 bucket
type S3Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	BucketName      string
	UseSSL          bool
}

type Server struct {
	DB          *sql.DB
	s3Client    *s3.Client
	bucket      string
	uploadsPath string
	logger      *slog.Logger
	http.Handler
}

func CreateServer(
	ctx context.Context,
	log *slog.Logger,
	dbURL string,
	s3Config *S3Config,
	uploadsPath string,
) (*Server, error) {

	log.Info("Creating server", "dbURL", dbURL, "s3Config", s3Config, "uploadsPath", uploadsPath)

	// Import required packages

	// Open SQLite database
	db, err := sql.Open("sqlite3", dbURL)
	if err != nil {
		return nil, err
	}

	context.AfterFunc(ctx, func() {
		err := db.Close()
		if err != nil {
			log.Error("failed to close database", "error", err)
		}
	})

	// Enable foreign key constraints in SQLite
	_, err = db.Exec("PRAGMA foreign_keys = ON;")
	if err != nil {
		return nil, fmt.Errorf("failed to enable foreign key constraints: %w", err)
	}

	// Ping the database to ensure it's accessible
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	log.Info("Connected to SQLite database", "url", dbURL)

	// Prepare migrations from embedded filesystem
	migrationFS, err := fs.Sub(migrationsFS, "sqlitestore/migrations")
	if err != nil {
		return nil, err
	}

	d, err := iofs.New(migrationFS, ".")
	if err != nil {
		return nil, err
	}

	// Initialize database driver for migrations
	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return nil, err
	}

	// Create migration instance
	m, err := migrate.NewWithInstance("iofs", d, "sqlite3", driver)
	if err != nil {
		return nil, fmt.Errorf("failed to create migrator: %w", err)
	}

	err = m.Up()
	switch err {
	case nil:
		log.Info("Applied database migrations")
	case migrate.ErrNoChange:
		log.Info("No migrations applied")
	default:
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}
	log.Info("Applied database migrations")

	// Initialize S3 client
	var s3Client *s3.Client
	if s3Config != nil {
		cfg := aws.Config{
			Region: s3Config.Region,
			Credentials: credentials.NewStaticCredentialsProvider(
				s3Config.AccessKeyID,
				s3Config.SecretAccessKey,
				"",
			),
		}

		// 2. Create the S3 client with functional options to customize behavior
		s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
			// Enable path-style addressing (bucket.s3.amazonaws.com vs. s3.amazonaws.com/bucket)
			// Required for most S3-compatible servers like MinIO
			o.UsePathStyle = true

			switch s3Config.UseSSL {
			case true:
				o.BaseEndpoint = aws.String(fmt.Sprintf("https://%s", s3Config.Endpoint))
			case false:
				o.BaseEndpoint = aws.String(fmt.Sprintf("http://%s", s3Config.Endpoint))
			}

		})

		log.Info("Connected to S3 storage", "endpoint", s3Config.Endpoint, "bucket", s3Config.BucketName)

		// Removed RestoreIfNeeded call - this functionality is now available as a separate sub-command
	}

	bucket := ""
	if s3Config != nil {
		bucket = s3Config.BucketName
	}

	server := &Server{
		DB:          db,
		s3Client:    s3Client,
		bucket:      bucket,
		uploadsPath: uploadsPath,
		logger:      log,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/datas3t", server.HandleListDatasets)
	mux.HandleFunc("PUT /api/v1/datas3t/{id}", server.HandleCreateDataset)
	mux.HandleFunc("DELETE /api/v1/datas3t/{id}", server.HandleDeleteDataset)
	mux.HandleFunc("GET /api/v1/datas3t/{id}", server.HandleGetDataset)
	mux.HandleFunc("POST /api/v1/datas3t/{id}", server.HandleUploadDatarange)
	mux.HandleFunc("GET /api/v1/datas3t/{id}/dataranges", server.HandleGetDataranges)
	mux.HandleFunc("GET /api/v1/datas3t/{id}/datarange/{start}/{end}", server.HandleGetDatarange)
	mux.HandleFunc("POST /api/v1/datas3t/{id}/aggregate/{start}/{end}", server.HandleAggregateDatarange)
	mux.HandleFunc("GET /api/v1/datas3t/{id}/missing-ranges", server.HandleGetMissingRanges)
	mux.HandleFunc("GET /healthy", server.HandleHealthCheck)

	server.Handler = mux

	// Start periodic cleanup job if S3 is configured
	if s3Client != nil && bucket != "" {
		server.startCleanupJob(ctx)
	}

	return server, nil
}

// startCleanupJob starts a periodic job to delete S3 keys that are marked for deletion
// and have passed their delete_at timestamp
func (s *Server) startCleanupJob(ctx context.Context) {
	go func() error {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		err := s.cleanupS3Keys(ctx)
		// Run once at startup
		if err != nil {
			s.logger.Error("error running S3 cleanup", "error", err)
		}

		for {
			select {
			case <-ctx.Done():
				s.logger.Info("stopping S3 cleanup job due to context cancellation")
				return nil
			case <-ticker.C:
				err := s.cleanupS3Keys(ctx)
				if err != nil {
					s.logger.Error("error running S3 cleanup", "error", err)
				}
			}
		}
	}()
	s.logger.Info("started periodic S3 cleanup job")
}

// cleanupS3Keys deletes S3 objects that are due for deletion
func (s *Server) cleanupS3Keys(ctx context.Context) error {
	logger := s.logger.With("job", "s3-cleanup")
	logger.Info("running S3 cleanup job")

	store := sqlitestore.New(s.DB)
	totalDeleted := 0

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Get keys that are due for deletion outside of a transaction
		keys, err := store.GetKeysToDelete(ctx)
		if err != nil {
			logger.Error("failed to get keys to delete", "error", err)
			return err
		}

		if len(keys) == 0 {
			logger.Info("no more S3 keys to delete")
			break // Exit the loop when no more keys are found
		}

		logger.Info("processing batch of keys to delete", "count", len(keys))

		// Create a new errgroup with concurrency limited to 5
		g, gCtx := errgroup.WithContext(ctx)
		g.SetLimit(5)

		// Use atomic counter instead of mutex
		var batchDeleted int64

		// Process S3 deletions in parallel with limited concurrency
		for _, key := range keys {
			key := key // Create a new variable for goroutine closure

			if gCtx.Err() != nil {
				continue
			}

			g.Go(func() error {
				// Delete from S3
				_, err := s.s3Client.DeleteObject(gCtx, &s3.DeleteObjectInput{
					Bucket: aws.String(s.bucket),
					Key:    aws.String(key.Key),
				})

				if err != nil {
					logger.Error("failed to delete object from S3", "key", key.Key, "error", err)
					return nil // Don't fail the entire errgroup for a single failure
				}

				// Only start a transaction after S3 delete succeeds
				tx, err := s.DB.BeginTx(gCtx, nil)
				if err != nil {
					logger.Error("failed to begin transaction", "key", key.Key, "error", err)
					return nil
				}

				// Use a deferred rollback that will be ignored if we commit successfully
				defer tx.Rollback()

				// Delete from database within a short transaction
				txStore := store.WithTx(tx)
				err = txStore.DeleteKeyToDeleteById(gCtx, key.ID)

				if err != nil {
					logger.Error("failed to delete key from database", "key", key.Key, "error", err)
					return nil
				}

				// Commit immediately after database operation
				err = tx.Commit()
				if err != nil {
					logger.Error("failed to commit transaction", "key", key.Key, "error", err)
					return nil
				}

				logger.Info("deleted S3 object", "key", key.Key)

				// Atomically increment counter
				atomic.AddInt64(&batchDeleted, 1)

				return nil
			})
		}

		// Wait for all goroutines to complete
		err = g.Wait()
		if err != nil {
			logger.Error("error in deletion goroutines", "error", err)
		}

		totalDeleted += int(batchDeleted)
		logger.Info("completed batch deletion", "deleted", batchDeleted, "total_deleted", totalDeleted)

		// If we got fewer keys than the limit, we're done
		if len(keys) < 100 {
			break
		}
	}

	logger.Info("completed S3 cleanup job", "total_deleted", totalDeleted)
	return nil
}

// HandleHealthCheck returns a 200 OK status to indicate the server is running
func (s *Server) HandleHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}
