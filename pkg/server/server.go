package server

import (
	"context"
	"log/slog"
	"net/http"

	"database/sql"
	"embed"
	"io/fs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
)

// Plan for the server
// endpoints
// 	create a datas3t:  PUT /api/v1/datas3t/{id}
// 	get a datas3 info: GET /api/v1/datas3t/{id}
// 	put a datas3t: PATCH /api/v1/datas3t/{id}
// 	post data to a datas3t: POST /api/v1/datas3t/{id}
//  get data for a datas3t range: GET /api/v1/datas3t/{id}/data/{start}/{end}
//  get data for a single data: GET /api/v1/datas3t/{id}/data/{id}

//go:embed sqlitestore/migrations/*.sql
var migrationsFS embed.FS

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
	db          *sql.DB
	s3Client    *s3.Client
	bucket      string
	uploadsPath string
	http.Handler
}

func CreateServer(
	ctx context.Context,
	log *slog.Logger,
	dbURL string,
	s3Config *S3Config,
	uploadsPath string,
) (*Server, error) {

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

	// Ensure database connection is working
	if err := db.Ping(); err != nil {
		return nil, err
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
		return nil, err
	}

	// Apply migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return nil, err
	}
	log.Info("Applied database migrations")

	// Initialize S3 client
	var s3Client *s3.Client
	if s3Config != nil {
		// IMPORTANT: AWS SDK v2 S3 Client Configuration
		// The following pattern is the recommended way to configure a custom endpoint for S3 in AWS SDK v2.
		// Do NOT use deprecated approaches such as:
		// - Direct EndpointOptions assignment (removed from SDK)
		// - EndpointResolverWithOptions with type conversion (causes compilation errors)
		// - Manually constructing the endpoint URL

		// 1. Create the AWS config with credentials and region
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

			// Set custom endpoint resolver to connect to non-AWS S3 services
			o.EndpointResolver = s3.EndpointResolverFunc(func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL: s3Config.Endpoint,
				}, nil
			})
		})

		log.Info("Connected to S3 storage", "endpoint", s3Config.Endpoint, "bucket", s3Config.BucketName)
	}

	// Initialize store
	store := sqlitestore.New(db)

	mux := http.NewServeMux()

	mux.HandleFunc("PUT /api/v1/datas3t/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		// Use the store here to avoid "declared but not used" error
		err := store.CreateDataset(r.Context(), id)
		if err != nil {
			log.Error("failed to create dataset", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /api/v1/datas3t/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		dataset, err := store.DatasetExists(r.Context(), id)
		if err != nil {
			log.Error("failed to get dataset", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !dataset {
			log.Error("dataset not found", "id", id)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)

	})

	bucket := ""
	if s3Config != nil {
		bucket = s3Config.BucketName
	}

	return &Server{
		db:          db,
		s3Client:    s3Client,
		bucket:      bucket,
		uploadsPath: uploadsPath,
		Handler:     mux,
	}, nil
}
