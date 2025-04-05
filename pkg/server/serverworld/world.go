package serverworld

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"

	"github.com/draganm/datas3t/pkg/server"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	miniomodule "github.com/testcontainers/testcontainers-go/modules/minio"

	_ "github.com/mattn/go-sqlite3"
)

type ctxKey struct{}

// World represents the test environment
type World struct {
	DB                    *sql.DB
	HTTPServer            *httptest.Server
	ServerURL             string
	LastResponseStatus    int
	LastResponseBody      []byte
	LastDatasetID         string
	NumUploadedDataPoints int
	LastDatarange         struct {
		ObjectKey       string `json:"object_key"`
		MinDatapointKey int64  `json:"min_datapoint_key"`
		MaxDatapointKey int64  `json:"max_datapoint_key"`
		SizeBytes       int64  `json:"size_bytes"`
	}
	LastDatasets          []server.Dataset
	MinioClient           *minio.Client
	MinioBucketName       string
	LastAggregateResponse server.AggregateResponse

	// MinIO related fields
	MinioContainer *miniomodule.MinioContainer
	MinioEndpoint  string
	MinioAccessKey string
	MinioSecretKey string
	UploadsPath    string
}

// ToContext adds the World to the context
func ToContext(ctx context.Context, world *World) context.Context {
	return context.WithValue(ctx, ctxKey{}, world)
}

// FromContext extracts the World from the context
func FromContext(ctx context.Context) (*World, bool) {
	world, ok := ctx.Value(ctxKey{}).(*World)
	return world, ok
}

// New creates a new test environment
func New(ctx context.Context) (*World, error) {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	// Start MinIO testcontainer
	minioContainer, err := miniomodule.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	if err != nil {
		return nil, fmt.Errorf("failed to start MinIO container: %w", err)
	}

	// Ensure MinIO container is terminated when context is cancelled
	context.AfterFunc(ctx, func() {
		_ = minioContainer.Terminate(context.Background())
	})

	// Get MinIO connection details
	endpoint, err := minioContainer.Endpoint(ctx, "")
	if err != nil {
		_ = minioContainer.Terminate(context.Background())
		return nil, fmt.Errorf("failed to get MinIO endpoint: %w", err)
	}

	// Create MinIO client
	// MinIO testcontainer uses default credentials (minioadmin/minioadmin)
	accessKey := "minioadmin"
	secretKey := "minioadmin"
	bucketName := "testbucket"

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: false,
	})
	if err != nil {
		_ = minioContainer.Terminate(context.Background())
		return nil, fmt.Errorf("failed to create MinIO client: %w", err)
	}

	// Create a test bucket
	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		_ = minioContainer.Terminate(context.Background())
		return nil, fmt.Errorf("failed to create MinIO bucket: %w", err)
	}

	// Create a temporary directory for uploads
	uploadsPath, err := os.MkdirTemp("", "datas3t-uploads-*")
	if err != nil {
		_ = minioContainer.Terminate(context.Background())
		return nil, fmt.Errorf("failed to create temporary uploads directory: %w", err)
	}

	// Ensure uploads directory is removed when context is cancelled
	context.AfterFunc(ctx, func() {
		_ = os.RemoveAll(uploadsPath)
	})

	// Create S3 configuration for the server
	s3Config := &server.S3Config{
		Endpoint:        endpoint,
		Region:          "us-east-1", // MinIO default region
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
		BucketName:      bucketName,
		UseSSL:          false,
	}

	// Create server with S3 configuration
	srv, err := server.CreateServer(
		ctx,
		log,
		"file::memory:?cache=shared",
		s3Config,
		uploadsPath,
	)
	if err != nil {
		_ = minioContainer.Terminate(context.Background())
		_ = os.RemoveAll(uploadsPath)
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	httpServer := httptest.NewServer(srv.Handler)
	context.AfterFunc(ctx, func() {
		httpServer.Close()
	})

	return &World{
		ServerURL:       httpServer.URL,
		MinioContainer:  minioContainer,
		MinioEndpoint:   endpoint,
		MinioAccessKey:  accessKey,
		MinioSecretKey:  secretKey,
		MinioClient:     minioClient,
		MinioBucketName: bucketName,
		UploadsPath:     uploadsPath,
		DB:              srv.DB,
	}, nil
}
