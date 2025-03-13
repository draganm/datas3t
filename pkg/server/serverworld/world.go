package serverworld

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"

	"github.com/draganm/datas3t/pkg/server"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	miniomodule "github.com/testcontainers/testcontainers-go/modules/minio"
)

type World struct {
	ServerURL          string
	CurrentDatasetID   string
	LastResponseStatus int

	// MinIO related fields
	MinioContainer  *miniomodule.MinioContainer
	MinioEndpoint   string
	MinioAccessKey  string
	MinioSecretKey  string
	MinioClient     *minio.Client
	MinioBucketName string
}

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
		":memory:",
		s3Config,
	)
	if err != nil {
		_ = minioContainer.Terminate(context.Background())
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
	}, nil
}

type worldKey string

const worldContextKey worldKey = "world"

func FromContext(ctx context.Context) (*World, bool) {
	world, ok := ctx.Value(worldContextKey).(*World)
	return world, ok
}

func ToContext(ctx context.Context, world *World) context.Context {
	return context.WithValue(ctx, worldContextKey, world)
}
