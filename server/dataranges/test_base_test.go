package dataranges_test

import (
	"archive/tar"
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsutil "github.com/draganm/datas3t/aws"
	"github.com/draganm/datas3t/postgresstore"
	"github.com/draganm/datas3t/server/bucket"
	"github.com/draganm/datas3t/server/dataranges"
	"github.com/draganm/datas3t/server/datas3t"
	"github.com/draganm/datas3t/tarindex"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	miniogo "github.com/minio/minio-go/v7"
	miniocreds "github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestEnvironment holds all the shared test infrastructure
type TestEnvironment struct {
	PgContainer          *tc_postgres.PostgresContainer
	MinioContainer       *minio.MinioContainer
	DB                   *pgxpool.Pool
	Queries              *postgresstore.Queries
	UploadSrv            *dataranges.UploadDatarangeServer
	BucketSrv            *bucket.BucketServer
	Datas3tSrv           *datas3t.Datas3tServer
	MinioEndpoint        string
	MinioHost            string
	MinioAccessKey       string
	MinioSecretKey       string
	TestBucketName       string
	TestBucketConfigName string
	TestDatas3tName      string
	S3Client             *s3.Client
	Logger               *slog.Logger
}

// SetupTestEnvironment creates and initializes the test environment
func SetupTestEnvironment(ctx SpecContext) *TestEnvironment {
	env := &TestEnvironment{}

	// Create logger that writes to GinkgoWriter for test visibility
	env.Logger = slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	var err error

	// Start PostgreSQL container
	env.PgContainer, err = tc_postgres.Run(ctx,
		"postgres:16-alpine",
		tc_postgres.WithDatabase("testdb"),
		tc_postgres.WithUsername("testuser"),
		tc_postgres.WithPassword("testpass"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
		testcontainers.WithLogger(log.New(GinkgoWriter, "", 0)),
	)
	Expect(err).NotTo(HaveOccurred())

	// Get PostgreSQL connection string
	connStr, err := env.PgContainer.ConnectionString(ctx, "sslmode=disable")
	Expect(err).NotTo(HaveOccurred())

	// Connect to PostgreSQL
	env.DB, err = pgxpool.New(ctx, connStr)
	Expect(err).NotTo(HaveOccurred())

	// Initialize queries instance
	env.Queries = postgresstore.New(env.DB)

	// Run migrations
	connStrForMigration, err := env.PgContainer.ConnectionString(ctx, "sslmode=disable")
	Expect(err).NotTo(HaveOccurred())

	m, err := migrate.New(
		"file://../../postgresstore/migrations",
		connStrForMigration)
	Expect(err).NotTo(HaveOccurred())

	err = m.Up()
	if err != nil && err != migrate.ErrNoChange {
		Expect(err).NotTo(HaveOccurred())
	}

	// Start MinIO container
	env.MinioContainer, err = minio.Run(
		ctx,
		"minio/minio:RELEASE.2024-01-16T16-07-38Z",
		minio.WithUsername("minioadmin"),
		minio.WithPassword("minioadmin"),
		testcontainers.WithLogger(log.New(GinkgoWriter, "", 0)),
	)
	Expect(err).NotTo(HaveOccurred())

	// Get MinIO connection details
	env.MinioEndpoint, err = env.MinioContainer.ConnectionString(ctx)
	Expect(err).NotTo(HaveOccurred())

	// Extract host:port from the full URL for both minioHost and storage endpoint
	env.MinioHost = strings.TrimPrefix(env.MinioEndpoint, "http://")
	env.MinioHost = strings.TrimPrefix(env.MinioHost, "https://")

	// Store endpoint without protocol (consistent with bucket_info.go approach)
	env.MinioEndpoint = env.MinioHost

	env.MinioAccessKey = "minioadmin"
	env.MinioSecretKey = "minioadmin"
	env.TestBucketName = "test-bucket"
	env.TestBucketConfigName = "test-bucket-config"
	env.TestDatas3tName = "test-datas3t"

	// Create test bucket in MinIO
	minioClient, err := miniogo.New(env.MinioHost, &miniogo.Options{
		Creds:  miniocreds.NewStaticV4(env.MinioAccessKey, env.MinioSecretKey, ""),
		Secure: false,
	})
	Expect(err).NotTo(HaveOccurred())

	err = minioClient.MakeBucket(ctx, env.TestBucketName, miniogo.MakeBucketOptions{})
	Expect(err).NotTo(HaveOccurred())

	// Create S3 client for test operations using shared utility
	env.S3Client, err = awsutil.CreateS3Client(ctx, awsutil.S3ClientConfig{
		AccessKey: env.MinioAccessKey,
		SecretKey: env.MinioSecretKey,
		Endpoint:  env.MinioEndpoint,
		Logger:    env.Logger,
	})
	Expect(err).NotTo(HaveOccurred())

	// Create server instances
	env.UploadSrv, err = dataranges.NewServer(env.DB, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
	Expect(err).NotTo(HaveOccurred())
	env.BucketSrv, err = bucket.NewServer(env.DB, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
	Expect(err).NotTo(HaveOccurred())
	env.Datas3tSrv, err = datas3t.NewServer(env.DB, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
	Expect(err).NotTo(HaveOccurred())

	// Add test bucket configuration
	bucketInfo := &bucket.BucketInfo{
		Name:      env.TestBucketConfigName,
		Endpoint:  env.MinioEndpoint,
		Bucket:    env.TestBucketName,
		AccessKey: env.MinioAccessKey,
		SecretKey: env.MinioSecretKey,
	}

	err = env.BucketSrv.AddBucket(ctx, env.Logger, bucketInfo)
	Expect(err).NotTo(HaveOccurred())

	// Add test datas3t
	datas3tReq := &datas3t.AddDatas3tRequest{
		Bucket: env.TestBucketConfigName,
		Name:   env.TestDatas3tName,
	}

	err = env.Datas3tSrv.AddDatas3t(ctx, env.Logger, datas3tReq)
	Expect(err).NotTo(HaveOccurred())

	return env
}

// TeardownTestEnvironment cleans up the test environment
func (env *TestEnvironment) TeardownTestEnvironment(ctx SpecContext) {
	if env.DB != nil {
		env.DB.Close()
	}
	if env.PgContainer != nil {
		err := env.PgContainer.Terminate(ctx)
		Expect(err).NotTo(HaveOccurred())
	}
	if env.MinioContainer != nil {
		err := env.MinioContainer.Terminate(ctx)
		Expect(err).NotTo(HaveOccurred())
	}
}

// Helper function to perform HTTP PUT requests
func HttpPut(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	return client.Do(req)
}

// CreateProperTarWithIndex creates a proper TAR archive with correctly named files and returns both the tar data and index
func CreateProperTarWithIndex(numFiles int, startIndex int64) ([]byte, []byte) {
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Create files with proper %020d.<extension> naming
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("%020d.txt", startIndex+int64(i))
		content := fmt.Sprintf("Content of file %d", startIndex+int64(i))

		header := &tar.Header{
			Name: filename,
			Size: int64(len(content)),
			Mode: 0644,
		}

		err := tw.WriteHeader(header)
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar header: %v", err))
		}

		_, err = tw.Write([]byte(content))
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar content: %v", err))
		}
	}

	err := tw.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to close tar writer: %v", err))
	}

	// Create the tar index
	tarReader := bytes.NewReader(tarBuf.Bytes())
	indexData, err := tarindex.IndexTar(tarReader)
	if err != nil {
		panic(fmt.Sprintf("Failed to create tar index: %v", err))
	}

	return tarBuf.Bytes(), indexData
}

// CreateTarWithInvalidNames creates a TAR archive with incorrectly named files for testing validation failures
func CreateTarWithInvalidNames() ([]byte, []byte) {
	var tarBuf bytes.Buffer
	tw := tar.NewWriter(&tarBuf)

	// Create files with invalid naming (not following %020d.<extension> format)
	invalidFiles := []struct {
		name    string
		content string
	}{
		{"invalid_name.txt", "Content 1"},
		{"123.txt", "Content 2"},                       // Too short
		{"file_00000000000000000003.txt", "Content 3"}, // Wrong format
	}

	for _, file := range invalidFiles {
		header := &tar.Header{
			Name: file.name,
			Size: int64(len(file.content)),
			Mode: 0644,
		}

		err := tw.WriteHeader(header)
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar header: %v", err))
		}

		_, err = tw.Write([]byte(file.content))
		if err != nil {
			panic(fmt.Sprintf("Failed to write tar content: %v", err))
		}
	}

	err := tw.Close()
	if err != nil {
		panic(fmt.Sprintf("Failed to close tar writer: %v", err))
	}

	// Create the tar index
	tarReader := bytes.NewReader(tarBuf.Bytes())
	indexData, err := tarindex.IndexTar(tarReader)
	if err != nil {
		panic(fmt.Sprintf("Failed to create tar index: %v", err))
	}

	return tarBuf.Bytes(), indexData
}

// CreateCompletedDatarangeForAggregation creates a completed datarange for aggregation testing
func (env *TestEnvironment) CreateCompletedDatarangeForAggregation(ctx SpecContext, firstDatapoint, numDatapoints uint64) string {
	// Create proper TAR data and index
	properTarData, properTarIndex := CreateProperTarWithIndex(int(numDatapoints), int64(firstDatapoint))

	// Start upload
	req := &dataranges.UploadDatarangeRequest{
		Datas3tName:         env.TestDatas3tName,
		DataSize:            uint64(len(properTarData)),
		NumberOfDatapoints:  numDatapoints,
		FirstDatapointIndex: firstDatapoint,
	}

	uploadResp, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
	Expect(err).NotTo(HaveOccurred())

	// Upload data and index
	dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(properTarData))
	Expect(err).NotTo(HaveOccurred())
	Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
	dataResp.Body.Close()

	indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
	Expect(err).NotTo(HaveOccurred())
	Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
	indexResp.Body.Close()

	// Complete upload
	completeReq := &dataranges.CompleteUploadRequest{
		DatarangeUploadID: uploadResp.DatarangeID,
	}

	err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
	Expect(err).NotTo(HaveOccurred())

	return uploadResp.ObjectKey
}

// CreateCompletedDatarange creates a completed datarange for testing
func (env *TestEnvironment) CreateCompletedDatarange(ctx SpecContext, firstDatapoint, numDatapoints uint64) (string, string) {
	// Create proper TAR data and index
	properTarData, properTarIndex := CreateProperTarWithIndex(int(numDatapoints), int64(firstDatapoint))

	// Start upload
	req := &dataranges.UploadDatarangeRequest{
		Datas3tName:         env.TestDatas3tName,
		DataSize:            uint64(len(properTarData)),
		NumberOfDatapoints:  numDatapoints,
		FirstDatapointIndex: firstDatapoint,
	}

	uploadResp, err := env.UploadSrv.StartDatarangeUpload(ctx, env.Logger, req)
	Expect(err).NotTo(HaveOccurred())

	// Upload data and index
	dataResp, err := HttpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(properTarData))
	Expect(err).NotTo(HaveOccurred())
	Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
	dataResp.Body.Close()

	indexResp, err := HttpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
	Expect(err).NotTo(HaveOccurred())
	Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
	indexResp.Body.Close()

	// Complete upload
	completeReq := &dataranges.CompleteUploadRequest{
		DatarangeUploadID: uploadResp.DatarangeID,
	}

	err = env.UploadSrv.CompleteDatarangeUpload(ctx, env.Logger, completeReq)
	Expect(err).NotTo(HaveOccurred())

	// Get the created datarange details
	dataObjectKey := uploadResp.ObjectKey

	// Get the index object key from upload details
	uploadDetails, err := env.Queries.GetDatarangeByExactRange(ctx, postgresstore.GetDatarangeByExactRangeParams{
		Name:            env.TestDatas3tName,
		MinDatapointKey: int64(firstDatapoint),
		MaxDatapointKey: int64(firstDatapoint + numDatapoints - 1),
	})
	Expect(err).NotTo(HaveOccurred())
	indexObjectKey := uploadDetails.IndexObjectKey

	return dataObjectKey, indexObjectKey
}
