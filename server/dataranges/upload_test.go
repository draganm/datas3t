package dataranges_test

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"archive/tar"

	"github.com/aws/aws-sdk-go-v2/aws"
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

// Helper function to perform HTTP PUT requests
func httpPut(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	return client.Do(req)
}

// createProperTarWithIndex creates a proper TAR archive with correctly named files and returns both the tar data and index
func createProperTarWithIndex(numFiles int, startIndex int64) ([]byte, []byte) {
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

// createTarWithInvalidNames creates a TAR archive with incorrectly named files for testing validation failures
func createTarWithInvalidNames() ([]byte, []byte) {
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

var _ = Describe("UploadDatarange", func() {
	var (
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		db                   *pgxpool.Pool
		queries              *postgresstore.Queries
		uploadSrv            *dataranges.UploadDatarangeServer
		bucketSrv            *bucket.BucketServer
		datas3tSrv           *datas3t.Datas3tServer
		minioEndpoint        string
		minioHost            string
		minioAccessKey       string
		minioSecretKey       string
		testBucketName       string
		testBucketConfigName string
		testDatas3tName      string
		s3Client             *s3.Client
		logger               *slog.Logger
	)

	BeforeEach(func(ctx SpecContext) {

		// Create logger that writes to GinkgoWriter for test visibility
		logger = slog.New(slog.NewTextHandler(GinkgoWriter, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))

		var err error

		// Start PostgreSQL container
		pgContainer, err = tc_postgres.Run(ctx,
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
		connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
		Expect(err).NotTo(HaveOccurred())

		// Connect to PostgreSQL
		db, err = pgxpool.New(ctx, connStr)
		Expect(err).NotTo(HaveOccurred())

		// Initialize queries instance
		queries = postgresstore.New(db)

		// Run migrations
		connStrForMigration, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
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
		minioContainer, err = minio.Run(
			ctx,
			"minio/minio:RELEASE.2024-01-16T16-07-38Z",
			minio.WithUsername("minioadmin"),
			minio.WithPassword("minioadmin"),
			testcontainers.WithLogger(log.New(GinkgoWriter, "", 0)),
		)
		Expect(err).NotTo(HaveOccurred())

		// Get MinIO connection details
		minioEndpoint, err = minioContainer.ConnectionString(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Extract host:port from the full URL for both minioHost and storage endpoint
		minioHost = strings.TrimPrefix(minioEndpoint, "http://")
		minioHost = strings.TrimPrefix(minioHost, "https://")

		// Store endpoint without protocol (consistent with bucket_info.go approach)
		minioEndpoint = minioHost

		minioAccessKey = "minioadmin"
		minioSecretKey = "minioadmin"
		testBucketName = "test-bucket"
		testBucketConfigName = "test-bucket-config"
		testDatas3tName = "test-datas3t"

		// Create test bucket in MinIO
		minioClient, err := miniogo.New(minioHost, &miniogo.Options{
			Creds:  miniocreds.NewStaticV4(minioAccessKey, minioSecretKey, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		err = minioClient.MakeBucket(ctx, testBucketName, miniogo.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Create S3 client for test operations using shared utility
		s3Client, err = awsutil.CreateS3Client(ctx, awsutil.S3ClientConfig{
			AccessKey: minioAccessKey,
			SecretKey: minioSecretKey,
			Endpoint:  minioEndpoint,
			Logger:    logger,
		})
		Expect(err).NotTo(HaveOccurred())

		// Create server instances
		uploadSrv, err = dataranges.NewServer(db, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
		Expect(err).NotTo(HaveOccurred())
		bucketSrv, err = bucket.NewServer(db, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
		Expect(err).NotTo(HaveOccurred())
		datas3tSrv, err = datas3t.NewServer(db, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
		Expect(err).NotTo(HaveOccurred())

		// Add test bucket configuration
		bucketInfo := &bucket.BucketInfo{
			Name:      testBucketConfigName,
			Endpoint:  minioEndpoint,
			Bucket:    testBucketName,
			AccessKey: minioAccessKey,
			SecretKey: minioSecretKey,
		}

		err = bucketSrv.AddBucket(ctx, logger, bucketInfo)
		Expect(err).NotTo(HaveOccurred())

		// Add test datas3t
		datas3tReq := &datas3t.AddDatas3tRequest{
			Bucket: testBucketConfigName,
			Name:   testDatas3tName,
		}

		err = datas3tSrv.AddDatas3t(ctx, logger, datas3tReq)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func(ctx SpecContext) {
		if db != nil {
			db.Close()
		}
		if pgContainer != nil {
			err := pgContainer.Terminate(ctx)
			Expect(err).NotTo(HaveOccurred())
		}
		if minioContainer != nil {
			err := minioContainer.Terminate(ctx)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	Context("StartDatarangeUpload", func() {
		Context("when starting a valid small upload (direct PUT)", func() {
			It("should successfully create upload with direct PUT URLs", func(ctx SpecContext) {
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            1024, // Small size < 5MB
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				resp, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp).NotTo(BeNil())
				Expect(resp.UseDirectPut).To(BeTrue())
				Expect(resp.PresignedDataPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedMultipartUploadPutURLs).To(BeEmpty())
				Expect(resp.DatarangeID).To(BeNumerically(">", 0))
				Expect(resp.FirstDatapointIndex).To(Equal(uint64(0)))

				// Verify database state - no datarange record yet (created on completion)
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))

				// Verify upload record
				uploads, err := queries.GetAllDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())

				var uploadCount int
				for _, upload := range uploads {
					uploadCount++
					Expect(upload.UploadID).To(Equal("DIRECT_PUT"))
					Expect(upload.FirstDatapointIndex).To(Equal(int64(0)))
					Expect(upload.NumberOfDatapoints).To(Equal(int64(10)))
					Expect(upload.DataSize).To(Equal(int64(1024)))
				}
				Expect(uploadCount).To(Equal(1))
			})
		})

		Context("when starting a valid large upload (multipart)", func() {
			It("should successfully create upload with multipart URLs", func(ctx SpecContext) {
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            10 * 1024 * 1024, // 10MB > 5MB threshold
					NumberOfDatapoints:  1000,
					FirstDatapointIndex: 100,
				}

				resp, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(resp).NotTo(BeNil())
				Expect(resp.UseDirectPut).To(BeFalse())
				Expect(resp.PresignedDataPutURL).To(BeEmpty())
				Expect(resp.PresignedIndexPutURL).NotTo(BeEmpty())
				Expect(resp.PresignedMultipartUploadPutURLs).NotTo(BeEmpty())
				Expect(len(resp.PresignedMultipartUploadPutURLs)).To(Equal(2)) // 10MB / 5MB = 2 parts
				Expect(resp.DatarangeID).To(BeNumerically(">", 0))
				Expect(resp.FirstDatapointIndex).To(Equal(uint64(100)))

				// Verify database state - no datarange record yet (created on completion)
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))

				// Verify upload record
				uploadIDs, err := queries.GetDatarangeUploadIDs(ctx)
				Expect(err).NotTo(HaveOccurred())

				Expect(len(uploadIDs)).To(Equal(1))
				uploadID := uploadIDs[0]
				Expect(uploadID).NotTo(Equal("DIRECT_PUT"))
				Expect(uploadID).NotTo(BeEmpty())
			})
		})

		Context("when validation fails", func() {
			It("should reject empty datas3t name", func(ctx SpecContext) {
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         "",
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("datas3t_name is required"))

				// Verify no database changes
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))
			})

			It("should reject zero data size", func(ctx SpecContext) {
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            0,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("data_size must be greater than 0"))

				// Verify no database changes
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))
			})

			It("should reject zero number of datapoints", func(ctx SpecContext) {
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            1024,
					NumberOfDatapoints:  0,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("number_of_datapoints must be greater than 0"))

				// Verify no database changes
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))
			})

			It("should reject non-existent datas3t", func(ctx SpecContext) {
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         "non-existent-datas3t",
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to find datas3t 'non-existent-datas3t'"))

				// Verify no database changes
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))
			})
		})

		Context("when handling overlapping dataranges", func() {
			BeforeEach(func(ctx SpecContext) {
				// Create an existing datarange from 0-99
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 0,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should allow overlapping upload ranges", func(ctx SpecContext) {
				// Try to create overlapping range 50-149 (should now be allowed)
				// Note: Ranges 0-99 (from BeforeEach) and 50-149 overlap from 50-99
				// This is allowed during upload start - disambiguation happens at completion
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 50,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred()) // Should succeed now

				// Verify both upload records exist (overlapping uploads are allowed)
				// Only the first one to complete will succeed; others will fail at completion
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(2)))
			})

			It("should allow adjacent ranges", func(ctx SpecContext) {
				// Create adjacent range 100-199 (no overlap)
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            1024,
					NumberOfDatapoints:  100,
					FirstDatapointIndex: 100,
				}

				_, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())

				// Verify two upload records exist
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(2)))
			})
		})
	})

	Context("CompleteUpload", func() {
		var uploadResp *dataranges.UploadDatarangeResponse
		var testData []byte
		var testIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Start an upload
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         testDatas3tName,
				DataSize:            1024,
				NumberOfDatapoints:  10,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Prepare test data
			testData = make([]byte, 1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			testIndex = []byte("test index data")
		})

		Context("when completing a successful direct PUT upload", func() {
			It("should complete successfully with both files uploaded", func(ctx SpecContext) {
				// Upload data file
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify datarange record still exists
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))
			})
		})

		Context("when index file is missing", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload only data file (no index)
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("index file not found"))

				// Verify cleanup happened - both upload and datarange records should be deleted
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2))) // Both data and index objects scheduled for deletion
			})
		})

		Context("when data file is missing", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload only index file (no data)
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get uploaded object info"))

				// Verify cleanup happened
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})

		Context("when data size is wrong", func() {
			It("should fail and schedule cleanup", func(ctx SpecContext) {
				// Upload wrong size data
				wrongSizeData := make([]byte, 512) // Expected 1024, uploading 512
				for i := range wrongSizeData {
					wrongSizeData[i] = byte(i % 256)
				}

				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(wrongSizeData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload (should fail)
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))
				Expect(err.Error()).To(ContainSubstring("expected 1024, got 512"))

				// Verify cleanup happened
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})
	})

	Context("Multipart Upload Complete", func() {
		var uploadResp *dataranges.UploadDatarangeResponse
		var testData []byte
		var testIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Start a large upload that requires multipart
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         testDatas3tName,
				DataSize:            10 * 1024 * 1024, // 10MB
				NumberOfDatapoints:  1000,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(uploadResp.UseDirectPut).To(BeFalse())

			// Prepare test data
			testData = make([]byte, 10*1024*1024)
			for i := range testData {
				testData[i] = byte(i % 256)
			}

			testIndex = []byte("test index data for multipart")
		})

		Context("when completing a successful multipart upload", func() {
			It("should complete successfully with all parts uploaded", func(ctx SpecContext) {
				// Upload all parts
				partSize := 5 * 1024 * 1024 // 5MB per part
				var etags []string

				for i, url := range uploadResp.PresignedMultipartUploadPutURLs {
					startOffset := i * partSize
					endOffset := startOffset + partSize
					if endOffset > len(testData) {
						endOffset = len(testData)
					}

					partData := testData[startOffset:endOffset]
					resp, err := httpPut(url, bytes.NewReader(partData))
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).To(Equal(http.StatusOK))

					// Get ETag from response
					etag := resp.Header.Get("ETag")
					Expect(etag).NotTo(BeEmpty())
					etags = append(etags, etag)
					resp.Body.Close()
				}

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
					UploadIDs:         etags,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify datarange record still exists
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))

				// Verify the file was actually uploaded and accessible
				getResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadResp.ObjectKey),
				})
				Expect(err).NotTo(HaveOccurred())
				defer getResp.Body.Close()

				downloadedData, err := io.ReadAll(getResp.Body)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(downloadedData)).To(Equal(len(testData)))
			})
		})

		Context("when multipart upload fails due to missing parts", func() {
			It("should fail to complete with incomplete parts", func(ctx SpecContext) {
				// Upload only the first part (missing second part)
				partSize := 5 * 1024 * 1024 // 5MB per part
				partData := testData[:partSize]

				resp, err := httpPut(uploadResp.PresignedMultipartUploadPutURLs[0], bytes.NewReader(partData))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))

				etag := resp.Header.Get("ETag")
				Expect(etag).NotTo(BeEmpty())
				resp.Body.Close()

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Try to complete with only one ETag (should fail)
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
					UploadIDs:         []string{etag}, // Missing second part
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				// When only partial data is uploaded, it fails with size mismatch before multipart completion
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))

				// Verify records were cleaned up (both upload and datarange should be deleted on failure)
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify cleanup tasks were scheduled
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(2)))
			})
		})
	})

	Context("CancelDatarangeUpload", func() {
		Context("when cancelling a direct PUT upload", func() {
			var uploadResp *dataranges.UploadDatarangeResponse

			BeforeEach(func(ctx SpecContext) {
				// Start a small upload that uses direct PUT
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            1024, // Small size < 5MB
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadResp.UseDirectPut).To(BeTrue())
			})

			It("should successfully cancel upload and clean up upload records", func(ctx SpecContext) {
				// Verify initial state
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(1)))

				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0))) // No datarange created yet

				// Cancel the upload
				cancelReq := &dataranges.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount2, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount2).To(Equal(int64(0)))

				// Verify no datarange record exists (never created)
				datarangeCount2, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(int64(0)))

				// Verify no cleanup tasks needed (objects didn't exist, so immediate deletion succeeded)
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(0)))
			})
		})

		Context("when cancelling a multipart upload", func() {
			var uploadResp *dataranges.UploadDatarangeResponse

			BeforeEach(func(ctx SpecContext) {
				// Start a large upload that requires multipart
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            10 * 1024 * 1024, // 10MB
					NumberOfDatapoints:  1000,
					FirstDatapointIndex: 100,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadResp.UseDirectPut).To(BeFalse())
			})

			It("should successfully cancel multipart upload and clean up upload records", func(ctx SpecContext) {
				// Verify initial state
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(1)))

				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0))) // No datarange created yet

				// Cancel the upload
				cancelReq := &dataranges.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload record was deleted
				uploadCount2, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount2).To(Equal(int64(0)))

				// Verify no datarange record exists (never created)
				datarangeCount2, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(int64(0)))

				// Verify no cleanup tasks needed (objects didn't exist, so immediate deletion succeeded)
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(0)))
			})

			It("should handle partial uploads by cancelling and cleaning up properly", func(ctx SpecContext) {
				// Upload one part to simulate partial upload
				testData := make([]byte, 5*1024*1024) // 5MB for first part
				for i := range testData {
					testData[i] = byte(i % 256)
				}

				resp, err := httpPut(uploadResp.PresignedMultipartUploadPutURLs[0], bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(resp.StatusCode).To(Equal(http.StatusOK))
				resp.Body.Close()

				// Cancel the upload (should abort multipart and clean up)
				cancelReq := &dataranges.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify database cleanup
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(0)))

				// Verify no cleanup tasks needed (multipart abort cleaned up parts, objects didn't exist)
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(0)))
			})

			It("should delete uploaded multipart parts and index when cancelling", func(ctx SpecContext) {
				// Upload both parts and index to S3
				testData := make([]byte, 10*1024*1024) // 10MB total
				for i := range testData {
					testData[i] = byte(i % 256)
				}
				testIndex := []byte("test index data for cancel multipart")

				// Upload both multipart parts
				partSize := 5 * 1024 * 1024 // 5MB per part
				for i, url := range uploadResp.PresignedMultipartUploadPutURLs {
					startOffset := i * partSize
					endOffset := startOffset + partSize
					if endOffset > len(testData) {
						endOffset = len(testData)
					}

					partData := testData[startOffset:endOffset]
					resp, err := httpPut(url, bytes.NewReader(partData))
					Expect(err).NotTo(HaveOccurred())
					Expect(resp.StatusCode).To(Equal(http.StatusOK))
					resp.Body.Close()
				}

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Get the actual index object key from the database
				uploadDetails, err := queries.GetDatarangeUploadWithDetails(ctx, uploadResp.DatarangeID)
				Expect(err).NotTo(HaveOccurred())

				// Verify index was uploaded
				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadDetails.IndexObjectKey),
				})
				Expect(err).NotTo(HaveOccurred()) // Index should exist

				// Cancel the upload
				cancelReq := &dataranges.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify database cleanup
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify multipart upload was aborted (data object should not exist)
				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadResp.ObjectKey),
				})
				Expect(err).To(HaveOccurred()) // Data object should not exist after abort

				// Verify index was deleted
				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadDetails.IndexObjectKey),
				})
				Expect(err).To(HaveOccurred()) // Index should be deleted

				// Verify no cleanup tasks needed (immediate deletion succeeded)
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(0)))
			})
		})

		Context("when cancelling a direct PUT upload with actual data", func() {
			var uploadResp *dataranges.UploadDatarangeResponse

			BeforeEach(func(ctx SpecContext) {
				// Start a small upload that uses direct PUT
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            1024, // Small size < 5MB
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadResp.UseDirectPut).To(BeTrue())
			})

			It("should delete uploaded data object and index when cancelling", func(ctx SpecContext) {
				// Upload both data and index to S3
				testData := make([]byte, 1024)
				for i := range testData {
					testData[i] = byte(i % 256)
				}
				testIndex := []byte("test index data for cancel direct PUT")

				// Upload data file
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(testData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload index file
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(testIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Get the actual index object key from the database
				uploadDetails, err := queries.GetDatarangeUploadWithDetails(ctx, uploadResp.DatarangeID)
				Expect(err).NotTo(HaveOccurred())

				// Verify both objects were uploaded
				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadResp.ObjectKey),
				})
				Expect(err).NotTo(HaveOccurred()) // Data object should exist

				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadDetails.IndexObjectKey),
				})
				Expect(err).NotTo(HaveOccurred()) // Index should exist

				// Cancel the upload
				cancelReq := &dataranges.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify database cleanup
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))

				// Verify data object was deleted
				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadResp.ObjectKey),
				})
				Expect(err).To(HaveOccurred()) // Data object should be deleted

				// Verify index was deleted
				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(uploadDetails.IndexObjectKey),
				})
				Expect(err).To(HaveOccurred()) // Index should be deleted

				// Verify no cleanup tasks needed (immediate deletion succeeded)
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(0)))
			})
		})

		Context("when validation fails", func() {
			It("should reject non-existent upload ID", func(ctx SpecContext) {
				cancelReq := &dataranges.CancelUploadRequest{
					DatarangeUploadID: 999999, // Non-existent ID
				}

				err := uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get datarange upload details"))
			})
		})

		Context("when upload has already been cancelled", func() {
			var uploadResp *dataranges.UploadDatarangeResponse

			BeforeEach(func(ctx SpecContext) {
				// Start an upload
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            1024,
					NumberOfDatapoints:  10,
					FirstDatapointIndex: 0,
				}

				var err error
				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())

				// Cancel it once
				cancelReq := &dataranges.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}
				err = uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).NotTo(HaveOccurred())
			})

			It("should return error when trying to cancel again", func(ctx SpecContext) {
				// Try to cancel again
				cancelReq := &dataranges.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err := uploadSrv.CancelDatarangeUpload(ctx, logger, cancelReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to get datarange upload details"))
			})
		})
	})

	Context("Tar Index Validation", func() {
		var uploadResp *dataranges.UploadDatarangeResponse
		var properTarData []byte
		var properTarIndex []byte

		BeforeEach(func(ctx SpecContext) {
			// Create a proper TAR archive with correctly named files
			properTarData, properTarIndex = createProperTarWithIndex(5, 0) // 5 files starting from index 0

			// Start an upload with the correct size
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         testDatas3tName,
				DataSize:            uint64(len(properTarData)),
				NumberOfDatapoints:  5,
				FirstDatapointIndex: 0,
			}

			var err error
			uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
		})

		Context("when tar validation succeeds", func() {
			It("should validate proper tar files with correct index", func(ctx SpecContext) {
				// Upload proper tar data
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(properTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload proper tar index
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload - should succeed with validation
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify upload completed successfully
				uploadCount, err := queries.CountDatarangeUploads(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(uploadCount).To(Equal(int64(0)))
			})
		})

		Context("when tar validation fails due to size mismatch", func() {
			It("should reject tar with incorrect size", func(ctx SpecContext) {
				// Create tar data with wrong size (truncate it)
				wrongSizeTarData := properTarData[:len(properTarData)-100]

				// Upload wrong size tar data
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(wrongSizeTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload proper tar index (which will now be inconsistent with data)
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload - should fail during validation
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("uploaded size mismatch"))
			})
		})

		Context("when tar validation fails due to invalid file names", func() {
			It("should reject tar with incorrectly named files", func(ctx SpecContext) {
				// Create tar with wrong file names
				invalidTarData, invalidTarIndex := createTarWithInvalidNames()

				// Update the upload request with correct size for the invalid tar
				err := uploadSrv.CancelDatarangeUpload(ctx, logger, &dataranges.CancelUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				})
				Expect(err).NotTo(HaveOccurred())

				// Start a new upload with correct size
				req := &dataranges.UploadDatarangeRequest{
					Datas3tName:         testDatas3tName,
					DataSize:            uint64(len(invalidTarData)),
					NumberOfDatapoints:  3,
					FirstDatapointIndex: 0,
				}

				uploadResp, err = uploadSrv.StartDatarangeUpload(ctx, logger, req)
				Expect(err).NotTo(HaveOccurred())

				// Upload invalid tar data
				dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(invalidTarData))
				Expect(err).NotTo(HaveOccurred())
				Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
				dataResp.Body.Close()

				// Upload invalid tar index
				indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(invalidTarIndex))
				Expect(err).NotTo(HaveOccurred())
				Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
				indexResp.Body.Close()

				// Complete the upload - should fail during validation
				completeReq := &dataranges.CompleteUploadRequest{
					DatarangeUploadID: uploadResp.DatarangeID,
				}

				err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("tar index validation failed"))
				Expect(err.Error()).To(ContainSubstring("invalid file name format"))
			})
		})
	})

	Context("DeleteDatarange", func() {
		var testDataObjectKey string
		var testIndexObjectKey string

		// Helper function to create a completed datarange for testing deletion
		createCompletedDatarange := func(ctx SpecContext, firstDatapoint, numDatapoints uint64) {
			// Create proper TAR data and index
			properTarData, properTarIndex := createProperTarWithIndex(int(numDatapoints), int64(firstDatapoint))

			// Start upload
			req := &dataranges.UploadDatarangeRequest{
				Datas3tName:         testDatas3tName,
				DataSize:            uint64(len(properTarData)),
				NumberOfDatapoints:  numDatapoints,
				FirstDatapointIndex: firstDatapoint,
			}

			uploadResp, err := uploadSrv.StartDatarangeUpload(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Upload data and index
			dataResp, err := httpPut(uploadResp.PresignedDataPutURL, bytes.NewReader(properTarData))
			Expect(err).NotTo(HaveOccurred())
			Expect(dataResp.StatusCode).To(Equal(http.StatusOK))
			dataResp.Body.Close()

			indexResp, err := httpPut(uploadResp.PresignedIndexPutURL, bytes.NewReader(properTarIndex))
			Expect(err).NotTo(HaveOccurred())
			Expect(indexResp.StatusCode).To(Equal(http.StatusOK))
			indexResp.Body.Close()

			// Complete upload
			completeReq := &dataranges.CompleteUploadRequest{
				DatarangeUploadID: uploadResp.DatarangeID,
			}

			err = uploadSrv.CompleteDatarangeUpload(ctx, logger, completeReq)
			Expect(err).NotTo(HaveOccurred())

			// Get the created datarange details
			testDataObjectKey = uploadResp.ObjectKey

			// Get the index object key from upload details (before it was deleted)
			uploadDetails, err := queries.GetDatarangeByExactRange(ctx, postgresstore.GetDatarangeByExactRangeParams{
				Name:            testDatas3tName,
				MinDatapointKey: int64(firstDatapoint),
				MaxDatapointKey: int64(firstDatapoint + numDatapoints - 1),
			})
			Expect(err).NotTo(HaveOccurred())
			testIndexObjectKey = uploadDetails.IndexObjectKey
		}

		Context("when deleting an existing datarange", func() {
			BeforeEach(func(ctx SpecContext) {
				createCompletedDatarange(ctx, 0, 10) // Create datarange from 0-9
			})

			It("should successfully delete the datarange and S3 objects", func(ctx SpecContext) {
				// Verify initial state
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))

				// Verify S3 objects exist
				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(testDataObjectKey),
				})
				Expect(err).NotTo(HaveOccurred()) // Data object should exist

				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(testIndexObjectKey),
				})
				Expect(err).NotTo(HaveOccurred()) // Index object should exist

				// Delete the datarange
				deleteReq := &dataranges.DeleteDatarangeRequest{
					Datas3tName:       testDatas3tName,
					FirstDatapointKey: 0,
					LastDatapointKey:  9,
				}

				err = uploadSrv.DeleteDatarange(ctx, logger, deleteReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify datarange was deleted from database
				datarangeCount2, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(int64(0)))

				// Verify S3 objects were deleted
				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(testDataObjectKey),
				})
				Expect(err).To(HaveOccurred()) // Data object should be deleted

				_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(testIndexObjectKey),
				})
				Expect(err).To(HaveOccurred()) // Index object should be deleted

				// Verify no cleanup tasks were scheduled (immediate deletion succeeded)
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(0)))
			})
		})

		Context("when validation fails", func() {
			It("should reject empty datas3t name", func(ctx SpecContext) {
				deleteReq := &dataranges.DeleteDatarangeRequest{
					Datas3tName:       "",
					FirstDatapointKey: 0,
					LastDatapointKey:  9,
				}

				err := uploadSrv.DeleteDatarange(ctx, logger, deleteReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("datas3t_name is required"))
			})

			It("should reject invalid datapoint key range", func(ctx SpecContext) {
				deleteReq := &dataranges.DeleteDatarangeRequest{
					Datas3tName:       testDatas3tName,
					FirstDatapointKey: 10,
					LastDatapointKey:  5, // Last < First
				}

				err := uploadSrv.DeleteDatarange(ctx, logger, deleteReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("last_datapoint_key must be greater than or equal to first_datapoint_key"))
			})

			It("should reject non-existent datas3t", func(ctx SpecContext) {
				deleteReq := &dataranges.DeleteDatarangeRequest{
					Datas3tName:       "non-existent-datas3t",
					FirstDatapointKey: 0,
					LastDatapointKey:  9,
				}

				err := uploadSrv.DeleteDatarange(ctx, logger, deleteReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to find datarange"))
			})

			It("should reject non-existent datarange", func(ctx SpecContext) {
				deleteReq := &dataranges.DeleteDatarangeRequest{
					Datas3tName:       testDatas3tName,
					FirstDatapointKey: 100, // Non-existent range
					LastDatapointKey:  199,
				}

				err := uploadSrv.DeleteDatarange(ctx, logger, deleteReq)
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("failed to find datarange"))
			})
		})

		Context("when datarange exists but S3 objects don't exist", func() {
			BeforeEach(func(ctx SpecContext) {
				createCompletedDatarange(ctx, 0, 10) // Create datarange from 0-9

				// Manually delete the S3 objects to simulate a scenario where they don't exist
				_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(testDataObjectKey),
				})
				Expect(err).NotTo(HaveOccurred())

				_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(testBucketName),
					Key:    aws.String(testIndexObjectKey),
				})
				Expect(err).NotTo(HaveOccurred())
			})

			It("should successfully delete the datarange even when S3 objects don't exist", func(ctx SpecContext) {
				// Verify initial state
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(1)))

				// Delete the datarange
				deleteReq := &dataranges.DeleteDatarangeRequest{
					Datas3tName:       testDatas3tName,
					FirstDatapointKey: 0,
					LastDatapointKey:  9,
				}

				err = uploadSrv.DeleteDatarange(ctx, logger, deleteReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify datarange was deleted from database
				datarangeCount2, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(int64(0)))

				// Verify no cleanup tasks were scheduled (objects didn't exist)
				cleanupTasks, err := queries.CountKeysToDelete(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(cleanupTasks).To(Equal(int64(0)))
			})
		})

		Context("when multiple dataranges exist", func() {
			BeforeEach(func(ctx SpecContext) {
				// Create multiple dataranges
				createCompletedDatarange(ctx, 0, 10)  // 0-9
				createCompletedDatarange(ctx, 10, 10) // 10-19
				createCompletedDatarange(ctx, 100, 5) // 100-104
			})

			It("should delete only the specified datarange", func(ctx SpecContext) {
				// Verify initial state
				datarangeCount, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount).To(Equal(int64(3)))

				// Delete the middle datarange (10-19)
				deleteReq := &dataranges.DeleteDatarangeRequest{
					Datas3tName:       testDatas3tName,
					FirstDatapointKey: 10,
					LastDatapointKey:  19,
				}

				err = uploadSrv.DeleteDatarange(ctx, logger, deleteReq)
				Expect(err).NotTo(HaveOccurred())

				// Verify only one datarange was deleted
				datarangeCount2, err := queries.CountDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(datarangeCount2).To(Equal(int64(2)))

				// Verify the correct dataranges remain
				remainingDataranges, err := queries.GetAllDataranges(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(remainingDataranges)).To(Equal(2))

				// Check that we have the expected ranges (0-9 and 100-104)
				minKeys := []int64{remainingDataranges[0].MinDatapointKey, remainingDataranges[1].MinDatapointKey}
				maxKeys := []int64{remainingDataranges[0].MaxDatapointKey, remainingDataranges[1].MaxDatapointKey}

				Expect(minKeys).To(ContainElements(int64(0), int64(100)))
				Expect(maxKeys).To(ContainElements(int64(9), int64(104)))
			})
		})
	})
})
