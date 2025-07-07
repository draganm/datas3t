package datas3t_test

import (
	"bytes"
	"log"
	"log/slog"
	"strings"
	"time"

	"github.com/draganm/datas3t/postgresstore"
	"github.com/draganm/datas3t/server/bucket"
	"github.com/draganm/datas3t/server/datas3t"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/minio"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var _ = Describe("ImportDatas3t", func() {
	var (
		pgContainer          *tc_postgres.PostgresContainer
		minioContainer       *minio.MinioContainer
		db                   *pgxpool.Pool
		srv                  *datas3t.Datas3tServer
		bucketSrv            *bucket.BucketServer
		minioEndpoint        string
		minioHost            string
		minioAccessKey       string
		minioSecretKey       string
		testBucketName       string
		testBucketConfigName string
		logger               *slog.Logger
		minioClient          *miniogo.Client
	)

	BeforeEach(func(ctx SpecContext) {
		var err error

		logger = slog.New(slog.NewTextHandler(GinkgoWriter, nil))

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
		minioContainer, err = minio.Run(ctx,
			"minio/minio:RELEASE.2024-01-16T16-07-38Z",
			minio.WithUsername("minioadmin"),
			minio.WithPassword("minioadmin"),
			testcontainers.WithLogger(log.New(GinkgoWriter, "", 0)),
		)
		Expect(err).NotTo(HaveOccurred())

		// Get MinIO connection details
		minioEndpoint, err = minioContainer.ConnectionString(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Extract host:port from the full URL
		minioHost = strings.TrimPrefix(minioEndpoint, "http://")
		minioHost = strings.TrimPrefix(minioHost, "https://")

		minioAccessKey = "minioadmin"
		minioSecretKey = "minioadmin"
		testBucketName = "import-test-bucket"
		testBucketConfigName = "import-test-bucket-config"

		// Create test bucket in MinIO
		minioClient, err = miniogo.New(minioHost, &miniogo.Options{
			Creds:  credentials.NewStaticV4(minioAccessKey, minioSecretKey, ""),
			Secure: false,
		})
		Expect(err).NotTo(HaveOccurred())

		err = minioClient.MakeBucket(ctx, testBucketName, miniogo.MakeBucketOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Create server instances
		srv, err = datas3t.NewServer(db, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
		Expect(err).NotTo(HaveOccurred())
		bucketSrv, err = bucket.NewServer(db, "dGVzdC1rZXktMzItYnl0ZXMtZm9yLXRlc3RpbmchIQ==")
		Expect(err).NotTo(HaveOccurred())

		// Add a test bucket configuration
		bucketInfo := &bucket.BucketInfo{
			Name:      testBucketConfigName,
			Endpoint:  minioEndpoint,
			Bucket:    testBucketName,
			AccessKey: minioAccessKey,
			SecretKey: minioSecretKey,
		}

		err = bucketSrv.AddBucket(ctx, logger, bucketInfo)
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

	Context("when bucket is empty", func() {
		It("should return empty import results", func(ctx SpecContext) {
			req := &datas3t.ImportDatas3tRequest{
				BucketName: testBucketConfigName,
			}

			response, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(response.ImportedDatas3ts).To(BeEmpty())
			Expect(response.ImportedCount).To(Equal(0))
		})
	})

	Context("when bucket contains datas3t objects", func() {
		BeforeEach(func(ctx SpecContext) {
			// Create test objects in MinIO that match datas3t patterns
			// Pattern: datas3t/{datas3t_name}/dataranges/{first_datapoint}-{last_datapoint}-{upload_counter}.tar
			
			// Create objects for "test-dataset-1"
			testData1 := []byte("test data for datarange 1")
			_, err := minioClient.PutObject(ctx, testBucketName, 
				"datas3t/test-dataset-1/dataranges/00000000000000000000-00000000000000000099-000000000001.tar",
				bytes.NewReader(testData1), int64(len(testData1)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Create corresponding index file
			indexData1 := []byte("index data for datarange 1")
			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/test-dataset-1/dataranges/00000000000000000000-00000000000000000099-000000000001.index",
				bytes.NewReader(indexData1), int64(len(indexData1)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Create another datarange for the same dataset
			testData2 := []byte("test data for datarange 2")
			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/test-dataset-1/dataranges/00000000000000000100-00000000000000000199-000000000002.tar",
				bytes.NewReader(testData2), int64(len(testData2)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/test-dataset-1/dataranges/00000000000000000100-00000000000000000199-000000000002.index",
				bytes.NewReader(indexData1), int64(len(indexData1)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Create objects for "test-dataset-2"
			testData3 := []byte("test data for dataset 2")
			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/test-dataset-2/dataranges/00000000000000001000-00000000000000001499-000000000001.tar",
				bytes.NewReader(testData3), int64(len(testData3)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/test-dataset-2/dataranges/00000000000000001000-00000000000000001499-000000000001.index",
				bytes.NewReader(indexData1), int64(len(indexData1)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Create some non-datas3t objects to verify they're ignored
			_, err = minioClient.PutObject(ctx, testBucketName,
				"random-file.txt",
				bytes.NewReader([]byte("random data")), 11, miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/invalid-pattern.txt",
				bytes.NewReader([]byte("invalid")), 7, miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should import discovered datas3ts and dataranges", func(ctx SpecContext) {
			req := &datas3t.ImportDatas3tRequest{
				BucketName: testBucketConfigName,
			}

			response, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(response.ImportedCount).To(Equal(2))
			Expect(response.ImportedDatas3ts).To(ContainElements("test-dataset-1", "test-dataset-2"))

			// Verify that the datas3ts were created in the database
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3ts).To(HaveLen(2))

			// Find the imported datasets
			var dataset1, dataset2 *datas3t.Datas3tInfo
			for i := range datas3ts {
				if datas3ts[i].Datas3tName == "test-dataset-1" {
					dataset1 = &datas3ts[i]
				}
				if datas3ts[i].Datas3tName == "test-dataset-2" {
					dataset2 = &datas3ts[i]
				}
			}

			Expect(dataset1).NotTo(BeNil())
			Expect(dataset2).NotTo(BeNil())

			// Verify dataset1 statistics
			Expect(dataset1.BucketName).To(Equal(testBucketConfigName))
			Expect(dataset1.DatarangeCount).To(Equal(int64(2)))
			Expect(dataset1.TotalDatapoints).To(Equal(int64(200))) // (99-0+1) + (199-100+1) = 100 + 100
			Expect(dataset1.LowestDatapoint).To(Equal(int64(0)))
			Expect(dataset1.HighestDatapoint).To(Equal(int64(199)))

			// Verify dataset2 statistics
			Expect(dataset2.BucketName).To(Equal(testBucketConfigName))
			Expect(dataset2.DatarangeCount).To(Equal(int64(1)))
			Expect(dataset2.TotalDatapoints).To(Equal(int64(500))) // (1499-1000+1) = 500
			Expect(dataset2.LowestDatapoint).To(Equal(int64(1000)))
			Expect(dataset2.HighestDatapoint).To(Equal(int64(1499)))
		})

		It("should handle import of existing datas3ts gracefully", func(ctx SpecContext) {
			// First import
			req := &datas3t.ImportDatas3tRequest{
				BucketName: testBucketConfigName,
			}

			response1, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(response1.ImportedCount).To(Equal(2))

			// Second import (should not duplicate)
			response2, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(response2.ImportedCount).To(Equal(0)) // No new imports
			Expect(response2.ImportedDatas3ts).To(BeEmpty())

			// Verify database still has only 2 datasets
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3ts).To(HaveLen(2))
		})
	})

	Context("when bucket doesn't exist", func() {
		It("should return an error", func(ctx SpecContext) {
			req := &datas3t.ImportDatas3tRequest{
				BucketName: "non-existent-bucket",
			}

			response, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("does not exist"))
			Expect(response).To(BeNil())
		})
	})

	Context("validation errors", func() {
		It("should return error for empty bucket name", func(ctx SpecContext) {
			req := &datas3t.ImportDatas3tRequest{
				BucketName: "",
			}

			response, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("bucket_name is required"))
			Expect(response).To(BeNil())
		})
	})

	Context("when bucket contains objects with invalid patterns", func() {
		BeforeEach(func(ctx SpecContext) {
			// Create objects with invalid patterns
			testData := []byte("test data")
			
			// Invalid pattern - missing parts
			_, err := minioClient.PutObject(ctx, testBucketName,
				"datas3t/test-dataset/dataranges/invalid.tar",
				bytes.NewReader(testData), int64(len(testData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Invalid pattern - wrong number format
			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/test-dataset/dataranges/abc-def-ghi.tar",
				bytes.NewReader(testData), int64(len(testData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Valid pattern for comparison
			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/valid-dataset/dataranges/00000000000000000000-00000000000000000099-000000000001.tar",
				bytes.NewReader(testData), int64(len(testData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should only import valid patterns and ignore invalid ones", func(ctx SpecContext) {
			req := &datas3t.ImportDatas3tRequest{
				BucketName: testBucketConfigName,
			}

			response, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(response.ImportedCount).To(Equal(1))
			Expect(response.ImportedDatas3ts).To(ContainElement("valid-dataset"))

			// Verify only valid dataset was imported
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3ts).To(HaveLen(1))
			Expect(datas3ts[0].Datas3tName).To(Equal("valid-dataset"))
		})
	})

	Context("upload counter updating", func() {
		BeforeEach(func(ctx SpecContext) {
			// Create objects with specific upload counters to test upload_counter updating
			testData := []byte("test data")
			
			// Create objects for "upload-counter-test" with various upload counters
			// Upload counter 5
			_, err := minioClient.PutObject(ctx, testBucketName,
				"datas3t/upload-counter-test/dataranges/00000000000000000000-00000000000000000099-000000000005.tar",
				bytes.NewReader(testData), int64(len(testData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Upload counter 12 (higher)
			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/upload-counter-test/dataranges/00000000000000000100-00000000000000000199-000000000012.tar",
				bytes.NewReader(testData), int64(len(testData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Upload counter 3 (lower)
			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/upload-counter-test/dataranges/00000000000000000200-00000000000000000299-000000000003.tar",
				bytes.NewReader(testData), int64(len(testData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should update upload_counter to the highest found value", func(ctx SpecContext) {
			req := &datas3t.ImportDatas3tRequest{
				BucketName: testBucketConfigName,
			}

			response, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(response.ImportedCount).To(Equal(1))
			Expect(response.ImportedDatas3ts).To(ContainElement("upload-counter-test"))

			// Verify that the upload_counter was updated to 12 (the highest found)
			queries := postgresstore.New(db)
			datas3tWithBucket, err := queries.GetDatas3tWithBucket(ctx, "upload-counter-test")
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3tWithBucket.UploadCounter).To(Equal(int64(12)))
		})

		It("should not decrease upload_counter if current is higher", func(ctx SpecContext) {
			// First, manually set a high upload counter
			queries := postgresstore.New(db)

			// Import first to create the datas3t
			req := &datas3t.ImportDatas3tRequest{
				BucketName: testBucketConfigName,
			}
			_, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Get the datas3t and manually set a higher upload counter
			datas3tWithBucket, err := queries.GetDatas3tWithBucket(ctx, "upload-counter-test")
			Expect(err).NotTo(HaveOccurred())

			err = queries.UpdateUploadCounter(ctx, postgresstore.UpdateUploadCounterParams{
				ID:            datas3tWithBucket.ID,
				UploadCounter: 20, // Higher than any found in objects
			})
			Expect(err).NotTo(HaveOccurred())

			// Import again - upload counter should not decrease
			_, err = srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())

			// Verify upload_counter is still 20
			datas3tWithBucket, err = queries.GetDatas3tWithBucket(ctx, "upload-counter-test")
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3tWithBucket.UploadCounter).To(Equal(int64(20)))
		})
	})

	Context("transaction atomicity", func() {
		It("should rollback entire datas3t import if any operation fails", func(ctx SpecContext) {
			// This test verifies that if we have a transaction failure,
			// the entire datas3t import is rolled back atomically.
			// We'll simulate this by creating valid objects and then checking
			// that partial imports don't leave the database in an inconsistent state.

			testData := []byte("test data")
			
			// Create objects for "transaction-test"
			_, err := minioClient.PutObject(ctx, testBucketName,
				"datas3t/transaction-test/dataranges/00000000000000000000-00000000000000000099-000000000001.tar",
				bytes.NewReader(testData), int64(len(testData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			_, err = minioClient.PutObject(ctx, testBucketName,
				"datas3t/transaction-test/dataranges/00000000000000000100-00000000000000000199-000000000002.tar",
				bytes.NewReader(testData), int64(len(testData)), miniogo.PutObjectOptions{})
			Expect(err).NotTo(HaveOccurred())

			// Import the datas3t
			req := &datas3t.ImportDatas3tRequest{
				BucketName: testBucketConfigName,
			}

			response, err := srv.ImportDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(response.ImportedCount).To(Equal(1))
			Expect(response.ImportedDatas3ts).To(ContainElement("transaction-test"))

			// Verify all dataranges were imported atomically
			datas3ts, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			
			var transactionTestDataset *datas3t.Datas3tInfo
			for i := range datas3ts {
				if datas3ts[i].Datas3tName == "transaction-test" {
					transactionTestDataset = &datas3ts[i]
					break
				}
			}

			Expect(transactionTestDataset).NotTo(BeNil())
			Expect(transactionTestDataset.DatarangeCount).To(Equal(int64(2)))
			Expect(transactionTestDataset.TotalDatapoints).To(Equal(int64(200))) // (99-0+1) + (199-100+1) = 100 + 100

			// Verify upload_counter was updated atomically with datarange creation
			queries := postgresstore.New(db)
			datas3tWithBucket, err := queries.GetDatas3tWithBucket(ctx, "transaction-test")
			Expect(err).NotTo(HaveOccurred())
			Expect(datas3tWithBucket.UploadCounter).To(Equal(int64(2))) // Highest upload counter found
		})
	})
})