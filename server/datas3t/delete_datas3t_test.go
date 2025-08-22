package datas3t_test

import (
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

var _ = Describe("DeleteDatas3t", func() {
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
		testDatas3tName      string
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
		testBucketName = "test-bucket"
		testBucketConfigName = "test-bucket-config"
		testDatas3tName = "test-datas3t"

		// Create test bucket in MinIO
		minioClient, err := miniogo.New(minioHost, &miniogo.Options{
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

		// Add a test datas3t
		err = srv.AddDatas3t(ctx, logger, &datas3t.AddDatas3tRequest{
			Name:   testDatas3tName,
			Bucket: testBucketConfigName,
		})
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

	Context("when datas3t is empty", func() {
		It("should successfully delete the datas3t", func(ctx SpecContext) {
			req := &datas3t.DeleteDatas3tRequest{
				Name: testDatas3tName,
			}

			resp, err := srv.DeleteDatas3t(ctx, logger, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(resp).NotTo(BeNil())

			// Verify datas3t no longer exists
			listResp, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			found := false
			for _, d := range listResp {
				if d.Datas3tName == testDatas3tName {
					found = true
					break
				}
			}
			Expect(found).To(BeFalse())
		})
	})

	Context("when datas3t has dataranges", func() {
		BeforeEach(func(ctx SpecContext) {
			// Get datas3t ID
			queries := postgresstore.New(db)
			datas3tID, err := queries.GetDatas3tIDByName(ctx, testDatas3tName)
			Expect(err).NotTo(HaveOccurred())

			// Insert a datarange directly into the database
			_, err = queries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
				Datas3tID:       datas3tID,
				DataObjectKey:   "test-data.tar",
				IndexObjectKey:  "test-data.index",
				MinDatapointKey: 0,
				MaxDatapointKey: 999,
				SizeBytes:       1024,
			})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should fail with appropriate error message", func(ctx SpecContext) {
			req := &datas3t.DeleteDatas3tRequest{
				Name: testDatas3tName,
			}

			resp, err := srv.DeleteDatas3t(ctx, logger, req)
			Expect(err).To(HaveOccurred())
			Expect(resp).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("cannot delete datas3t"))
			Expect(err.Error()).To(ContainSubstring("contains 1 dataranges"))
			Expect(err.Error()).To(ContainSubstring("Use 'clear' command first"))
		})

		It("should allow deletion after clearing", func(ctx SpecContext) {
			// First clear the datas3t
			clearReq := &datas3t.ClearDatas3tRequest{
				Name: testDatas3tName,
			}
			clearResp, err := srv.ClearDatas3t(ctx, logger, clearReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(clearResp.DatarangesDeleted).To(Equal(1))

			// Now delete should succeed
			deleteReq := &datas3t.DeleteDatas3tRequest{
				Name: testDatas3tName,
			}
			deleteResp, err := srv.DeleteDatas3t(ctx, logger, deleteReq)
			Expect(err).NotTo(HaveOccurred())
			Expect(deleteResp).NotTo(BeNil())

			// Verify datas3t no longer exists
			listResp, err := srv.ListDatas3ts(ctx, logger)
			Expect(err).NotTo(HaveOccurred())
			found := false
			for _, d := range listResp {
				if d.Datas3tName == testDatas3tName {
					found = true
					break
				}
			}
			Expect(found).To(BeFalse())
		})
	})

	Context("when datas3t doesn't exist", func() {
		It("should return an error", func(ctx SpecContext) {
			req := &datas3t.DeleteDatas3tRequest{
				Name: "non-existent-datas3t",
			}

			resp, err := srv.DeleteDatas3t(ctx, logger, req)
			Expect(err).To(HaveOccurred())
			Expect(resp).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("does not exist"))
		})
	})

	Context("validation", func() {
		It("should reject empty name", func(ctx SpecContext) {
			req := &datas3t.DeleteDatas3tRequest{
				Name: "",
			}

			resp, err := srv.DeleteDatas3t(ctx, logger, req)
			Expect(err).To(HaveOccurred())
			Expect(resp).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("name is required"))
		})

		It("should reject invalid name format", func(ctx SpecContext) {
			req := &datas3t.DeleteDatas3tRequest{
				Name: "Invalid Name!",
			}

			resp, err := srv.DeleteDatas3t(ctx, logger, req)
			Expect(err).To(HaveOccurred())
			Expect(resp).To(BeNil())
			Expect(err.Error()).To(ContainSubstring("must be a valid datas3t name"))
		})
	})
})