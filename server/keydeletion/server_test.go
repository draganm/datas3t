package keydeletion_test

import (
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/draganm/datas3t/server/keydeletion"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
	tc_postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestKeyDeletion(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "KeyDeletion Suite")
}

var _ = Describe("KeyDeletionServer", func() {
	var (
		server      *keydeletion.KeyDeletionServer
		pgContainer *tc_postgres.PostgresContainer
		db          *pgxpool.Pool
		logger      *slog.Logger
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

		// Create server instance
		server = keydeletion.NewServer(db)
	})

	AfterEach(func(ctx SpecContext) {
		if db != nil {
			db.Close()
		}
		if pgContainer != nil {
			err := pgContainer.Terminate(ctx)
			Expect(err).NotTo(HaveOccurred())
		}
	})

	Describe("DeleteKeys", func() {
		It("should delete keys from database after successful S3 deletion", func(ctx SpecContext) {
			// Create a test HTTP server that returns 200 for DELETE requests
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.Method).To(Equal("DELETE"))
				w.WriteHeader(http.StatusOK)
			}))
			defer testServer.Close()

			// Insert a test key
			_, err := db.Exec(ctx, 
				"INSERT INTO keys_to_delete (presigned_delete_url) VALUES ($1)",
				testServer.URL+"/test-key")
			Expect(err).ToNot(HaveOccurred())

			// Verify key exists
			var count int
			err = db.QueryRow(ctx, "SELECT COUNT(*) FROM keys_to_delete").Scan(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(1))

			// Run deletion once
			err = server.DeleteKeys(ctx, logger)
			Expect(err).ToNot(HaveOccurred())

			// Verify key was deleted from database
			err = db.QueryRow(ctx, "SELECT COUNT(*) FROM keys_to_delete").Scan(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(0))
		})

		It("should handle 404 responses as successful deletions", func(ctx SpecContext) {
			// Create a test HTTP server that returns 404 for DELETE requests
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.Method).To(Equal("DELETE"))
				w.WriteHeader(http.StatusNotFound)
			}))
			defer testServer.Close()

			// Insert a test key
			_, err := db.Exec(ctx, 
				"INSERT INTO keys_to_delete (presigned_delete_url) VALUES ($1)",
				testServer.URL+"/test-key")
			Expect(err).ToNot(HaveOccurred())

			// Run deletion once
			err = server.DeleteKeys(ctx, logger)
			Expect(err).ToNot(HaveOccurred())

			// Verify key was deleted from database (404 is treated as success)
			var count int
			err = db.QueryRow(ctx, "SELECT COUNT(*) FROM keys_to_delete").Scan(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(0))
		})

		It("should not delete keys from database if S3 deletion fails", func(ctx SpecContext) {
			// Create a test HTTP server that returns 500 for DELETE requests
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.Method).To(Equal("DELETE"))
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer testServer.Close()

			// Insert a test key
			_, err := db.Exec(ctx, 
				"INSERT INTO keys_to_delete (presigned_delete_url) VALUES ($1)",
				testServer.URL+"/test-key")
			Expect(err).ToNot(HaveOccurred())

			// Run deletion once
			err = server.DeleteKeys(ctx, logger)
			Expect(err).ToNot(HaveOccurred())

			// Verify key was NOT deleted from database
			var count int
			err = db.QueryRow(ctx, "SELECT COUNT(*) FROM keys_to_delete").Scan(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(1))
		})

		It("should process up to 20 keys at a time", func(ctx SpecContext) {
			// Create a test HTTP server that returns 200 for DELETE requests
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.Method).To(Equal("DELETE"))
				w.WriteHeader(http.StatusOK)
			}))
			defer testServer.Close()

			// Insert 25 test keys (more than the limit of 20)
			for i := 0; i < 25; i++ {
				_, err := db.Exec(ctx, 
					"INSERT INTO keys_to_delete (presigned_delete_url) VALUES ($1)",
					testServer.URL+"/test-key")
				Expect(err).ToNot(HaveOccurred())
			}

			// Run deletion once
			err := server.DeleteKeys(ctx, logger)
			Expect(err).ToNot(HaveOccurred())

			// Verify only 20 keys were deleted, 5 remain
			var count int
			err = db.QueryRow(ctx, "SELECT COUNT(*) FROM keys_to_delete").Scan(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(5))
		})

		It("should process all keys when fewer than limit", func(ctx SpecContext) {
			// Create a test HTTP server that returns 200 for DELETE requests
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				Expect(r.Method).To(Equal("DELETE"))
				w.WriteHeader(http.StatusOK)
			}))
			defer testServer.Close()

			// Insert 7 test keys (fewer than the limit of 20)
			for i := 0; i < 7; i++ {
				_, err := db.Exec(ctx, 
					"INSERT INTO keys_to_delete (presigned_delete_url) VALUES ($1)",
					testServer.URL+"/test-key")
				Expect(err).ToNot(HaveOccurred())
			}

			// Run deletion once
			err := server.DeleteKeys(ctx, logger)
			Expect(err).ToNot(HaveOccurred())

			// Verify all 7 keys were deleted, 0 remain
			var count int
			err = db.QueryRow(ctx, "SELECT COUNT(*) FROM keys_to_delete").Scan(&count)
			Expect(err).ToNot(HaveOccurred())
			Expect(count).To(Equal(0))
		})
	})
})