package server

import (
	"context"
	"log/slog"
	"net/http"

	"database/sql"
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	tarmmap "github.com/draganm/tar-mmap-go"
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

	mux.HandleFunc("POST /api/v1/datas3t/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		// Check if dataset exists
		exists, err := store.DatasetExists(r.Context(), id)
		if err != nil {
			log.Error("failed to check if dataset exists", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !exists {
			log.Error("dataset not found", "id", id)
			http.Error(w, "dataset not found", http.StatusNotFound)
			return
		}

		// Create temporary file for upload
		tmpFile, err := os.CreateTemp(uploadsPath, fmt.Sprintf("dataset-%s-*.tar", id))
		if err != nil {
			log.Error("failed to create temporary file", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Close and remove the temporary file when done
		defer func() {
			tmpFile.Close()
			os.Remove(tmpFile.Name())
		}()

		// Copy uploaded data to the temporary file
		_, err = io.Copy(tmpFile, r.Body)
		if err != nil {
			log.Error("failed to copy uploaded data to temporary file", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Ensure all data is written to disk
		err = tmpFile.Sync()
		if err != nil {
			log.Error("failed to sync temporary file", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Rewind to the beginning of the file
		_, err = tmpFile.Seek(0, 0)
		if err != nil {
			log.Error("failed to seek to beginning of temporary file", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Open the tar file for validation
		tr, err := tarmmap.Open(tmpFile.Name())
		if err != nil {
			log.Error("failed to open tar file", "error", err)
			http.Error(w, "invalid tar file", http.StatusBadRequest)
			return
		}
		defer tr.Close()

		// Define pattern for file names
		filePattern := regexp.MustCompile(`^(\d{20})\..+$`)

		// Track file sequence numbers
		var fileNumbers []uint64

		// Validate file names and collect sequence numbers
		for _, header := range tr.Headers {
			matches := filePattern.FindStringSubmatch(filepath.Base(header.Name))
			if matches == nil {
				log.Error("invalid file name pattern", "filename", header.Name)
				http.Error(w, fmt.Sprintf("invalid file name pattern: %s", header.Name), http.StatusBadRequest)
				return
			}

			// Parse the sequence number
			seqNumStr := matches[1]
			seqNum, err := strconv.ParseUint(seqNumStr, 10, 64)
			if err != nil {
				log.Error("failed to parse sequence number", "filename", header.Name, "error", err)
				http.Error(w, fmt.Sprintf("invalid sequence number in file name: %s", header.Name), http.StatusBadRequest)
				return
			}

			fileNumbers = append(fileNumbers, seqNum)
		}

		// Check that we have at least one file
		if len(fileNumbers) == 0 {
			log.Error("no valid files in tar archive")
			http.Error(w, "no valid files in tar archive", http.StatusBadRequest)
			return
		}

		// Sort the sequence numbers
		sort.Slice(fileNumbers, func(i, j int) bool {
			return fileNumbers[i] < fileNumbers[j]
		})

		// Check for gaps in the sequence
		for i := 0; i < len(fileNumbers)-1; i++ {
			if fileNumbers[i+1] != fileNumbers[i]+1 {
				log.Error("gap detected in file sequence", "expected", fileNumbers[i]+1, "got", fileNumbers[i+1])
				http.Error(w, fmt.Sprintf("gap in file sequence: expected %d, got %d", fileNumbers[i]+1, fileNumbers[i+1]), http.StatusBadRequest)
				return
			}
		}

		// Get min and max datapoint keys
		minDatapointKey := fileNumbers[0]
		maxDatapointKey := fileNumbers[len(fileNumbers)-1]

		// Create S3 object key with the pattern dataset/<dataset_name>/datapoints/<from>-<to>.tar
		objectKey := fmt.Sprintf("dataset/%s/datapoints/%020d-%020d.tar", id, minDatapointKey, maxDatapointKey)

		// Upload file to S3
		file, err := os.Open(tmpFile.Name())
		if err != nil {
			log.Error("failed to open temporary file for S3 upload", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer file.Close()

		_, err = s3Client.PutObject(r.Context(), &s3.PutObjectInput{
			Bucket: aws.String(s3Config.BucketName),
			Key:    aws.String(objectKey),
			Body:   file,
		})
		if err != nil {
			log.Error("failed to upload file to S3", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Insert data range into database
		err = store.InsertDataRange(r.Context(), sqlitestore.InsertDataRangeParams{
			DatasetName:     id,
			ObjectKey:       objectKey,
			MinDatapointKey: int64(minDatapointKey),
			MaxDatapointKey: int64(maxDatapointKey),
		})
		if err != nil {
			log.Error("failed to insert data range into database", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		log.Info("successfully uploaded data to S3",
			"dataset", id,
			"objectKey", objectKey,
			"minKey", minDatapointKey,
			"maxKey", maxDatapointKey)
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
