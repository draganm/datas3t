package restore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	"github.com/klauspost/compress/zstd"
)

// Config holds the necessary configuration for the restore process
type Config struct {
	Logger   *slog.Logger
	DB       *sql.DB
	S3Client *s3.Client
	Bucket   string
}

// DatapointMetadata represents the metadata for a single datapoint
type DatapointMetadata struct {
	ID          uint64 `json:"id,string"`
	BeginOffset uint64 `json:"begin_offset,string"`
	EndOffset   uint64 `json:"end_offset,string"`
	DataHash    string `json:"data_hash"`
}

// RestoreIfNeeded checks if the database is empty and restores from S3 if it is
func RestoreIfNeeded(ctx context.Context, config Config) error {
	config.Logger.Info("checking if database restoration is needed")

	// Check if database is empty by looking for any datasets
	isEmpty, err := isDatabaseEmpty(ctx, config.DB)
	if err != nil {
		return fmt.Errorf("failed to check if database is empty: %w", err)
	}

	if !isEmpty {
		config.Logger.Info("database is not empty, skipping restoration")
		return nil
	}

	config.Logger.Info("database is empty, starting restoration from S3")
	return restoreFromS3(ctx, config)
}

// isDatabaseEmpty checks if the database is empty by checking for any datasets
func isDatabaseEmpty(ctx context.Context, db *sql.DB) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM datasets").Scan(&count)
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

// restoreFromS3 restores the database from S3 objects in a single transaction
func restoreFromS3(ctx context.Context, config Config) error {
	// Create S3 client
	s3Client := config.S3Client

	// Start transaction
	tx, err := config.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Ensure transaction is rolled back on error
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()

	// Create query store with transaction
	store := sqlitestore.New(tx).WithTx(tx)

	// List all objects in the bucket
	datasets, err := discoverDatasets(ctx, s3Client, config.Bucket)
	if err != nil {
		return fmt.Errorf("failed to discover datasets: %w", err)
	}

	config.Logger.Info("discovered datasets from S3", "count", len(datasets))

	// Process each dataset
	for datasetName, dataranges := range datasets {
		// Create dataset in database
		err = store.CreateDataset(ctx, datasetName)
		if err != nil {
			return fmt.Errorf("failed to create dataset %s: %w", datasetName, err)
		}

		config.Logger.Info("created dataset", "name", datasetName)

		// Process each datarange
		for _, dr := range dataranges {
			if err := processDatarange(ctx, config, store, datasetName, dr); err != nil {
				return fmt.Errorf("failed to process datarange %s: %w", dr, err)
			}
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Set transaction to nil to prevent rollback in defer
	tx = nil

	config.Logger.Info("database restoration from S3 completed successfully")
	return nil
}

// discoverDatasets discovers datasets and their dataranges from S3 objects
func discoverDatasets(ctx context.Context, s3Client *s3.Client, bucket string) (map[string][]string, error) {
	// Define regular expression to extract dataset names from object keys
	// Format: dataset/<dataset_name>/datapoints/<from>-<to>.tar.metadata
	datasetPattern := regexp.MustCompile(`^dataset/([^/]+)/datapoints/(\d+)-(\d+)\.tar\.metadata$`)

	// Map to store dataset names and their dataranges
	datasets := make(map[string][]string)

	// List all objects in the bucket
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})

	// Process each page of results
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket: %w", err)
		}

		// Process each object
		for _, obj := range page.Contents {
			key := *obj.Key

			// Check if the object is a metadata file
			matches := datasetPattern.FindStringSubmatch(key)
			if matches == nil {
				continue
			}

			// Extract dataset name
			datasetName := matches[1]

			// Add to datasets map
			if _, ok := datasets[datasetName]; !ok {
				datasets[datasetName] = []string{}
			}

			// Add datarange object key (without .metadata suffix)
			datarangePath := strings.TrimSuffix(key, ".metadata")
			datasets[datasetName] = append(datasets[datasetName], datarangePath)
		}
	}

	return datasets, nil
}

// processDatarange processes a single datarange and inserts it into the database
func processDatarange(ctx context.Context, config Config, store *sqlitestore.Queries, datasetName, objectKey string) error {
	// Extract min and max datapoint keys from object key
	// Format: dataset/<dataset_name>/datapoints/<from>-<to>.tar
	parts := strings.Split(objectKey, "/")
	if len(parts) != 4 {
		return fmt.Errorf("invalid object key format: %s", objectKey)
	}

	filename := parts[3] // <from>-<to>.tar
	rangeStr := strings.TrimSuffix(filename, ".tar")
	rangeParts := strings.Split(rangeStr, "-")
	if len(rangeParts) != 2 {
		return fmt.Errorf("invalid range format in filename: %s", filename)
	}

	minKey, err := strconv.ParseInt(rangeParts[0], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse min key: %w", err)
	}

	maxKey, err := strconv.ParseInt(rangeParts[1], 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse max key: %w", err)
	}

	// Get object size
	headObj, err := config.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(config.Bucket),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to get object info: %w", err)
	}

	sizeBytes := *headObj.ContentLength

	// Insert datarange into database
	dataRangeID, err := store.InsertDataRange(ctx, sqlitestore.InsertDataRangeParams{
		DatasetName:     datasetName,
		ObjectKey:       objectKey,
		MinDatapointKey: minKey,
		MaxDatapointKey: maxKey,
		SizeBytes:       sizeBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to insert datarange: %w", err)
	}

	// Download and process metadata
	metadataKey := objectKey + ".metadata"
	metadata, err := downloadAndDecodeMetadata(ctx, config, metadataKey)
	if err != nil {
		return fmt.Errorf("failed to download and decode metadata: %w", err)
	}

	// Insert datapoints
	for _, dp := range metadata {
		err = store.InsertDatapoint(ctx, sqlitestore.InsertDatapointParams{
			DatarangeID:  dataRangeID,
			DatapointKey: int64(dp.ID),
			BeginOffset:  int64(dp.BeginOffset),
			EndOffset:    int64(dp.EndOffset),
		})
		if err != nil {
			return fmt.Errorf("failed to insert datapoint: %w", err)
		}
	}

	config.Logger.Info("processed datarange",
		"dataset", datasetName,
		"objectKey", objectKey,
		"minKey", minKey,
		"maxKey", maxKey,
		"sizeBytes", sizeBytes,
		"datapoints", len(metadata))

	return nil
}

// downloadAndDecodeMetadata downloads and decodes the metadata file
func downloadAndDecodeMetadata(ctx context.Context, config Config, metadataKey string) ([]DatapointMetadata, error) {
	// Download metadata file
	resp, err := config.S3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(config.Bucket),
		Key:    aws.String(metadataKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to download metadata: %w", err)
	}
	defer resp.Body.Close()

	// Decompress data using zstd
	rawDataReader, err := zstd.NewReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to create zstd decoder: %w", err)
	}
	defer rawDataReader.Close()

	// Unmarshal JSON
	var metadata []DatapointMetadata
	decoder := json.NewDecoder(rawDataReader)
	err = decoder.Decode(&metadata)

	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return metadata, nil
}
