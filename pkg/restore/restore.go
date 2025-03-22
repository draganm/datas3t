package restore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"sort"
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

// DatarangeInfo holds information about a datarange
type DatarangeInfo struct {
	ObjectKey string
	MinKey    int64
	MaxKey    int64
	SizeBytes int64
	// Calculate the range span for easier comparison
	Span int64
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

// filterOverlappingDataranges filters out smaller dataranges that overlap with larger ones
// Returns error if any ranges have partial overlaps (where one range doesn't fully contain the other)
func filterOverlappingDataranges(dataranges []string) ([]DatarangeInfo, []DatarangeInfo, error) {
	// Parse datarange info
	var allRanges []DatarangeInfo
	for _, objectKey := range dataranges {
		parts := strings.Split(objectKey, "/")
		if len(parts) != 4 {
			continue
		}

		filename := parts[3] // <from>-<to>.tar
		rangeStr := strings.TrimSuffix(filename, ".tar")
		rangeParts := strings.Split(rangeStr, "-")
		if len(rangeParts) != 2 {
			continue
		}

		minKey, err := strconv.ParseInt(rangeParts[0], 10, 64)
		if err != nil {
			continue
		}

		maxKey, err := strconv.ParseInt(rangeParts[1], 10, 64)
		if err != nil {
			continue
		}

		// Calculate span (range size)
		span := maxKey - minKey + 1

		allRanges = append(allRanges, DatarangeInfo{
			ObjectKey: objectKey,
			MinKey:    minKey,
			MaxKey:    maxKey,
			Span:      span,
		})
	}

	// First, check for partial overlaps
	for i := 0; i < len(allRanges); i++ {
		for j := i + 1; j < len(allRanges); j++ {
			rangeA := allRanges[i]
			rangeB := allRanges[j]

			// Check if ranges overlap at all
			if rangeA.MinKey <= rangeB.MaxKey && rangeB.MinKey <= rangeA.MaxKey {
				// Check if this is a partial overlap (neither range fully contains the other)
				if !((rangeA.MinKey <= rangeB.MinKey && rangeA.MaxKey >= rangeB.MaxKey) ||
					(rangeB.MinKey <= rangeA.MinKey && rangeB.MaxKey >= rangeA.MaxKey)) {
					return nil, nil, fmt.Errorf("partial overlap detected between dataranges: %s (%d-%d) and %s (%d-%d)",
						rangeA.ObjectKey, rangeA.MinKey, rangeA.MaxKey,
						rangeB.ObjectKey, rangeB.MinKey, rangeB.MaxKey)
				}
			}
		}
	}

	// Sort by span (largest first) and then by min key (for deterministic results)
	sort.Slice(allRanges, func(i, j int) bool {
		if allRanges[i].Span != allRanges[j].Span {
			return allRanges[i].Span > allRanges[j].Span // Largest span first
		}
		return allRanges[i].MinKey < allRanges[j].MinKey // Then by min key
	})

	var keptRanges []DatarangeInfo
	var discardedRanges []DatarangeInfo

	// Now we process ranges in order (largest first)
	for i := 0; i < len(allRanges); i++ {
		currentRange := allRanges[i]

		// Check if this range is covered by any of the kept ranges
		covered := false
		for _, kr := range keptRanges {
			// Check if current range is fully covered by a kept range
			if currentRange.MinKey >= kr.MinKey && currentRange.MaxKey <= kr.MaxKey {
				covered = true
				discardedRanges = append(discardedRanges, currentRange)
				break
			}
		}

		if !covered {
			keptRanges = append(keptRanges, currentRange)
		}
	}

	return keptRanges, discardedRanges, nil
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

		// Filter out overlapping dataranges, keeping only the larger ones
		keptRanges, discardedRanges, err := filterOverlappingDataranges(dataranges)
		if err != nil {
			return fmt.Errorf("failed to filter overlapping dataranges: %w", err)
		}

		config.Logger.Info("filtered overlapping dataranges",
			"dataset", datasetName,
			"total", len(dataranges),
			"kept", len(keptRanges),
			"discarded", len(discardedRanges))

		// Schedule discarded dataranges for deletion
		for _, dr := range discardedRanges {
			// Add the main object key to deletion list
			err = store.InsertKeyToDelete(ctx, dr.ObjectKey)
			if err != nil {
				return fmt.Errorf("failed to schedule datarange for deletion: %w", err)
			}

			// Add the metadata key as well
			err = store.InsertKeyToDelete(ctx, dr.ObjectKey+".metadata")
			if err != nil {
				return fmt.Errorf("failed to schedule datarange metadata for deletion: %w", err)
			}

			config.Logger.Info("scheduled overlapping datarange for deletion",
				"dataset", datasetName,
				"object_key", dr.ObjectKey,
				"min_key", dr.MinKey,
				"max_key", dr.MaxKey)
		}

		// Process each kept datarange
		for _, dr := range keptRanges {
			// Get object size
			headObj, err := config.S3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(config.Bucket),
				Key:    aws.String(dr.ObjectKey),
			})
			if err != nil {
				return fmt.Errorf("failed to get object info: %w", err)
			}

			dr.SizeBytes = *headObj.ContentLength

			if err := processDatarange(ctx, config, store, datasetName, dr); err != nil {
				return fmt.Errorf("failed to process datarange %s: %w", dr.ObjectKey, err)
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

// processDatarange processes a single datarange and inserts it into the database
func processDatarange(ctx context.Context, config Config, store *sqlitestore.Queries, datasetName string, dr DatarangeInfo) error {
	// Insert datarange into database
	dataRangeID, err := store.InsertDataRange(ctx, sqlitestore.InsertDataRangeParams{
		DatasetName:     datasetName,
		ObjectKey:       dr.ObjectKey,
		MinDatapointKey: dr.MinKey,
		MaxDatapointKey: dr.MaxKey,
		SizeBytes:       dr.SizeBytes,
	})
	if err != nil {
		return fmt.Errorf("failed to insert datarange: %w", err)
	}

	// Download and process metadata
	metadataKey := dr.ObjectKey + ".metadata"
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
		"objectKey", dr.ObjectKey,
		"minKey", dr.MinKey,
		"maxKey", dr.MaxKey,
		"sizeBytes", dr.SizeBytes,
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
