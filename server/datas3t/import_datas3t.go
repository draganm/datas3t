package datas3t

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsutil "github.com/draganm/datas3t/aws"
	"github.com/draganm/datas3t/postgresstore"
)

type ImportDatas3tRequest struct {
	BucketName string `json:"bucket_name"`
}

type ImportDatas3tResponse struct {
	ImportedDatas3ts []string `json:"imported_datas3ts"`
	ImportedCount    int      `json:"imported_count"`
}

// Regular expression to match datas3t datarange object keys
// Pattern: datas3t/{datas3t_name}/dataranges/{first_datapoint}-{last_datapoint}-{upload_counter}.tar
var datarangeObjectKeyRegex = regexp.MustCompile(`^datas3t/([^/]+)/dataranges/(\d{20})-(\d{20})-(\d{12})\.tar$`)

func (r *ImportDatas3tRequest) Validate(ctx context.Context) error {
	if r.BucketName == "" {
		return ValidationError(fmt.Errorf("bucket_name is required"))
	}
	return nil
}

func (s *Datas3tServer) ImportDatas3t(ctx context.Context, log *slog.Logger, req *ImportDatas3tRequest) (_ *ImportDatas3tResponse, err error) {
	log = log.With("bucket_name", req.BucketName)
	log.Info("Starting datas3t import")

	defer func() {
		if err != nil {
			log.Error("Failed to import datas3t", "error", err)
		} else {
			log.Info("Datas3t import completed successfully")
		}
	}()

	err = req.Validate(ctx)
	if err != nil {
		return nil, err
	}

	queries := postgresstore.New(s.db)

	// Check if bucket exists
	bucketExists, err := queries.BucketExists(ctx, req.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if !bucketExists {
		return nil, fmt.Errorf("bucket '%s' does not exist", req.BucketName)
	}

	// Get bucket credentials and create S3 client
	bucketCredentials, err := queries.GetBucketCredentials(ctx, req.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket credentials: %w", err)
	}

	s3Client, err := s.createS3ClientForBucket(ctx, log, req.BucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Scan bucket for datas3t objects
	discoveredDatas3ts, err := s.scanBucketForDatas3ts(ctx, log, s3Client, bucketCredentials.Bucket)
	if err != nil {
		return nil, fmt.Errorf("failed to scan bucket: %w", err)
	}

	// Import discovered datas3ts
	importedDatas3ts, err := s.importDiscoveredDatas3ts(ctx, log, req.BucketName, discoveredDatas3ts)
	if err != nil {
		return nil, fmt.Errorf("failed to import discovered datas3ts: %w", err)
	}

	return &ImportDatas3tResponse{
		ImportedDatas3ts: importedDatas3ts,
		ImportedCount:    len(importedDatas3ts),
	}, nil
}

// DatarangeInfo represents a discovered datarange
type DatarangeInfo struct {
	Datas3tName      string
	DataObjectKey    string
	IndexObjectKey   string
	FirstDatapoint   int64
	LastDatapoint    int64
	UploadCounter    int64
	Size             int64
}

func (s *Datas3tServer) createS3ClientForBucket(ctx context.Context, log *slog.Logger, bucketName string) (*s3.Client, error) {
	queries := postgresstore.New(s.db)
	
	// Get bucket credentials directly
	bucketCredentials, err := queries.GetBucketCredentials(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to get bucket credentials: %w", err)
	}

	// Decrypt credentials and create S3 client
	accessKey, secretKey, err := s.encryptor.DecryptCredentials(bucketCredentials.AccessKey, bucketCredentials.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	return awsutil.CreateS3Client(ctx, awsutil.S3ClientConfig{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Endpoint:  bucketCredentials.Endpoint,
		Logger:    log,
	})
}

func (s *Datas3tServer) scanBucketForDatas3ts(ctx context.Context, log *slog.Logger, s3Client *s3.Client, bucketName string) (map[string][]DatarangeInfo, error) {
	discoveredDatas3ts := make(map[string][]DatarangeInfo)

	// List all objects in the bucket with the datas3t prefix
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String("datas3t/"),
	}

	paginator := s3.NewListObjectsV2Paginator(s3Client, listObjectsInput)

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			objectKey := *obj.Key
			
			// Check if this is a datarange TAR file
			matches := datarangeObjectKeyRegex.FindStringSubmatch(objectKey)
			if matches == nil {
				continue // Not a datarange file
			}

			datas3tName := matches[1]
			firstDatapoint, err := strconv.ParseInt(matches[2], 10, 64)
			if err != nil {
				log.Warn("Failed to parse first datapoint", "object_key", objectKey, "error", err)
				continue
			}

			lastDatapoint, err := strconv.ParseInt(matches[3], 10, 64)
			if err != nil {
				log.Warn("Failed to parse last datapoint", "object_key", objectKey, "error", err)
				continue
			}

			uploadCounter, err := strconv.ParseInt(matches[4], 10, 64)
			if err != nil {
				log.Warn("Failed to parse upload counter", "object_key", objectKey, "error", err)
				continue
			}

			// Generate the corresponding index object key
			indexObjectKey := strings.Replace(objectKey, ".tar", ".index", 1)

			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}

			datarangeInfo := DatarangeInfo{
				Datas3tName:      datas3tName,
				DataObjectKey:    objectKey,
				IndexObjectKey:   indexObjectKey,
				FirstDatapoint:   firstDatapoint,
				LastDatapoint:    lastDatapoint,
				UploadCounter:    uploadCounter,
				Size:             size,
			}

			discoveredDatas3ts[datas3tName] = append(discoveredDatas3ts[datas3tName], datarangeInfo)
		}
	}

	log.Info("Discovered datas3ts", "count", len(discoveredDatas3ts))
	return discoveredDatas3ts, nil
}

func (s *Datas3tServer) importDiscoveredDatas3ts(ctx context.Context, log *slog.Logger, bucketName string, discoveredDatas3ts map[string][]DatarangeInfo) ([]string, error) {
	var importedDatas3ts []string

	for datas3tName, dataranges := range discoveredDatas3ts {
		log := log.With("datas3t_name", datas3tName)
		log.Info("Importing datas3t", "datarange_count", len(dataranges))

		// Import this datas3t in a transaction to ensure atomicity
		imported, err := s.importSingleDatas3tInTransaction(ctx, log, bucketName, datas3tName, dataranges)
		if err != nil {
			log.Error("Failed to import datas3t", "error", err)
			continue
		}

		if imported {
			importedDatas3ts = append(importedDatas3ts, datas3tName)
		}
	}

	return importedDatas3ts, nil
}

func (s *Datas3tServer) importSingleDatas3tInTransaction(ctx context.Context, log *slog.Logger, bucketName, datas3tName string, dataranges []DatarangeInfo) (bool, error) {
	// Start a transaction for atomic operations
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	txQueries := postgresstore.New(tx)

	// Check if datas3t already exists
	existingDatas3ts, err := txQueries.AllDatas3ts(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to check existing datas3ts: %w", err)
	}

	datas3tExists := false
	for _, existing := range existingDatas3ts {
		if existing == datas3tName {
			datas3tExists = true
			break
		}
	}

	// Create datas3t if it doesn't exist
	if !datas3tExists {
		err = txQueries.AddDatas3t(ctx, postgresstore.AddDatas3tParams{
			Datas3tName: datas3tName,
			BucketName:  bucketName,
		})
		if err != nil {
			return false, fmt.Errorf("failed to create datas3t: %w", err)
		}
		log.Info("Created datas3t")
	}

	// Get the datas3t with bucket info
	datas3tWithBucket, err := txQueries.GetDatas3tWithBucket(ctx, datas3tName)
	if err != nil {
		return false, fmt.Errorf("failed to get datas3t with bucket: %w", err)
	}

	// Track the maximum upload counter found in this datas3t
	maxUploadCounter := int64(0)
	for _, datarange := range dataranges {
		if datarange.UploadCounter > maxUploadCounter {
			maxUploadCounter = datarange.UploadCounter
		}
	}

	// Import each datarange
	importedCount := 0
	for _, datarange := range dataranges {
		// Check if this datarange already exists
		existingDatarange, err := txQueries.GetDatarangeByExactRange(ctx, postgresstore.GetDatarangeByExactRangeParams{
			Name:                datas3tName,
			MinDatapointKey:     datarange.FirstDatapoint,
			MaxDatapointKey:     datarange.LastDatapoint,
		})
		if err == nil && existingDatarange.ID != 0 {
			// Datarange already exists, skip
			continue
		}

		// Create the datarange
		_, err = txQueries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
			Datas3tID:       datas3tWithBucket.ID,
			DataObjectKey:   datarange.DataObjectKey,
			IndexObjectKey:  datarange.IndexObjectKey,
			MinDatapointKey: datarange.FirstDatapoint,
			MaxDatapointKey: datarange.LastDatapoint,
			SizeBytes:       datarange.Size,
		})
		if err != nil {
			return false, fmt.Errorf("failed to create datarange: %w", err)
		}

		importedCount++
	}

	// Update upload_counter if we found any dataranges and the max counter is higher than current
	if maxUploadCounter > 0 && maxUploadCounter >= datas3tWithBucket.UploadCounter {
		err = txQueries.UpdateUploadCounter(ctx, postgresstore.UpdateUploadCounterParams{
			ID:            datas3tWithBucket.ID,
			UploadCounter: maxUploadCounter,
		})
		if err != nil {
			return false, fmt.Errorf("failed to update upload counter: %w", err)
		}
		log.Info("Updated upload counter", "old_counter", datas3tWithBucket.UploadCounter, "new_counter", maxUploadCounter)
	}

	// Commit the transaction
	err = tx.Commit(ctx)
	if err != nil {
		return false, fmt.Errorf("failed to commit transaction: %w", err)
	}

	if importedCount > 0 {
		log.Info("Imported dataranges", "imported_count", importedCount)
		return true, nil
	}

	return false, nil
}