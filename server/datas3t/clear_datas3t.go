package datas3t

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsutil "github.com/draganm/datas3t/aws"
	"github.com/draganm/datas3t/postgresstore"
)

type ClearDatas3tRequest struct {
	Name string `json:"name"`
}

type ClearDatas3tResponse struct {
	DatarangesDeleted int `json:"dataranges_deleted"`
	ObjectsScheduled  int `json:"objects_scheduled"`
}

func (r *ClearDatas3tRequest) Validate(ctx context.Context) error {
	if r.Name == "" {
		return ValidationError(fmt.Errorf("name is required"))
	}
	if !datas3tNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("name must be a valid datas3t name"))
	}
	return nil
}

func (s *Datas3tServer) ClearDatas3t(ctx context.Context, log *slog.Logger, req *ClearDatas3tRequest) (_ *ClearDatas3tResponse, err error) {
	log = log.With("datas3t_name", req.Name)
	log.Info("Starting datas3t clear operation")

	defer func() {
		if err != nil {
			log.Error("Failed to clear datas3t", "error", err)
		} else {
			log.Info("Datas3t clear operation completed successfully")
		}
	}()

	err = req.Validate(ctx)
	if err != nil {
		return nil, err
	}

	// Begin transaction for all database operations
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	txQueries := postgresstore.New(tx)

	// 1. Check if datas3t exists
	datas3tExists, err := s.checkDatas3tExists(ctx, txQueries, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to check if datas3t exists: %w", err)
	}
	if !datas3tExists {
		return nil, fmt.Errorf("datas3t '%s' does not exist", req.Name)
	}

	// 2. Get all dataranges for this datas3t
	dataranges, err := txQueries.ClearDatas3tDataranges(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get dataranges for datas3t: %w", err)
	}

	if len(dataranges) == 0 {
		log.Info("No dataranges found for datas3t, nothing to clear")
		return &ClearDatas3tResponse{
			DatarangesDeleted: 0,
			ObjectsScheduled:  0,
		}, nil
	}

	log.Info("Found dataranges to clear", "count", len(dataranges))

	// 3. Schedule all S3 objects for deletion (data + index objects)
	objectsScheduled, err := s.scheduleAllObjectsForDeletion(ctx, log, txQueries, dataranges)
	if err != nil {
		return nil, fmt.Errorf("failed to schedule objects for deletion: %w", err)
	}

	// 4. Delete all dataranges from database
	err = s.deleteDatarangesFromDatabase(ctx, txQueries, dataranges)
	if err != nil {
		return nil, fmt.Errorf("failed to delete dataranges from database: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &ClearDatas3tResponse{
		DatarangesDeleted: len(dataranges),
		ObjectsScheduled:  objectsScheduled,
	}, nil
}

// checkDatas3tExists checks if the datas3t exists
func (s *Datas3tServer) checkDatas3tExists(ctx context.Context, queries *postgresstore.Queries, datas3tName string) (bool, error) {
	existingDatas3ts, err := queries.AllDatas3ts(ctx)
	if err != nil {
		return false, err
	}

	return slices.Contains(existingDatas3ts, datas3tName), nil
}

// scheduleAllObjectsForDeletion schedules all S3 objects for deletion
func (s *Datas3tServer) scheduleAllObjectsForDeletion(ctx context.Context, log *slog.Logger, queries *postgresstore.Queries, dataranges []postgresstore.ClearDatas3tDatarangesRow) (int, error) {
	if len(dataranges) == 0 {
		return 0, nil
	}

	// Create S3 client from the first datarange (all should have same credentials)
	s3Client, err := s.createS3ClientFromDatarange(ctx, log, dataranges[0])
	if err != nil {
		return 0, fmt.Errorf("failed to create S3 client: %w", err)
	}

	presigner := s3.NewPresignClient(s3Client)
	objectsScheduled := 0

	// Schedule all objects for deletion
	for _, datarange := range dataranges {
		// Schedule data object for deletion
		dataDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(datarange.Bucket),
			Key:    aws.String(datarange.DataObjectKey),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = 24 * time.Hour
		})
		if err != nil {
			log.Error("Failed to presign data object delete", "data_object_key", datarange.DataObjectKey, "error", err)
			continue
		}

		err = queries.ScheduleKeyForDeletion(ctx, dataDeleteReq.URL)
		if err != nil {
			log.Error("Failed to schedule data object deletion", "data_object_key", datarange.DataObjectKey, "error", err)
			continue
		}
		objectsScheduled++

		// Schedule index object for deletion
		indexDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(datarange.Bucket),
			Key:    aws.String(datarange.IndexObjectKey),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = 24 * time.Hour
		})
		if err != nil {
			log.Error("Failed to presign index object delete", "index_object_key", datarange.IndexObjectKey, "error", err)
			continue
		}

		err = queries.ScheduleKeyForDeletion(ctx, indexDeleteReq.URL)
		if err != nil {
			log.Error("Failed to schedule index object deletion", "index_object_key", datarange.IndexObjectKey, "error", err)
			continue
		}
		objectsScheduled++
	}

	log.Info("Scheduled objects for deletion", "total_objects", objectsScheduled)
	return objectsScheduled, nil
}

// createS3ClientFromDatarange creates an S3 client from datarange details
func (s *Datas3tServer) createS3ClientFromDatarange(ctx context.Context, log *slog.Logger, datarange postgresstore.ClearDatas3tDatarangesRow) (*s3.Client, error) {
	// Decrypt credentials
	accessKey, secretKey, err := s.encryptor.DecryptCredentials(datarange.AccessKey, datarange.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	// Use shared AWS utility for S3 client creation with logging
	return awsutil.CreateS3Client(ctx, awsutil.S3ClientConfig{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Endpoint:  datarange.Endpoint,
		Logger:    log,
	})
}

// deleteDatarangesFromDatabase deletes all dataranges from database using the provided transaction
func (s *Datas3tServer) deleteDatarangesFromDatabase(ctx context.Context, queries *postgresstore.Queries, dataranges []postgresstore.ClearDatas3tDatarangesRow) error {
	if len(dataranges) == 0 {
		return nil
	}

	// Extract datarange IDs
	datarangeIDs := make([]int64, len(dataranges))
	for i, datarange := range dataranges {
		datarangeIDs[i] = datarange.ID
	}

	// Delete all dataranges in a single operation
	err := queries.DeleteDatarangesByIDs(ctx, datarangeIDs)
	if err != nil {
		return fmt.Errorf("failed to delete dataranges: %w", err)
	}

	return nil
}