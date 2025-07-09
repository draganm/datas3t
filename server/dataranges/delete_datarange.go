package dataranges

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsutil "github.com/draganm/datas3t/aws"
	"github.com/draganm/datas3t/postgresstore"
)

type DeleteDatarangeRequest struct {
	Datas3tName       string `json:"datas3t_name"`
	FirstDatapointKey uint64 `json:"first_datapoint_key"`
	LastDatapointKey  uint64 `json:"last_datapoint_key"`
}

func (r *DeleteDatarangeRequest) Validate(ctx context.Context) error {
	if r.Datas3tName == "" {
		return fmt.Errorf("datas3t_name is required")
	}

	if r.LastDatapointKey < r.FirstDatapointKey {
		return fmt.Errorf("last_datapoint_key must be greater than or equal to first_datapoint_key")
	}

	return nil
}

func (s *UploadDatarangeServer) DeleteDatarange(ctx context.Context, log *slog.Logger, req *DeleteDatarangeRequest) (err error) {
	log = log.With(
		"datas3t_name", req.Datas3tName,
		"first_datapoint_key", req.FirstDatapointKey,
		"last_datapoint_key", req.LastDatapointKey,
	)
	log.Info("Deleting datarange")

	defer func() {
		if err != nil {
			log.Error("Failed to delete datarange", "error", err)
		} else {
			log.Info("Datarange deleted successfully")
		}
	}()

	err = req.Validate(ctx)
	if err != nil {
		return err
	}

	// 1. Get datarange details (read-only operation)
	queries := postgresstore.New(s.db)
	datarangeDetails, err := queries.GetDatarangeByExactRange(ctx, postgresstore.GetDatarangeByExactRangeParams{
		Name:            req.Datas3tName,
		MinDatapointKey: int64(req.FirstDatapointKey),
		MaxDatapointKey: int64(req.LastDatapointKey),
	})
	if err != nil {
		return fmt.Errorf("failed to find datarange: %w", err)
	}

	// 2. Create S3 client
	s3Client, err := s.createS3ClientFromDatarangeDetails(ctx, log, datarangeDetails)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// 3. Delete S3 objects immediately
	err = s.deleteS3Objects(ctx, log, s3Client, datarangeDetails)
	if err != nil {
		// S3 deletion failed - schedule for later deletion and continue with database cleanup
		log.Warn("Immediate S3 deletion failed, scheduling for later", "error", err)
		scheduleErr := s.scheduleDatarangeObjectsForDeletion(ctx, queries, s3Client, datarangeDetails)
		if scheduleErr != nil {
			log.Error("Failed to schedule objects for deletion", "error", scheduleErr)
			// Continue with database cleanup even if scheduling fails
		}
	}

	// 4. Delete from database in a transaction
	return s.deleteDatarangeFromDatabase(ctx, queries, datarangeDetails.ID)
}

// createS3ClientFromDatarangeDetails creates an S3 client from datarange details
func (s *UploadDatarangeServer) createS3ClientFromDatarangeDetails(ctx context.Context, log *slog.Logger, datarangeDetails postgresstore.GetDatarangeByExactRangeRow) (*s3.Client, error) {
	// Decrypt credentials
	accessKey, secretKey, err := s.encryptor.DecryptCredentials(datarangeDetails.AccessKey, datarangeDetails.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	// Use shared AWS utility for S3 client creation with logging
	return awsutil.CreateS3Client(ctx, awsutil.S3ClientConfig{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Endpoint:  datarangeDetails.Endpoint,
		Logger:    log,
	})
}

// deleteS3Objects attempts to delete both data and index objects immediately
func (s *UploadDatarangeServer) deleteS3Objects(ctx context.Context, log *slog.Logger, s3Client *s3.Client, datarangeDetails postgresstore.GetDatarangeByExactRangeRow) error {
	var deletionErrors []error

	// Delete data object
	err := s.attemptImmediateObjectDeletion(ctx, log, s3Client, datarangeDetails.Bucket, datarangeDetails.DataObjectKey, "data")
	if err != nil {
		deletionErrors = append(deletionErrors, fmt.Errorf("failed to delete data object: %w", err))
	}

	// Delete index object
	err = s.attemptImmediateObjectDeletion(ctx, log, s3Client, datarangeDetails.Bucket, datarangeDetails.IndexObjectKey, "index")
	if err != nil {
		deletionErrors = append(deletionErrors, fmt.Errorf("failed to delete index object: %w", err))
	}

	// Return the first error if any occurred
	if len(deletionErrors) > 0 {
		for i, err := range deletionErrors {
			log.Error("S3 deletion error", "error_index", i, "error", err)
		}
		return deletionErrors[0]
	}

	return nil
}

// scheduleDatarangeObjectsForDeletion schedules both data and index objects for later deletion
func (s *UploadDatarangeServer) scheduleDatarangeObjectsForDeletion(ctx context.Context, queries *postgresstore.Queries, s3Client *s3.Client, datarangeDetails postgresstore.GetDatarangeByExactRangeRow) error {
	presigner := s3.NewPresignClient(s3Client)

	// Schedule data object for deletion
	dataDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(datarangeDetails.Bucket),
		Key:    aws.String(datarangeDetails.DataObjectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return fmt.Errorf("failed to presign data object delete: %w", err)
	}

	// Schedule index object for deletion
	indexDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(datarangeDetails.Bucket),
		Key:    aws.String(datarangeDetails.IndexObjectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return fmt.Errorf("failed to presign index object delete: %w", err)
	}

	// Schedule both objects for deletion

	err = queries.ScheduleKeyForDeletion(ctx, dataDeleteReq.URL)
	if err != nil {
		return fmt.Errorf("failed to schedule data object deletion: %w", err)
	}

	err = queries.ScheduleKeyForDeletion(ctx, indexDeleteReq.URL)
	if err != nil {
		return fmt.Errorf("failed to schedule index object deletion: %w", err)
	}

	return nil
}

// deleteDatarangeFromDatabase deletes the datarange record from the database in a transaction
func (s *UploadDatarangeServer) deleteDatarangeFromDatabase(ctx context.Context, queries *postgresstore.Queries, datarangeID int64) error {
	// Begin transaction
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create queries with transaction
	txQueries := queries.WithTx(tx)

	// Delete the datarange record
	err = txQueries.DeleteDatarange(ctx, datarangeID)
	if err != nil {
		return fmt.Errorf("failed to delete datarange record: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
