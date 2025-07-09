package dataranges

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/postgresstore"
)

type CancelAggregateRequest struct {
	AggregateUploadID int64 `json:"aggregate_upload_id"`
}

func (s *UploadDatarangeServer) CancelAggregate(
	ctx context.Context,
	log *slog.Logger,
	req *CancelAggregateRequest,
) (err error) {
	log = log.With("aggregate_upload_id", req.AggregateUploadID)
	log.Info("Cancelling aggregate upload")

	defer func() {
		if err != nil {
			log.Error("Failed to cancel aggregate upload", "error", err)
		} else {
			log.Info("Aggregate upload cancelled")
		}
	}()

	// Start a transaction for atomic operations
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	queries := postgresstore.New(tx)
	uploadDetails, err := queries.GetAggregateUploadWithDetails(ctx, req.AggregateUploadID)
	if err != nil {
		return fmt.Errorf("failed to get aggregate upload details: %w", err)
	}

	s3Client, err := s.createS3ClientFromAggregateUploadDetails(ctx, log, uploadDetails)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Create a timeout context for S3 operations (separate from main context)
	s3Ctx, s3Cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer s3Cancel()

	// Perform comprehensive cleanup based on upload type
	err = s.performAggregateComprehensiveCleanup(s3Ctx, log, s3Client, uploadDetails, queries)
	if err != nil {
		log.Warn("Some cleanup operations failed, but continuing with database cleanup", "error", err)
		// Don't return error here - we want to continue with database cleanup
	}

	// Delete the aggregate upload record from the database
	err = queries.DeleteAggregateUpload(ctx, req.AggregateUploadID)
	if err != nil {
		return fmt.Errorf("failed to delete aggregate upload record: %w", err)
	}

	// Note: No aggregate datarange record to delete since dataranges are only created on successful completion
	// The original dataranges remain untouched during cancellation

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// performAggregateComprehensiveCleanup handles thorough cleanup of S3 objects and multipart uploads for aggregates
func (s *UploadDatarangeServer) performAggregateComprehensiveCleanup(
	ctx context.Context,
	log *slog.Logger,
	s3Client *s3.Client,
	uploadDetails postgresstore.GetAggregateUploadWithDetailsRow,
	queries *postgresstore.Queries,
) error {
	var cleanupErrors []error

	if uploadDetails.UploadID == "DIRECT_PUT" {
		log.Info("Cleaning up direct PUT aggregate upload")

		// For direct PUT uploads, try immediate deletion first, then schedule if needed
		err := s.cleanupAggregateDirectPutUpload(ctx, log, s3Client, uploadDetails, queries)
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("direct PUT cleanup failed: %w", err))
		}
	} else {
		log.Info("Cleaning up multipart aggregate upload", "upload_id", uploadDetails.UploadID)

		// For multipart uploads, abort the upload and clean up any objects
		err := s.cleanupAggregateMultipartUpload(ctx, log, s3Client, uploadDetails, queries)
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("multipart cleanup failed: %w", err))
		}
	}

	// Return the first error if any occurred, but log all errors
	if len(cleanupErrors) > 0 {
		for i, err := range cleanupErrors {
			log.Error("Cleanup error", "error_index", i, "error", err)
		}
		return cleanupErrors[0]
	}

	return nil
}

// cleanupAggregateDirectPutUpload handles cleanup for direct PUT aggregate uploads
func (s *UploadDatarangeServer) cleanupAggregateDirectPutUpload(
	ctx context.Context,
	log *slog.Logger,
	s3Client *s3.Client,
	uploadDetails postgresstore.GetAggregateUploadWithDetailsRow,
	queries *postgresstore.Queries,
) error {
	var cleanupErrors []error

	// Try to delete data object immediately
	err := s.attemptImmediateObjectDeletion(ctx, log, s3Client, uploadDetails.Bucket, uploadDetails.DataObjectKey, "data")
	if err != nil {
		log.Warn("Immediate data object deletion failed, scheduling for later", "error", err)
		// Schedule for deletion if immediate deletion fails
		scheduleErr := s.scheduleAggregateObjectForDeletion(ctx, queries, s3Client, uploadDetails.Bucket, uploadDetails.DataObjectKey)
		if scheduleErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to schedule data object deletion: %w", scheduleErr))
		}
	} else {
		log.Info("Successfully deleted data object immediately", "key", uploadDetails.DataObjectKey)
	}

	// Try to delete index object immediately
	err = s.attemptImmediateObjectDeletion(ctx, log, s3Client, uploadDetails.Bucket, uploadDetails.IndexObjectKey, "index")
	if err != nil {
		log.Warn("Immediate index object deletion failed, scheduling for later", "error", err)
		// Schedule for deletion if immediate deletion fails
		scheduleErr := s.scheduleAggregateObjectForDeletion(ctx, queries, s3Client, uploadDetails.Bucket, uploadDetails.IndexObjectKey)
		if scheduleErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to schedule index object deletion: %w", scheduleErr))
		}
	} else {
		log.Info("Successfully deleted index object immediately", "key", uploadDetails.IndexObjectKey)
	}

	if len(cleanupErrors) > 0 {
		return cleanupErrors[0]
	}
	return nil
}

// cleanupAggregateMultipartUpload handles cleanup for multipart aggregate uploads
func (s *UploadDatarangeServer) cleanupAggregateMultipartUpload(
	ctx context.Context,
	log *slog.Logger,
	s3Client *s3.Client,
	uploadDetails postgresstore.GetAggregateUploadWithDetailsRow,
	queries *postgresstore.Queries,
) error {
	var cleanupErrors []error

	// First, abort the multipart upload to clean up any uploaded parts
	log.Info("Aborting multipart upload", "upload_id", uploadDetails.UploadID, "key", uploadDetails.DataObjectKey)
	_, err := s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(uploadDetails.Bucket),
		Key:      aws.String(uploadDetails.DataObjectKey),
		UploadId: aws.String(uploadDetails.UploadID),
	})
	if err != nil {
		log.Error("Failed to abort multipart upload", "error", err)
		cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to abort multipart upload: %w", err))
	} else {
		log.Info("Successfully aborted multipart upload")
	}

	// Try to delete any potentially existing data object (in case the upload was completed but failed validation)
	err = s.attemptImmediateObjectDeletion(ctx, log, s3Client, uploadDetails.Bucket, uploadDetails.DataObjectKey, "data")
	if err != nil {
		log.Debug("Data object deletion failed (expected if object doesn't exist)", "error", err)
		// For multipart uploads, we don't schedule deletion of the data object since
		// AbortMultipartUpload should have handled the parts
	} else {
		log.Info("Successfully deleted completed data object", "key", uploadDetails.DataObjectKey)
	}

	// Try to delete index object immediately
	err = s.attemptImmediateObjectDeletion(ctx, log, s3Client, uploadDetails.Bucket, uploadDetails.IndexObjectKey, "index")
	if err != nil {
		log.Warn("Immediate index object deletion failed, scheduling for later", "error", err)
		// Schedule for deletion if immediate deletion fails
		scheduleErr := s.scheduleAggregateObjectForDeletion(ctx, queries, s3Client, uploadDetails.Bucket, uploadDetails.IndexObjectKey)
		if scheduleErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("failed to schedule index object deletion: %w", scheduleErr))
		}
	} else {
		log.Info("Successfully deleted index object immediately", "key", uploadDetails.IndexObjectKey)
	}

	if len(cleanupErrors) > 0 {
		return cleanupErrors[0]
	}
	return nil
}

// scheduleAggregateObjectForDeletion schedules an object for later deletion using presigned URLs
func (s *UploadDatarangeServer) scheduleAggregateObjectForDeletion(
	ctx context.Context,
	queries *postgresstore.Queries,
	s3Client *s3.Client,
	bucket, key string,
) error {
	presigner := s3.NewPresignClient(s3Client)

	deleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return fmt.Errorf("failed to presign delete object: %w", err)
	}


	// Use the original context for database operations
	originalCtx := context.Background()
	if deadline, ok := ctx.Deadline(); ok {
		dbCtx, cancel := context.WithDeadline(originalCtx, deadline.Add(10*time.Second))
		defer cancel()
		originalCtx = dbCtx
	}

	err = queries.ScheduleKeyForDeletion(originalCtx, deleteReq.URL)
	if err != nil {
		return fmt.Errorf("failed to schedule object deletion: %w", err)
	}

	return nil
}
