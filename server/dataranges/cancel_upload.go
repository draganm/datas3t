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

type CancelUploadRequest struct {
	DatarangeUploadID int64 `json:"datarange_upload_id"`
}

func (s *UploadDatarangeServer) CancelDatarangeUpload(
	ctx context.Context,
	log *slog.Logger,
	req *CancelUploadRequest,
) (err error) {
	log = log.With("datarange_upload_id", req.DatarangeUploadID)
	log.Info("Cancelling datarange upload")

	defer func() {
		if err != nil {
			log.Error("Failed to cancel datarange upload", "error", err)
		} else {
			log.Info("Datarange upload cancelled")
		}
	}()

	// Start a transaction for atomic operations
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	queries := postgresstore.New(tx)
	uploadDetails, err := queries.GetDatarangeUploadWithDetails(ctx, req.DatarangeUploadID)
	if err != nil {
		return fmt.Errorf("failed to get datarange upload details: %w", err)
	}

	s3Client, err := s.createS3ClientFromUploadDetails(ctx, log, uploadDetails)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Create a timeout context for S3 operations (separate from main context)
	s3Ctx, s3Cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer s3Cancel()

	// Perform comprehensive cleanup based on upload type
	err = s.performComprehensiveCleanup(s3Ctx, log, s3Client, uploadDetails, queries)
	if err != nil {
		log.Warn("Some cleanup operations failed, but continuing with database cleanup", "error", err)
		// Don't return error here - we want to continue with database cleanup
	}

	// Delete the upload record from the database
	err = queries.DeleteDatarangeUpload(ctx, req.DatarangeUploadID)
	if err != nil {
		return fmt.Errorf("failed to delete datarange upload record: %w", err)
	}

	// Note: No datarange record to delete since dataranges are only created on successful completion

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// performComprehensiveCleanup handles thorough cleanup of S3 objects and multipart uploads
func (s *UploadDatarangeServer) performComprehensiveCleanup(
	ctx context.Context,
	log *slog.Logger,
	s3Client *s3.Client,
	uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow,
	queries *postgresstore.Queries,
) error {
	var cleanupErrors []error

	if uploadDetails.UploadID == "DIRECT_PUT" {
		log.Info("Cleaning up direct PUT upload")

		// For direct PUT uploads, try immediate deletion first, then schedule if needed
		err := s.cleanupDirectPutUpload(ctx, log, s3Client, uploadDetails, queries)
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("direct PUT cleanup failed: %w", err))
		}
	} else {
		log.Info("Cleaning up multipart upload", "upload_id", uploadDetails.UploadID)

		// For multipart uploads, abort the upload and clean up any objects
		err := s.cleanupMultipartUpload(ctx, log, s3Client, uploadDetails, queries)
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

// cleanupDirectPutUpload handles cleanup for direct PUT uploads
func (s *UploadDatarangeServer) cleanupDirectPutUpload(
	ctx context.Context,
	log *slog.Logger,
	s3Client *s3.Client,
	uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow,
	queries *postgresstore.Queries,
) error {
	var cleanupErrors []error

	// Try to delete data object immediately
	err := s.attemptImmediateObjectDeletion(ctx, log, s3Client, uploadDetails.Bucket, uploadDetails.DataObjectKey, "data")
	if err != nil {
		log.Warn("Immediate data object deletion failed, scheduling for later", "error", err)
		// Schedule for deletion if immediate deletion fails
		scheduleErr := s.scheduleObjectForDeletion(ctx, queries, s3Client, uploadDetails.Bucket, uploadDetails.DataObjectKey)
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
		scheduleErr := s.scheduleObjectForDeletion(ctx, queries, s3Client, uploadDetails.Bucket, uploadDetails.IndexObjectKey)
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

// cleanupMultipartUpload handles cleanup for multipart uploads
func (s *UploadDatarangeServer) cleanupMultipartUpload(
	ctx context.Context,
	log *slog.Logger,
	s3Client *s3.Client,
	uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow,
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
		scheduleErr := s.scheduleObjectForDeletion(ctx, queries, s3Client, uploadDetails.Bucket, uploadDetails.IndexObjectKey)
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

// attemptImmediateObjectDeletion tries to delete an S3 object immediately
func (s *UploadDatarangeServer) attemptImmediateObjectDeletion(
	ctx context.Context,
	log *slog.Logger,
	s3Client *s3.Client,
	bucket, key, objectType string,
) error {
	log = log.With("bucket", bucket, "key", key, "object_type", objectType)

	// First check if the object exists
	_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		log.Debug("Object does not exist, skipping deletion", "error", err)
		return nil // Object doesn't exist, nothing to delete
	}

	// Object exists, attempt deletion
	_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete %s object: %w", objectType, err)
	}

	log.Info("Successfully deleted object", "object_type", objectType)
	return nil
}

// scheduleObjectForDeletion schedules an object for later deletion using presigned URLs
func (s *UploadDatarangeServer) scheduleObjectForDeletion(
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

// scheduleObjectsForDeletion schedules multiple objects for deletion (legacy function for backward compatibility)
func (s *UploadDatarangeServer) scheduleObjectsForDeletion(ctx context.Context, queries *postgresstore.Queries, s3Client *s3.Client, uploadDetails postgresstore.GetDatarangeUploadWithDetailsRow, indexObjectKey string) error {
	// Schedule data object for deletion
	err := s.scheduleObjectForDeletion(ctx, queries, s3Client, uploadDetails.Bucket, uploadDetails.DataObjectKey)
	if err != nil {
		return fmt.Errorf("failed to schedule data object deletion: %w", err)
	}

	// Schedule index object for deletion
	err = s.scheduleObjectForDeletion(ctx, queries, s3Client, uploadDetails.Bucket, indexObjectKey)
	if err != nil {
		return fmt.Errorf("failed to schedule index object deletion: %w", err)
	}

	return nil
}
