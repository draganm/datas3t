package keydeletion

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	awsutil "github.com/draganm/datas3t/aws"
	"github.com/draganm/datas3t/postgresstore"
)

// deletionJob represents work to be done by a deletion worker
type deletionJob struct {
	bucketObjects []postgresstore.GetObjectsToDeleteRow
}

// deletionResult represents the result of a deletion job
type deletionResult struct {
	successfulIDs []int64
	err           error
}

func (s *KeyDeletionServer) DeleteObjects(ctx context.Context, log *slog.Logger) (int, error) {
	// Get up to 1000 objects to delete (AWS DeleteObjects limit)
	objects, err := s.queries.GetObjectsToDelete(ctx, 1000)
	if err != nil {
		return 0, err
	}

	if len(objects) == 0 {
		return 0, nil
	}

	log.Info("Processing objects for deletion", "count", len(objects))

	// Group objects by bucket to optimize S3 client creation
	bucketGroups := make(map[string][]postgresstore.GetObjectsToDeleteRow)
	for _, obj := range objects {
		key := fmt.Sprintf("%s|%s", obj.Endpoint, obj.Bucket)
		bucketGroups[key] = append(bucketGroups[key], obj)
	}

	// Create channels for work distribution
	jobs := make(chan deletionJob, len(bucketGroups))
	results := make(chan deletionResult, len(bucketGroups))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < s.concurrency && i < len(bucketGroups); i++ {
		wg.Add(1)
		go s.batchDeletionWorker(ctx, jobs, results, &wg, log)
	}

	// Send jobs
	go func() {
		defer close(jobs)
		for _, bucketObjects := range bucketGroups {
			if len(bucketObjects) > 0 {
				jobs <- deletionJob{bucketObjects: bucketObjects}
			}
		}
	}()

	// Wait for workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	var successfulDeletions []int64
	for result := range results {
		if result.err != nil {
			log.Error("Worker encountered error", "error", result.err)
		} else {
			successfulDeletions = append(successfulDeletions, result.successfulIDs...)
		}
	}

	// Remove successfully deleted objects from database
	if len(successfulDeletions) > 0 {
		err = s.queries.DeleteObjectsToDelete(ctx, successfulDeletions)
		if err != nil {
			return 0, fmt.Errorf("failed to remove deleted objects from database: %w", err)
		}

		log.Info("Removed objects from database", "count", len(successfulDeletions))
	}

	return len(objects), nil
}

// batchDeletionWorker processes deletion jobs from the job channel
func (s *KeyDeletionServer) batchDeletionWorker(ctx context.Context, jobs <-chan deletionJob, results chan<- deletionResult, wg *sync.WaitGroup, log *slog.Logger) {
	defer wg.Done()

	for job := range jobs {
		result := deletionResult{}

		if len(job.bucketObjects) == 0 {
			results <- result
			continue
		}

		// Create S3 client for this bucket group
		firstObj := job.bucketObjects[0]
		s3Client, err := s.createS3Client(ctx, log, firstObj)
		if err != nil {
			result.err = fmt.Errorf("failed to create S3 client for %s/%s: %w", firstObj.Endpoint, firstObj.Bucket, err)
			results <- result
			continue
		}

		// Delete objects in this bucket using batch operations
		successfulIDs := s.batchDeleteObjects(ctx, s3Client, job.bucketObjects, log)
		result.successfulIDs = successfulIDs

		results <- result
	}
}

// createS3Client creates an S3 client using the object's bucket credentials
func (s *KeyDeletionServer) createS3Client(ctx context.Context, log *slog.Logger, obj postgresstore.GetObjectsToDeleteRow) (*s3.Client, error) {
	// Decrypt credentials
	accessKey, secretKey, err := s.encryptor.DecryptCredentials(obj.AccessKey, obj.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	// Use shared AWS utility for S3 client creation with logging
	return awsutil.CreateS3Client(ctx, awsutil.S3ClientConfig{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Endpoint:  obj.Endpoint,
		Logger:    log,
	})
}

// batchDeleteObjects deletes multiple objects from S3 using the batch DeleteObjects API
func (s *KeyDeletionServer) batchDeleteObjects(ctx context.Context, s3Client *s3.Client, objects []postgresstore.GetObjectsToDeleteRow, log *slog.Logger) []int64 {
	if len(objects) == 0 {
		return nil
	}

	bucket := objects[0].Bucket
	var successfulDeletions []int64

	// Process objects in batches of 1000 (AWS limit)
	batchSize := 1000
	for i := 0; i < len(objects); i += batchSize {
		end := i + batchSize
		if end > len(objects) {
			end = len(objects)
		}

		batch := objects[i:end]
		batchSuccesses := s.deleteBatch(ctx, s3Client, bucket, batch, log)
		successfulDeletions = append(successfulDeletions, batchSuccesses...)
	}

	return successfulDeletions
}

// deleteBatch deletes a single batch of objects using AWS DeleteObjects API
func (s *KeyDeletionServer) deleteBatch(ctx context.Context, s3Client *s3.Client, bucket string, objects []postgresstore.GetObjectsToDeleteRow, log *slog.Logger) []int64 {
	if len(objects) == 0 {
		return nil
	}

	// Build delete request
	var deleteObjects []types.ObjectIdentifier
	var objectIDMap = make(map[string]int64) // Map object key to database ID

	for _, obj := range objects {
		if obj.ObjectName == nil {
			log.Error("Object name is nil, skipping", "object_id", obj.ID)
			continue
		}

		deleteObjects = append(deleteObjects, types.ObjectIdentifier{
			Key: obj.ObjectName,
		})
		objectIDMap[*obj.ObjectName] = obj.ID
	}

	if len(deleteObjects) == 0 {
		return nil
	}

	log.Debug("Deleting batch of objects", "bucket", bucket, "count", len(deleteObjects))

	// Execute batch delete with retry logic
	result, err := s.executeWithRetry(ctx, func() (*s3.DeleteObjectsOutput, error) {
		return s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: aws.String(bucket),
			Delete: &types.Delete{
				Objects: deleteObjects,
				Quiet:   aws.Bool(false), // Return info about deleted objects
			},
		})
	}, log)

	var successfulDeletions []int64

	if err != nil {
		log.Error("Batch delete operation failed after retries", "bucket", bucket, "error", err)
		return successfulDeletions
	}

	// Process successful deletions
	for _, deleted := range result.Deleted {
		if deleted.Key != nil {
			if objectID, exists := objectIDMap[*deleted.Key]; exists {
				successfulDeletions = append(successfulDeletions, objectID)
				log.Debug("Successfully deleted object", "object_id", objectID, "key", *deleted.Key)
			}
		}
	}

	// Log any errors from the batch operation
	for _, deleteError := range result.Errors {
		if deleteError.Key != nil {
			log.Error("Failed to delete object in batch",
				"key", *deleteError.Key,
				"code", aws.ToString(deleteError.Code),
				"message", aws.ToString(deleteError.Message))
		}
	}

	log.Info("Batch delete completed",
		"bucket", bucket,
		"requested", len(deleteObjects),
		"successful", len(successfulDeletions),
		"failed", len(result.Errors))

	return successfulDeletions
}

// executeWithRetry executes a function with exponential backoff retry logic
func (s *KeyDeletionServer) executeWithRetry(ctx context.Context, operation func() (*s3.DeleteObjectsOutput, error), log *slog.Logger) (*s3.DeleteObjectsOutput, error) {
	const maxRetries = 3
	const baseDelay = 100 * time.Millisecond
	const maxDelay = 5 * time.Second

	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Calculate delay with exponential backoff and jitter
			delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(attempt-1)))
			if delay > maxDelay {
				delay = maxDelay
			}

			log.Debug("Retrying after delay", "attempt", attempt, "delay", delay)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		result, err := operation()
		if err == nil {
			if attempt > 0 {
				log.Info("Operation succeeded after retry", "attempt", attempt)
			}
			return result, nil
		}

		lastErr = err
		log.Warn("Operation failed, will retry", "attempt", attempt, "error", err)
	}

	return nil, fmt.Errorf("operation failed after %d retries: %w", maxRetries, lastErr)
}
