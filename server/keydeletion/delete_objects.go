package keydeletion

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsutil "github.com/draganm/datas3t/aws"
	"github.com/draganm/datas3t/postgresstore"
)

func (s *KeyDeletionServer) DeleteObjects(ctx context.Context, log *slog.Logger) (int, error) {
	// Get up to 20 objects to delete
	objects, err := s.queries.GetObjectsToDelete(ctx, 20)
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

	var successfulDeletions []int64

	// Process each bucket group
	for _, bucketObjects := range bucketGroups {
		if len(bucketObjects) == 0 {
			continue
		}

		// Create S3 client for this bucket (all objects in group have same credentials)
		firstObj := bucketObjects[0]
		s3Client, err := s.createS3Client(ctx, log, firstObj)
		if err != nil {
			log.Error("Failed to create S3 client", "endpoint", firstObj.Endpoint, "bucket", firstObj.Bucket, "error", err)
			continue
		}

		// Delete objects in this bucket
		for _, obj := range bucketObjects {
			if obj.ObjectName == nil {
				log.Error("Object name is nil, skipping", "object_id", obj.ID)
				continue
			}
			
			err := s.deleteObject(ctx, s3Client, obj)
			if err != nil {
				log.Error("Failed to delete object", "object_name", *obj.ObjectName, "bucket", obj.Bucket, "error", err)
				continue
			}
			
			successfulDeletions = append(successfulDeletions, obj.ID)
			log.Debug("Successfully deleted object", "object_id", obj.ID, "object_name", *obj.ObjectName)
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

// deleteObject deletes a single object from S3
func (s *KeyDeletionServer) deleteObject(ctx context.Context, s3Client *s3.Client, obj postgresstore.GetObjectsToDeleteRow) error {
	_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &obj.Bucket,
		Key:    obj.ObjectName,
	})
	
	// S3 DeleteObject returns success even if the object doesn't exist
	// This is the desired behavior for our use case
	return err
}