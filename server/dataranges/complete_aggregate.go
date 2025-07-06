package dataranges

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	awsutil "github.com/draganm/datas3t/aws"
	"github.com/draganm/datas3t/postgresstore"
	"github.com/draganm/datas3t/tarindex"
	"github.com/jackc/pgx/v5/pgtype"
)

type CompleteAggregateRequest struct {
	AggregateUploadID int64    `json:"aggregate_upload_id"`
	UploadIDs         []string `json:"upload_ids,omitempty"` // Only used for multipart uploads
}

func (s *UploadDatarangeServer) CompleteAggregate(ctx context.Context, log *slog.Logger, req *CompleteAggregateRequest) (err error) {
	log = log.With("aggregate_upload_id", req.AggregateUploadID)
	log.Info("Completing aggregate upload")

	defer func() {
		if err != nil {
			log.Error("Failed to complete aggregate upload", "error", err)
		} else {
			log.Info("Aggregate upload completed successfully")
		}
	}()

	// 1. Get aggregate upload details (read-only operation)
	queries := postgresstore.New(s.db)
	uploadDetails, err := queries.GetAggregateUploadWithDetails(ctx, req.AggregateUploadID)
	if err != nil {
		return fmt.Errorf("failed to get aggregate upload details: %w", err)
	}

	// 2. Create S3 client
	s3Client, err := s.createS3ClientFromAggregateUploadDetails(ctx, log, uploadDetails)
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// 3. Perform all S3 operations first (without database changes)
	err = s.performAggregateS3Operations(ctx, s3Client, uploadDetails, req.UploadIDs)
	if err != nil {
		// S3 operations failed - handle cleanup in a single transaction
		return s.handleAggregateFailureInTransaction(ctx, queries, s3Client, uploadDetails, err)
	}

	// 4. S3 operations succeeded - complete in a single transaction
	return s.handleAggregateSuccessInTransaction(ctx, queries, req.AggregateUploadID)
}

// performAggregateS3Operations handles all S3 network calls without any database changes
func (s *UploadDatarangeServer) performAggregateS3Operations(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetAggregateUploadWithDetailsRow, uploadIDs []string) error {
	// Complete upload (different logic for direct PUT vs multipart)
	isDirectPut := uploadDetails.UploadID == "DIRECT_PUT"

	if !isDirectPut {
		// Handle multipart upload completion
		var completedParts []types.CompletedPart
		for i, uploadID := range uploadIDs {
			completedParts = append(completedParts, types.CompletedPart{
				ETag:       aws.String(uploadID),
				PartNumber: aws.Int32(int32(i + 1)),
			})
		}

		completeInput := &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(uploadDetails.Bucket),
			Key:      aws.String(uploadDetails.DataObjectKey),
			UploadId: aws.String(uploadDetails.UploadID),
			MultipartUpload: &types.CompletedMultipartUpload{
				Parts: completedParts,
			},
		}

		_, err := s3Client.CompleteMultipartUpload(ctx, completeInput)
		if err != nil {
			// Abort the upload if completion fails
			s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(uploadDetails.Bucket),
				Key:      aws.String(uploadDetails.DataObjectKey),
				UploadId: aws.String(uploadDetails.UploadID),
			})
			return fmt.Errorf("failed to complete multipart upload: %w", err)
		}
	}
	// For direct PUT, no completion step needed - the object should already be uploaded

	// Check if the index is present
	indexObjectKey := uploadDetails.IndexObjectKey
	_, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(indexObjectKey),
	})
	if err != nil {
		return fmt.Errorf("index file not found: %w", err)
	}

	// Check the size of the uploaded data
	headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.DataObjectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to get uploaded object info: %w", err)
	}

	if headResp.ContentLength == nil || *headResp.ContentLength != uploadDetails.TotalDataSize {
		return fmt.Errorf("uploaded size mismatch: expected %d, got %d",
			uploadDetails.TotalDataSize, aws.ToInt64(headResp.ContentLength))
	}

	// Perform tar index validation
	err = s.validateAggregateTarIndex(ctx, s3Client, uploadDetails)
	if err != nil {
		return fmt.Errorf("aggregate tar index validation failed: %w", err)
	}

	return nil
}

// handleAggregateSuccessInTransaction performs all success-case database operations in a single transaction
func (s *UploadDatarangeServer) handleAggregateSuccessInTransaction(ctx context.Context, queries *postgresstore.Queries, aggregateUploadID int64) error {
	// Begin transaction
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create queries with transaction
	txQueries := queries.WithTx(tx)

	// Get upload details first
	uploadDetails, err := txQueries.GetAggregateUploadWithDetails(ctx, aggregateUploadID)
	if err != nil {
		return fmt.Errorf("failed to get upload details: %w", err)
	}

	// Create the new aggregate datarange record
	_, err = txQueries.CreateDatarange(ctx, postgresstore.CreateDatarangeParams{
		Datas3tID:       uploadDetails.Datas3tID,
		DataObjectKey:   uploadDetails.DataObjectKey,
		IndexObjectKey:  uploadDetails.IndexObjectKey,
		MinDatapointKey: uploadDetails.FirstDatapointIndex,
		MaxDatapointKey: uploadDetails.LastDatapointIndex,
		SizeBytes:       uploadDetails.TotalDataSize,
	})
	if err != nil {
		return fmt.Errorf("failed to create aggregate datarange: %w", err)
	}

	// Schedule the original dataranges for deletion
	err = s.scheduleOriginalDatarangesForDeletion(ctx, txQueries, uploadDetails)
	if err != nil {
		return fmt.Errorf("failed to schedule original dataranges for deletion: %w", err)
	}

	// Delete the original dataranges from the database
	err = txQueries.DeleteDatarangesByIDs(ctx, uploadDetails.SourceDatarangeIds)
	if err != nil {
		return fmt.Errorf("failed to delete original dataranges: %w", err)
	}

	// Delete the aggregate upload record
	err = txQueries.DeleteAggregateUpload(ctx, aggregateUploadID)
	if err != nil {
		return fmt.Errorf("failed to delete aggregate upload record: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// handleAggregateFailureInTransaction performs all failure-case database operations in a single transaction
func (s *UploadDatarangeServer) handleAggregateFailureInTransaction(ctx context.Context, queries *postgresstore.Queries, s3Client *s3.Client, uploadDetails postgresstore.GetAggregateUploadWithDetailsRow, originalErr error) error {
	// Begin transaction
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Create queries with transaction
	txQueries := queries.WithTx(tx)

	// Generate presigned delete URLs for both data and index objects
	presigner := s3.NewPresignClient(s3Client)

	// Schedule data object for deletion
	dataDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.DataObjectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return fmt.Errorf("failed to presign data object delete: %w", err)
	}

	// Schedule index object for deletion
	indexDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.IndexObjectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour
	})
	if err != nil {
		return fmt.Errorf("failed to presign index object delete: %w", err)
	}

	// Schedule both objects for deletion
	deleteAfter := pgtype.Timestamp{
		Time:  time.Now().Add(time.Hour), // Delete after 1 hour
		Valid: true,
	}

	err = txQueries.ScheduleKeyForDeletion(ctx, postgresstore.ScheduleKeyForDeletionParams{
		PresignedDeleteUrl: dataDeleteReq.URL,
		DeleteAfter:        deleteAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to schedule data object deletion: %w", err)
	}

	err = txQueries.ScheduleKeyForDeletion(ctx, postgresstore.ScheduleKeyForDeletionParams{
		PresignedDeleteUrl: indexDeleteReq.URL,
		DeleteAfter:        deleteAfter,
	})
	if err != nil {
		return fmt.Errorf("failed to schedule index object deletion: %w", err)
	}

	// Delete the aggregate upload record (no datarange record exists yet since upload failed)
	err = txQueries.DeleteAggregateUpload(ctx, uploadDetails.ID)
	if err != nil {
		return fmt.Errorf("failed to delete aggregate upload record: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// Return the original error that caused the failure
	return originalErr
}

// scheduleOriginalDatarangesForDeletion schedules the original dataranges for deletion from S3
func (s *UploadDatarangeServer) scheduleOriginalDatarangesForDeletion(ctx context.Context, queries *postgresstore.Queries, uploadDetails postgresstore.GetAggregateUploadWithDetailsRow) error {
	// Get all the original dataranges that need to be deleted
	originalDataranges, err := queries.GetDatarangesInRange(ctx, postgresstore.GetDatarangesInRangeParams{
		Name:            uploadDetails.Datas3tName,
		MinDatapointKey: uploadDetails.FirstDatapointIndex,
		MaxDatapointKey: uploadDetails.LastDatapointIndex,
	})
	if err != nil {
		return fmt.Errorf("failed to get original dataranges for deletion: %w", err)
	}

	// Create S3 client for scheduling deletions
	s3Client, err := s.createS3ClientFromAggregateUploadDetails(ctx, slog.Default(), uploadDetails)
	if err != nil {
		return fmt.Errorf("failed to create S3 client for scheduling deletions: %w", err)
	}

	presigner := s3.NewPresignClient(s3Client)

	// Schedule each datarange's data and index objects for deletion
	for _, dr := range originalDataranges {
		// Schedule data object for deletion
		dataDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(dr.Bucket),
			Key:    aws.String(dr.DataObjectKey),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = 24 * time.Hour
		})
		if err != nil {
			return fmt.Errorf("failed to presign data object delete for datarange %d: %w", dr.ID, err)
		}

		// Schedule index object for deletion
		indexDeleteReq, err := presigner.PresignDeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(dr.Bucket),
			Key:    aws.String(dr.IndexObjectKey),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = 24 * time.Hour
		})
		if err != nil {
			return fmt.Errorf("failed to presign index object delete for datarange %d: %w", dr.ID, err)
		}

		// Schedule both objects for deletion
		deleteAfter := pgtype.Timestamp{
			Time:  time.Now().Add(time.Hour), // Delete after 1 hour
			Valid: true,
		}

		err = queries.ScheduleKeyForDeletion(ctx, postgresstore.ScheduleKeyForDeletionParams{
			PresignedDeleteUrl: dataDeleteReq.URL,
			DeleteAfter:        deleteAfter,
		})
		if err != nil {
			return fmt.Errorf("failed to schedule data object deletion for datarange %d: %w", dr.ID, err)
		}

		err = queries.ScheduleKeyForDeletion(ctx, postgresstore.ScheduleKeyForDeletionParams{
			PresignedDeleteUrl: indexDeleteReq.URL,
			DeleteAfter:        deleteAfter,
		})
		if err != nil {
			return fmt.Errorf("failed to schedule index object deletion for datarange %d: %w", dr.ID, err)
		}
	}

	return nil
}

func (s *UploadDatarangeServer) createS3ClientFromAggregateUploadDetails(ctx context.Context, log *slog.Logger, uploadDetails postgresstore.GetAggregateUploadWithDetailsRow) (*s3.Client, error) {
	// Decrypt credentials
	accessKey, secretKey, err := s.encryptor.DecryptCredentials(uploadDetails.AccessKey, uploadDetails.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	// Use shared AWS utility for S3 client creation with logging
	return awsutil.CreateS3Client(ctx, awsutil.S3ClientConfig{
		AccessKey: accessKey,
		SecretKey: secretKey,
		Endpoint:  uploadDetails.Endpoint,
		Logger:    log,
	})
}

// validateAggregateTarIndex performs validation of the aggregate tar index
func (s *UploadDatarangeServer) validateAggregateTarIndex(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetAggregateUploadWithDetailsRow) error {
	// Download the index file
	indexResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.IndexObjectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to download index file: %w", err)
	}
	defer indexResp.Body.Close()

	// Read the index into memory
	indexData, err := io.ReadAll(indexResp.Body)
	if err != nil {
		return fmt.Errorf("failed to read index data: %w", err)
	}

	// Parse the index
	if len(indexData)%16 != 0 {
		// Skip validation if index is not in expected tar index format
		return nil
	}

	numEntries := len(indexData) / 16
	if numEntries == 0 {
		return fmt.Errorf("index file is empty")
	}

	// Calculate expected number of datapoints
	expectedDatapoints := uploadDetails.LastDatapointIndex - uploadDetails.FirstDatapointIndex + 1
	if int64(numEntries) != expectedDatapoints {
		return fmt.Errorf("index entry count mismatch: expected %d entries, got %d", expectedDatapoints, numEntries)
	}

	// Create a fake index to use the existing validation API
	fakeIndex := &tarindex.Index{
		Bytes: indexData,
	}

	// Validate tar file size against expected size
	err = s.validateAggregateTarFileSize(ctx, s3Client, uploadDetails, fakeIndex)
	if err != nil {
		return fmt.Errorf("aggregate tar file size validation failed: %w", err)
	}

	// Validate a few random entries
	err = s.validateAggregateRandomEntries(ctx, s3Client, uploadDetails, fakeIndex, numEntries)
	if err != nil {
		return fmt.Errorf("aggregate entry validation failed: %w", err)
	}

	return nil
}

// validateAggregateTarFileSize validates the actual tar file size against the expected size
func (s *UploadDatarangeServer) validateAggregateTarFileSize(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetAggregateUploadWithDetailsRow, index *tarindex.Index) error {
	// Get the actual file size from the database
	expectedSizeFromDB := uploadDetails.TotalDataSize

	// Calculate expected size from the tar index
	numFiles := index.NumFiles()
	if numFiles == 0 {
		return fmt.Errorf("tar index is empty")
	}

	// Get metadata for the last file
	lastFileMetadata, err := index.GetFileMetadata(numFiles - 1)
	if err != nil {
		return fmt.Errorf("failed to get last file metadata: %w", err)
	}

	// Calculate expected tar file size
	headerSize := int64(512)
	paddedContentSize := ((lastFileMetadata.Size + 511) / 512) * 512
	endOfArchiveSize := int64(1024)

	calculatedSize := lastFileMetadata.Start + headerSize + paddedContentSize + endOfArchiveSize

	// Check against database size
	if expectedSizeFromDB != calculatedSize {
		return fmt.Errorf("tar size mismatch: database says %d bytes, calculated from index %d bytes",
			expectedSizeFromDB, calculatedSize)
	}

	// Double-check against actual S3 object size
	headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.DataObjectKey),
	})
	if err != nil {
		return fmt.Errorf("failed to get actual object size: %w", err)
	}

	actualSize := aws.ToInt64(headResp.ContentLength)
	if actualSize != calculatedSize {
		return fmt.Errorf("tar size mismatch: actual S3 object is %d bytes, calculated from index %d bytes",
			actualSize, calculatedSize)
	}

	return nil
}

// validateAggregateRandomEntries validates a few random entries in the aggregate
func (s *UploadDatarangeServer) validateAggregateRandomEntries(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetAggregateUploadWithDetailsRow, index *tarindex.Index, numEntries int) error {
	// Validate first, last, and a few random entries
	var indicesToCheck []int

	// Always include first entry
	indicesToCheck = append(indicesToCheck, 0)

	// Always include last entry (if different from first)
	if numEntries > 1 {
		indicesToCheck = append(indicesToCheck, numEntries-1)
	}

	// Add up to 3 random entries from the middle
	if numEntries > 2 {
		rand.New(rand.NewSource(time.Now().UnixNano()))
		maxRandomSamples := min(3, numEntries-2)
		selectedIndices := make(map[int]bool)

		for len(selectedIndices) < maxRandomSamples {
			idx := rand.Intn(numEntries-2) + 1
			if !selectedIndices[idx] {
				selectedIndices[idx] = true
				indicesToCheck = append(indicesToCheck, idx)
			}
		}
	}

	// Validate each selected entry
	for _, entryIdx := range indicesToCheck {
		err := s.validateAggregateTarEntry(ctx, s3Client, uploadDetails, index, uint64(entryIdx))
		if err != nil {
			return fmt.Errorf("validation failed for entry %d: %w", entryIdx, err)
		}
	}

	return nil
}

// validateAggregateTarEntry validates a single tar entry in the aggregate
func (s *UploadDatarangeServer) validateAggregateTarEntry(ctx context.Context, s3Client *s3.Client, uploadDetails postgresstore.GetAggregateUploadWithDetailsRow, index *tarindex.Index, entryIdx uint64) error {
	// Get file metadata from index
	metadata, err := index.GetFileMetadata(entryIdx)
	if err != nil {
		return fmt.Errorf("failed to get file metadata: %w", err)
	}

	// Calculate the range to download (header + some content to read the full header)
	headerSize := int64(512)
	rangeStart := metadata.Start
	rangeEnd := metadata.Start + headerSize - 1

	// Download the tar header portion
	headerResp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(uploadDetails.Bucket),
		Key:    aws.String(uploadDetails.DataObjectKey),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)),
	})
	if err != nil {
		return fmt.Errorf("failed to download tar header: %w", err)
	}
	defer headerResp.Body.Close()

	headerData, err := io.ReadAll(headerResp.Body)
	if err != nil {
		return fmt.Errorf("failed to read header data: %w", err)
	}

	if len(headerData) != int(headerSize) {
		return fmt.Errorf("incomplete header data: expected %d bytes, got %d", headerSize, len(headerData))
	}

	// Parse the tar header
	reader := tar.NewReader(bytes.NewReader(headerData))
	header, err := reader.Next()
	if err != nil {
		return fmt.Errorf("failed to parse tar header: %w", err)
	}

	// Validate file size matches
	if header.Size != metadata.Size {
		return fmt.Errorf("file size mismatch: header says %d, index says %d", header.Size, metadata.Size)
	}

	// Validate file name format and datapoint key
	fileName := header.Name
	if !s.isValidFileName(fileName) {
		return fmt.Errorf("invalid file name format: %s", fileName)
	}

	// Extract the numeric part and validate it matches expected datapoint key
	expectedDatapointKey := uploadDetails.FirstDatapointIndex + int64(entryIdx)
	actualDatapointKey, err := s.extractDatapointKeyFromFileName(fileName)
	if err != nil {
		return fmt.Errorf("failed to extract datapoint key from filename %s: %w", fileName, err)
	}

	if actualDatapointKey != expectedDatapointKey {
		return fmt.Errorf("datapoint key mismatch: filename has %d, expected %d", actualDatapointKey, expectedDatapointKey)
	}

	return nil
}
