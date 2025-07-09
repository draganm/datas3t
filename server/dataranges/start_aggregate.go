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

type StartAggregateRequest struct {
	Datas3tName         string `json:"datas3t_name"`
	FirstDatapointIndex uint64 `json:"first_datapoint_index"`
	LastDatapointIndex  uint64 `json:"last_datapoint_index"`
}

type StartAggregateResponse struct {
	AggregateUploadID int64  `json:"aggregate_upload_id"`
	ObjectKey         string `json:"object_key"`

	// Download URLs for source dataranges
	SourceDatarangeDownloadURLs []DatarangeDownloadURL `json:"source_datarange_download_urls"`

	// Upload URLs for the new aggregate
	UseDirectPut                    bool     `json:"use_direct_put"`
	PresignedMultipartUploadPutURLs []string `json:"presigned_multipart_upload_urls,omitempty"`
	PresignedDataPutURL             string   `json:"presigned_data_put_url,omitempty"`
	PresignedIndexPutURL            string   `json:"presigned_index_put_url"`
}

type DatarangeDownloadURL struct {
	DatarangeID       int64  `json:"datarange_id"`
	DataObjectKey     string `json:"data_object_key"`
	IndexObjectKey    string `json:"index_object_key"`
	MinDatapointKey   int64  `json:"min_datapoint_key"`
	MaxDatapointKey   int64  `json:"max_datapoint_key"`
	SizeBytes         int64  `json:"size_bytes"`
	PresignedDataURL  string `json:"presigned_data_url"`
	PresignedIndexURL string `json:"presigned_index_url"`
}

var ErrInsufficientDataranges = fmt.Errorf("range must contain at least two dataranges")
var ErrRangeNotFullyCovered = fmt.Errorf("range is not fully covered by existing dataranges")

func (r *StartAggregateRequest) Validate(ctx context.Context) error {
	if r.Datas3tName == "" {
		return ValidationError(fmt.Errorf("datas3t_name is required"))
	}

	if r.FirstDatapointIndex > r.LastDatapointIndex {
		return ValidationError(fmt.Errorf("first_datapoint_index must be <= last_datapoint_index"))
	}

	return nil
}

func (s *UploadDatarangeServer) StartAggregate(ctx context.Context, log *slog.Logger, req *StartAggregateRequest) (_ *StartAggregateResponse, err error) {
	log = log.With(
		"datas3t_name", req.Datas3tName,
		"first_datapoint_index", req.FirstDatapointIndex,
		"last_datapoint_index", req.LastDatapointIndex,
	)
	log.Info("Starting aggregate operation")

	defer func() {
		if err != nil {
			log.Error("Failed to start aggregate operation", "error", err)
		} else {
			log.Info("Aggregate operation started successfully")
		}
	}()

	err = req.Validate(ctx)
	if err != nil {
		return nil, err
	}

	// Get datas3t with bucket information
	noTxQueries := postgresstore.New(s.db)
	datas3t, err := noTxQueries.GetDatas3tWithBucket(ctx, req.Datas3tName)
	if err != nil {
		return nil, fmt.Errorf("failed to find datas3t '%s': %w", req.Datas3tName, err)
	}

	// Check if the range is fully covered by existing dataranges
	isCovered, err := noTxQueries.CheckFullDatarangeCoverage(ctx, postgresstore.CheckFullDatarangeCoverageParams{
		Name:            req.Datas3tName,
		MinDatapointKey: int64(req.FirstDatapointIndex),
		MaxDatapointKey: int64(req.LastDatapointIndex),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to check datarange coverage: %w", err)
	}

	if !isCovered {
		return nil, fmt.Errorf("%w: range %d-%d is not fully covered by at least two existing dataranges",
			ErrRangeNotFullyCovered, req.FirstDatapointIndex, req.LastDatapointIndex)
	}

	// Get all dataranges in the requested range
	sourceDataranges, err := noTxQueries.GetDatarangesInRange(ctx, postgresstore.GetDatarangesInRangeParams{
		Name:            req.Datas3tName,
		MinDatapointKey: int64(req.FirstDatapointIndex),
		MaxDatapointKey: int64(req.LastDatapointIndex),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get dataranges in range: %w", err)
	}

	if len(sourceDataranges) < 2 {
		return nil, fmt.Errorf("%w: found %d dataranges, need at least 2", ErrInsufficientDataranges, len(sourceDataranges))
	}

	// Create S3 client
	s3Client, err := s.createS3Client(ctx, log, datas3t)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Start transaction for atomic operations
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	queries := postgresstore.New(tx)

	// Increment upload counter and get the new value
	uploadCounter, err := queries.IncrementUploadCounter(ctx, datas3t.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to increment upload counter: %w", err)
	}

	// Generate object keys for the aggregate
	objectKey := fmt.Sprintf(
		"datas3t/%s/dataranges/%020d-%020d-%012d.tar",
		req.Datas3tName,
		req.FirstDatapointIndex,
		req.LastDatapointIndex,
		uploadCounter,
	)

	indexObjectKey := fmt.Sprintf(
		"datas3t/%s/dataranges/%020d-%020d-%012d.index",
		req.Datas3tName,
		req.FirstDatapointIndex,
		req.LastDatapointIndex,
		uploadCounter,
	)

	// Calculate estimated total data size for upload method decision
	// Note: This is just an estimate for choosing direct PUT vs multipart
	// The actual size will be determined by the uploaded aggregated content
	var estimatedDataSize int64
	for _, dr := range sourceDataranges {
		estimatedDataSize += dr.SizeBytes
	}
	
	// Account for TAR trailer removal during aggregation
	// Each TAR file has a 1KB trailer (two 512-byte zero blocks)
	// When aggregating, all but the last trailer are removed
	const TarTrailerSize = 1024 // 2 * 512 bytes
	if len(sourceDataranges) > 1 {
		trailersRemoved := len(sourceDataranges) - 1
		estimatedDataSize -= int64(trailersRemoved * TarTrailerSize)
		
		// Ensure we don't go negative (shouldn't happen in practice)
		if estimatedDataSize < 0 {
			estimatedDataSize = 0
		}
	}

	// Determine upload method based on estimated data size
	useDirectPut := uint64(estimatedDataSize) < MinPartSize
	var uploadID string
	var presignedPutURLs []string
	var presignedDataPutURL string

	if useDirectPut {
		// For small aggregates, use direct PUT
		uploadID = "DIRECT_PUT"
		presignedDataPutURL, err = s.generatePresignedPutURL(ctx, s3Client, datas3t.Bucket, objectKey)
		if err != nil {
			return nil, fmt.Errorf("failed to generate data upload URL: %w", err)
		}
	} else {
		// For large aggregates, use multipart upload
		createResp, err := s3Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(datas3t.Bucket),
			Key:    aws.String(objectKey),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create multipart upload: %w", err)
		}

		uploadID = *createResp.UploadId

		defer func() {
			if err != nil {
				s3Client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(datas3t.Bucket),
					Key:      aws.String(objectKey),
					UploadId: aws.String(uploadID),
				})
			}
		}()

		// Calculate number of parts for multipart upload based on estimate
		partSize := s.calculatePartSize(uint64(estimatedDataSize))
		numParts := s.calculateNumberOfParts(uint64(estimatedDataSize), partSize)

		// Generate presigned URLs for multipart upload parts
		presignedPutURLs, err = s.generateMultipartUploadURLs(ctx, s3Client, datas3t.Bucket, objectKey, uploadID, numParts)
		if err != nil {
			return nil, fmt.Errorf("failed to generate multipart upload URLs: %w", err)
		}
	}

	// Generate presigned URL for index upload
	presignedIndexURL, err := s.generatePresignedPutURL(ctx, s3Client, datas3t.Bucket, indexObjectKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate index upload URL: %w", err)
	}

	// Generate presigned download URLs for source dataranges
	var sourceDownloadURLs []DatarangeDownloadURL
	for _, dr := range sourceDataranges {
		dataDownloadURL, err := s.generatePresignedGetURL(ctx, s3Client, dr.Bucket, dr.DataObjectKey)
		if err != nil {
			return nil, fmt.Errorf("failed to generate data download URL for datarange %d: %w", dr.ID, err)
		}

		indexDownloadURL, err := s.generatePresignedGetURL(ctx, s3Client, dr.Bucket, dr.IndexObjectKey)
		if err != nil {
			return nil, fmt.Errorf("failed to generate index download URL for datarange %d: %w", dr.ID, err)
		}

		sourceDownloadURLs = append(sourceDownloadURLs, DatarangeDownloadURL{
			DatarangeID:       dr.ID,
			DataObjectKey:     dr.DataObjectKey,
			IndexObjectKey:    dr.IndexObjectKey,
			MinDatapointKey:   dr.MinDatapointKey,
			MaxDatapointKey:   dr.MaxDatapointKey,
			SizeBytes:         dr.SizeBytes,
			PresignedDataURL:  dataDownloadURL,
			PresignedIndexURL: indexDownloadURL,
		})
	}

	// Extract source datarange IDs
	var sourceDatarangeIDs []int64
	for _, dr := range sourceDataranges {
		sourceDatarangeIDs = append(sourceDatarangeIDs, dr.ID)
	}

	// Create aggregate upload record
	// Note: TotalDataSize will be updated when upload completes with actual size
	aggregateUploadID, err := queries.CreateAggregateUpload(ctx, postgresstore.CreateAggregateUploadParams{
		Datas3tID:           datas3t.ID,
		UploadID:            uploadID,
		DataObjectKey:       objectKey,
		IndexObjectKey:      indexObjectKey,
		FirstDatapointIndex: int64(req.FirstDatapointIndex),
		LastDatapointIndex:  int64(req.LastDatapointIndex),
		TotalDataSize:       0, // Will be set to actual size upon completion
		SourceDatarangeIds:  sourceDatarangeIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregate upload record: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &StartAggregateResponse{
		AggregateUploadID:               aggregateUploadID,
		ObjectKey:                       objectKey,
		SourceDatarangeDownloadURLs:     sourceDownloadURLs,
		UseDirectPut:                    useDirectPut,
		PresignedMultipartUploadPutURLs: presignedPutURLs,
		PresignedDataPutURL:             presignedDataPutURL,
		PresignedIndexPutURL:            presignedIndexURL,
	}, nil
}

func (s *UploadDatarangeServer) generatePresignedGetURL(ctx context.Context, s3Client *s3.Client, bucket, objectKey string) (string, error) {
	presigner := s3.NewPresignClient(s3Client)

	req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour // URL expires in 24 hours
	})
	if err != nil {
		return "", fmt.Errorf("failed to presign get object: %w", err)
	}

	return req.URL, nil
}
