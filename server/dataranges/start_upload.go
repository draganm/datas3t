package dataranges

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/datas3t/postgresstore"
)

type UploadDatarangeRequest struct {
	Datas3tName         string `json:"datas3t_name"`
	DataSize            uint64 `json:"data_size"`
	NumberOfDatapoints  uint64 `json:"number_of_datapoints"`
	FirstDatapointIndex uint64 `json:"first_datapoint_index"`
}

type UploadDatarangeResponse struct {
	DatarangeID         int64  `json:"datarange_id"` // Upload record ID (for completion) - actual datarange created on success
	ObjectKey           string `json:"object_key"`
	FirstDatapointIndex uint64 `json:"first_datapoint_index"`

	// Upload type indicator
	UseDirectPut bool `json:"use_direct_put"`

	// For multipart upload (DataSize >= 5MB)
	PresignedMultipartUploadPutURLs []string `json:"presigned_multipart_upload_urls,omitempty"`

	// For direct PUT (DataSize < 5MB)
	PresignedDataPutURL string `json:"presigned_data_put_url,omitempty"`

	// Common fields
	PresignedIndexPutURL string `json:"presigned_index_put_url"`
}

type ValidationError error

const (
	// 5MB minimum part size for S3 multipart upload
	MinPartSize = 5 * 1024 * 1024
	// 100MB maximum part size to keep reasonable number of parts
	MaxPartSize = 100 * 1024 * 1024
	// Maximum number of parts allowed by S3
	MaxParts = 10000
)

func (r *UploadDatarangeRequest) Validate(ctx context.Context) error {
	if r.Datas3tName == "" {
		return ValidationError(fmt.Errorf("datas3t_name is required"))
	}

	if r.DataSize == 0 {
		return ValidationError(fmt.Errorf("data_size must be greater than 0"))
	}

	if r.NumberOfDatapoints == 0 {
		return ValidationError(fmt.Errorf("number_of_datapoints must be greater than 0"))
	}

	return nil
}

var ErrDatarangeOverlap = fmt.Errorf("datarange overlaps with existing dataranges")

func (s *UploadDatarangeServer) StartDatarangeUpload(ctx context.Context, log *slog.Logger, req *UploadDatarangeRequest) (_ *UploadDatarangeResponse, err error) {
	log = log.With(
		"datas3t_name", req.Datas3tName,
		"data_size", req.DataSize,
		"number_of_datapoints", req.NumberOfDatapoints,
		"first_datapoint_index", req.FirstDatapointIndex,
	)
	log.Info("Starting datarange upload")

	defer func() {
		if err != nil {
			log.Error("Failed to start datarange upload", "error", err)
		} else {
			log.Info("Datarange upload started successfully")
		}
	}()

	err = req.Validate(ctx)
	if err != nil {
		return nil, err
	}

	var s3Client *s3.Client

	// Check for overlapping dataranges without starting a transaction first

	noTxQueries := postgresstore.New(s.db)
	// Get datas3t with bucket information
	datas3t, err := noTxQueries.GetDatas3tWithBucket(ctx, req.Datas3tName)
	if err != nil {
		return nil, fmt.Errorf("failed to find datas3t '%s': %w", req.Datas3tName, err)
	}

	{
		// Calculate datapoint range
		firstDatapointIndex := int64(req.FirstDatapointIndex)
		lastDatapointIndex := firstDatapointIndex + int64(req.NumberOfDatapoints) - 1

		// Check for overlapping dataranges (completed uploads)
		hasDatarangeOverlap, err := noTxQueries.CheckDatarangeOverlap(ctx, postgresstore.CheckDatarangeOverlapParams{
			Datas3tID:       datas3t.ID,
			MinDatapointKey: lastDatapointIndex + 1, // Check if existing max >= our min
			MaxDatapointKey: firstDatapointIndex,    // Check if existing min < our max
		})
		if err != nil {
			return nil, fmt.Errorf("failed to check datarange overlap: %w", err)
		}

		if hasDatarangeOverlap {
			return nil, fmt.Errorf("%w: datarange overlaps with existing dataranges", ErrDatarangeOverlap)
		}

		// Allow overlapping uploads - they will be disambiguated at completion time
		// Only the first one to complete will succeed

		// Create S3 client
		s3Client, err = s.createS3Client(ctx, datas3t)
		if err != nil {
			return nil, fmt.Errorf("failed to create S3 client: %w", err)
		}

	}

	// Start a transaction for atomic operations
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

	// Generate object key for the data using upload counter
	objectKey := fmt.Sprintf(
		"datas3t/%s/dataranges/%012d-%020d-%020d.tar",
		req.Datas3tName,
		uploadCounter,
		req.FirstDatapointIndex,
		req.FirstDatapointIndex+req.NumberOfDatapoints-1,
	)

	// Generate presigned URL for index using upload counter
	indexObjectKey := fmt.Sprintf(
		"datas3t/%s/dataranges/%012d-%020d-%020d.index.zst",
		req.Datas3tName,
		uploadCounter,
		req.FirstDatapointIndex,
		req.FirstDatapointIndex+req.NumberOfDatapoints-1,
	)

	// Determine upload method based on data size
	useDirectPut := req.DataSize < MinPartSize
	var uploadID string
	var presignedPutURLs []string
	var presignedDataPutURL string

	if useDirectPut {
		// For small objects, use direct PUT
		uploadID = "DIRECT_PUT"
		presignedDataPutURL, err = s.generatePresignedPutURL(ctx, s3Client, datas3t.Bucket, objectKey)
		if err != nil {
			return nil, fmt.Errorf("failed to generate data upload URL: %w", err)
		}
	} else {
		// For large objects, use multipart upload
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

		// Calculate number of parts for multipart upload
		partSize := s.calculatePartSize(req.DataSize)
		numParts := s.calculateNumberOfParts(req.DataSize, partSize)

		// Generate presigned URLs for multipart upload parts
		presignedPutURLs, err = s.generateMultipartUploadURLs(ctx, s3Client, datas3t.Bucket, objectKey, uploadID, numParts)
		if err != nil {
			return nil, fmt.Errorf("failed to generate multipart upload URLs: %w", err)
		}
	}
	presignedIndexURL, err := s.generatePresignedPutURL(ctx, s3Client, datas3t.Bucket, indexObjectKey)
	if err != nil {
		return nil, fmt.Errorf("failed to generate index upload URL: %w", err)
	}

	// Calculate datapoint range for upload record creation
	firstDatapointIndex := int64(req.FirstDatapointIndex)
	lastDatapointIndex := firstDatapointIndex + int64(req.NumberOfDatapoints) - 1

	// Check for overlapping dataranges (completed uploads) within transaction
	hasDatarangeOverlap, err := queries.CheckDatarangeOverlap(ctx, postgresstore.CheckDatarangeOverlapParams{
		Datas3tID:       datas3t.ID,
		MinDatapointKey: lastDatapointIndex + 1, // Check if existing max >= our min
		MaxDatapointKey: firstDatapointIndex,    // Check if existing min < our max
	})
	if err != nil {
		return nil, fmt.Errorf("failed to check datarange overlap: %w", err)
	}

	if hasDatarangeOverlap {
		return nil, fmt.Errorf("%w: datarange overlaps with existing dataranges", ErrDatarangeOverlap)
	}

	// Create datarange upload record
	uploadRecordID, err := queries.CreateDatarangeUpload(ctx, postgresstore.CreateDatarangeUploadParams{
		Datas3tID:           datas3t.ID,
		UploadID:            uploadID,
		DataObjectKey:       objectKey,
		IndexObjectKey:      indexObjectKey,
		FirstDatapointIndex: firstDatapointIndex,
		NumberOfDatapoints:  int64(req.NumberOfDatapoints),
		DataSize:            int64(req.DataSize),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create datarange upload: %w", err)
	}

	// Commit transaction
	err = tx.Commit(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return &UploadDatarangeResponse{
		DatarangeID:                     uploadRecordID, // Return upload record ID for completion
		ObjectKey:                       objectKey,
		FirstDatapointIndex:             req.FirstDatapointIndex,
		UseDirectPut:                    useDirectPut,
		PresignedMultipartUploadPutURLs: presignedPutURLs,
		PresignedDataPutURL:             presignedDataPutURL,
		PresignedIndexPutURL:            presignedIndexURL,
	}, nil
}

func (s *UploadDatarangeServer) calculatePartSize(dataSize uint64) uint64 {
	// Calculate optimal part size
	// Start with a reasonable default and adjust based on data size
	partSize := uint64(MinPartSize)

	// If data is large, increase part size to keep number of parts reasonable
	for dataSize/partSize > MaxParts && partSize < MaxPartSize {
		partSize *= 2
	}

	return partSize
}

func (s *UploadDatarangeServer) calculateNumberOfParts(dataSize, partSize uint64) int {
	numParts := int(dataSize / partSize)
	if dataSize%partSize != 0 {
		numParts++
	}
	return numParts
}

func (s *UploadDatarangeServer) createS3Client(ctx context.Context, datas3t postgresstore.GetDatas3tWithBucketRow) (*s3.Client, error) {
	// Decrypt credentials
	accessKey, secretKey, err := s.encryptor.DecryptCredentials(datas3t.AccessKey, datas3t.SecretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt credentials: %w", err)
	}

	// Normalize endpoint to ensure it has proper protocol scheme
	endpoint := datas3t.Endpoint
	if !regexp.MustCompile(`^https?://`).MatchString(endpoint) {
		// If no scheme provided, default to http (non-TLS)
		endpoint = "http://" + endpoint
	}

	// Create AWS config with custom credentials and timeouts
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			accessKey,
			secretKey,
			"", // token
		)),
		config.WithRegion("us-east-1"), // default region
		config.WithHTTPClient(&http.Client{
			Timeout: 30 * time.Second, // 30 second timeout for all HTTP operations
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // Use path-style addressing for custom S3 endpoints
	})

	return s3Client, nil
}

func (s *UploadDatarangeServer) generateMultipartUploadURLs(ctx context.Context, s3Client *s3.Client, bucket, objectKey, uploadID string, numParts int) ([]string, error) {
	presigner := s3.NewPresignClient(s3Client)
	urls := make([]string, numParts)

	for i := 0; i < numParts; i++ {
		partNumber := int32(i + 1)

		req, err := presigner.PresignUploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(objectKey),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(partNumber),
		}, func(opts *s3.PresignOptions) {
			opts.Expires = 24 * time.Hour // URL expires in 24 hours
		})
		if err != nil {
			return nil, fmt.Errorf("failed to presign part %d: %w", partNumber, err)
		}

		urls[i] = req.URL
	}

	return urls, nil
}

func (s *UploadDatarangeServer) generatePresignedPutURL(ctx context.Context, s3Client *s3.Client, bucket, objectKey string) (string, error) {
	presigner := s3.NewPresignClient(s3Client)

	req, err := presigner.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(objectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour // URL expires in 24 hours
	})
	if err != nil {
		return "", fmt.Errorf("failed to presign put object: %w", err)
	}

	return req.URL, nil
}
