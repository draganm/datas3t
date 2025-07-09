package download

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsutil "github.com/draganm/datas3t/aws"
	"github.com/draganm/datas3t/postgresstore"
	"github.com/draganm/datas3t/tarindex"
)

type PreSignDownloadForDatapointsRequest struct {
	Datas3tName    string `json:"datas3t_name"`
	FirstDatapoint uint64 `json:"first_datapoint"`
	LastDatapoint  uint64 `json:"last_datapoint"`
}

type DownloadSegment struct {
	PresignedURL string `json:"presigned_url"`
	Range        string `json:"range"`
}

type PreSignDownloadForDatapointsResponse struct {
	DownloadSegments []DownloadSegment `json:"download_segments"`
}

func (s *DownloadServer) PreSignDownloadForDatapoints(ctx context.Context, log *slog.Logger, request PreSignDownloadForDatapointsRequest) (PreSignDownloadForDatapointsResponse, error) {
	// 1. Validate request
	err := request.Validate()
	if err != nil {
		return PreSignDownloadForDatapointsResponse{}, err
	}

	// 2. Get the dataranges from the database that contain the datapoints
	queries := postgresstore.New(s.pgxPool)
	dataranges, err := queries.GetDatarangesForDatapoints(ctx, postgresstore.GetDatarangesForDatapointsParams{
		Name:            request.Datas3tName,
		MinDatapointKey: int64(request.LastDatapoint),  // datarange starts before or at our last datapoint
		MaxDatapointKey: int64(request.FirstDatapoint), // datarange ends after or at our first datapoint
	})
	if err != nil {
		return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("failed to get dataranges: %w", err)
	}

	if len(dataranges) == 0 {
		// Check if the datas3t exists to provide a better error message
		allDatas3ts, err := queries.AllDatas3ts(ctx)
		if err != nil {
			return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("failed to check if datas3t exists: %w", err)
		}
		
		// Check if the datas3t exists
		datas3tExists := false
		for _, name := range allDatas3ts {
			if name == request.Datas3tName {
				datas3tExists = true
				break
			}
		}
		
		if !datas3tExists {
			return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("no dataranges found for datapoints %d-%d in datas3t %s", request.FirstDatapoint, request.LastDatapoint, request.Datas3tName)
		}
		
		// Datas3t exists but no dataranges found - check if datas3t has ANY dataranges
		allDataranges, err := queries.GetDatarangesForDatas3t(ctx, request.Datas3tName)
		if err != nil {
			return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("failed to check datas3t dataranges: %w", err)
		}
		
		if len(allDataranges) == 0 {
			// Datas3t exists but is completely empty - return empty response for graceful handling
			return PreSignDownloadForDatapointsResponse{
				DownloadSegments: []DownloadSegment{},
			}, nil
		}
		
		// Datas3t has dataranges but none overlap with the requested range
		return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("no dataranges found for datapoints %d-%d in datas3t %s", request.FirstDatapoint, request.LastDatapoint, request.Datas3tName)
	}

	var downloadSegments []DownloadSegment

	// 3. For each datarange, get the index from the disk cache and create download segments
	for _, datarange := range dataranges {
		// Create S3 client for this datarange
		s3Client, err := s.createS3Client(ctx, log, datarange)
		if err != nil {
			return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("failed to create S3 client: %w", err)
		}

		// Create disk cache key by concatenating datas3t name and index object key
		cacheKey := datarange.Datas3tName + datarange.IndexObjectKey

		// Get the tar index from disk cache
		err = s.diskCache.OnIndex(cacheKey, func(index *tarindex.Index) error {
			// Create download segments for the files we need
			segments, err := s.createDownloadSegments(ctx, s3Client, datarange, index, request.FirstDatapoint, request.LastDatapoint)
			if err != nil {
				return fmt.Errorf("failed to create download segments: %w", err)
			}
			downloadSegments = append(downloadSegments, segments...)
			return nil
		}, func() ([]byte, error) {
			// Index generator: download the index from S3 if not cached
			return s.downloadIndexFromS3(ctx, s3Client, datarange.Bucket, datarange.IndexObjectKey)
		})

		if err != nil {
			return PreSignDownloadForDatapointsResponse{}, fmt.Errorf("failed to get index for datarange %d: %w", datarange.ID, err)
		}
	}

	return PreSignDownloadForDatapointsResponse{
		DownloadSegments: downloadSegments,
	}, nil
}

func (r *PreSignDownloadForDatapointsRequest) Validate() error {
	if r.Datas3tName == "" {
		return fmt.Errorf("datas3t_name is required")
	}

	if r.FirstDatapoint > r.LastDatapoint {
		return fmt.Errorf("first_datapoint (%d) cannot be greater than last_datapoint (%d)", r.FirstDatapoint, r.LastDatapoint)
	}

	return nil
}

func (s *DownloadServer) createS3Client(ctx context.Context, log *slog.Logger, datarange postgresstore.GetDatarangesForDatapointsRow) (*s3.Client, error) {
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
		// Note: Could add a logger here if needed for debugging download operations
	})
}

func (s *DownloadServer) downloadIndexFromS3(ctx context.Context, s3Client *s3.Client, bucket, indexObjectKey string) ([]byte, error) {
	resp, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(indexObjectKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get index object from S3: %w", err)
	}
	defer resp.Body.Close()

	// Read the entire index file using io.ReadAll
	indexData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read index data: %w", err)
	}

	return indexData, nil
}

func (s *DownloadServer) createDownloadSegments(ctx context.Context, s3Client *s3.Client, datarange postgresstore.GetDatarangesForDatapointsRow, index *tarindex.Index, firstDatapoint, lastDatapoint uint64) ([]DownloadSegment, error) {
	var segments []DownloadSegment

	// Calculate the range of files we need to download
	datarangeFirst := uint64(datarange.MinDatapointKey)
	datarangeLast := uint64(datarange.MaxDatapointKey)

	// Determine the actual range we need from this datarange
	actualFirst := max(firstDatapoint, datarangeFirst)
	actualLast := min(lastDatapoint, datarangeLast)

	if actualFirst > actualLast {
		// No overlap with this datarange
		return segments, nil
	}

	// Calculate file indices within the datarange
	firstFileIndex := actualFirst - datarangeFirst
	lastFileIndex := actualLast - datarangeFirst

	if lastFileIndex >= index.NumFiles() {
		return nil, fmt.Errorf("file index %d exceeds number of files in index (%d)", lastFileIndex, index.NumFiles())
	}

	// Get metadata for the first and last files
	firstFileMetadata, err := index.GetFileMetadata(firstFileIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for first file (index %d): %w", firstFileIndex, err)
	}

	lastFileMetadata, err := index.GetFileMetadata(lastFileIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to get metadata for last file (index %d): %w", lastFileIndex, err)
	}

	// Calculate the byte range we need
	startByte := firstFileMetadata.Start

	// Calculate end byte including proper TAR padding for the last file
	// 1. Start of last file
	// 2. + Header size (HeaderBlocks * 512)
	// 3. + File content padded to 512-byte boundary
	lastFileHeaderSize := int64(lastFileMetadata.HeaderBlocks) * 512
	lastFileContentPaddedSize := ((lastFileMetadata.Size + 511) / 512) * 512 // Round up to 512-byte boundary

	endByte := lastFileMetadata.Start + lastFileHeaderSize + lastFileContentPaddedSize - 1

	// Create presigned URL for the data object with byte range
	presigner := s3.NewPresignClient(s3Client)
	req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(datarange.Bucket),
		Key:    aws.String(datarange.DataObjectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = 24 * time.Hour // URL expires in 24 hours
	})
	if err != nil {
		return nil, fmt.Errorf("failed to presign get object: %w", err)
	}

	segments = append(segments, DownloadSegment{
		PresignedURL: req.URL,
		Range:        fmt.Sprintf("bytes=%d-%d", startByte, endByte),
	})

	return segments, nil
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
