package bucket

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	awsutil "github.com/draganm/datas3t/aws"
)

type BucketInfo struct {
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	Bucket    string `json:"bucket"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

// BucketListInfo represents bucket information for listing (without sensitive credentials)
type BucketListInfo struct {
	Name     string `json:"name"`
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`
}

var bucketNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

type ValidationError error

// IsEndpointTLS determines if an endpoint uses TLS based on its protocol
func IsEndpointTLS(endpoint string) bool {
	return strings.HasPrefix(endpoint, "https://")
}

func (r *BucketInfo) Validate(ctx context.Context, log *slog.Logger) error {
	if !bucketNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("invalid bucket name: %s", r.Name))
	}

	if r.Endpoint == "" {
		return ValidationError(fmt.Errorf("endpoint is required"))
	}

	if r.Bucket == "" {
		return ValidationError(fmt.Errorf("bucket is required"))
	}

	err := r.testConnection(ctx, log)
	if err != nil {
		return ValidationError(fmt.Errorf("failed to test connection: %w", err))
	}

	return nil
}

func (r *BucketInfo) testConnection(ctx context.Context, log *slog.Logger) error {
	// Create S3 client using shared utility
	s3Client, err := awsutil.CreateS3Client(ctx, awsutil.S3ClientConfig{
		AccessKey: r.AccessKey,
		SecretKey: r.SecretKey,
		Endpoint:  r.Endpoint,
		Logger:    log,
		// Note: No logger provided for validation to keep it silent
	})
	if err != nil {
		return fmt.Errorf("failed to create S3 client: %w", err)
	}

	// Test connection by listing objects (with max 1 object to minimize data transfer)
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(r.Bucket),
		MaxKeys: aws.Int32(1),
	}

	_, err = s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to connect to S3 bucket %s at %s: %w", r.Bucket, r.Endpoint, err)
	}

	return nil
}
