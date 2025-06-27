package bucket

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// normalizeEndpoint ensures the endpoint has the correct protocol scheme
func normalizeEndpoint(endpoint string) string {
	if regexp.MustCompile(`^https?://`).MatchString(endpoint) {
		return endpoint
	}

	// If no scheme provided, default to http (non-TLS)
	return "http://" + endpoint
}

func (r *BucketInfo) Validate(ctx context.Context) error {
	if !bucketNameRegex.MatchString(r.Name) {
		return ValidationError(fmt.Errorf("invalid bucket name: %s", r.Name))
	}

	if r.Endpoint == "" {
		return ValidationError(fmt.Errorf("endpoint is required"))
	}

	if r.Bucket == "" {
		return ValidationError(fmt.Errorf("bucket is required"))
	}

	err := r.testConnection(ctx)
	if err != nil {
		return ValidationError(fmt.Errorf("failed to test connection: %w", err))
	}

	return nil
}

func (r *BucketInfo) testConnection(ctx context.Context) error {
	// Normalize the endpoint to ensure it has a protocol scheme
	endpoint := normalizeEndpoint(r.Endpoint)

	// Create AWS config with custom credentials and timeouts
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			r.AccessKey,
			r.SecretKey,
			"", // token
		)),
		config.WithRegion("us-east-1"), // default region, can be overridden by endpoint
		config.WithHTTPClient(&http.Client{
			Timeout: 30 * time.Second, // 30 second timeout for all HTTP operations
		}),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with custom endpoint
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true // Use path-style addressing for custom S3 endpoints
	})

	// Test connection by listing objects (with max 1 object to minimize data transfer)
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(r.Bucket),
		MaxKeys: aws.Int32(1),
	}

	_, err = s3Client.ListObjectsV2(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to connect to S3 bucket %s at %s: %w", r.Bucket, endpoint, err)
	}

	return nil
}
