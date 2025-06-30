package aws

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
	"github.com/aws/smithy-go/logging"
)

// SlogAWSAdapter wraps slog.Logger to implement smithy-go's logging.Logger interface
type SlogAWSAdapter struct {
	logger *slog.Logger
}

// NewSlogAWSAdapter creates a new SlogAWSAdapter that implements the smithy-go Logger interface
func NewSlogAWSAdapter(logger *slog.Logger) *SlogAWSAdapter {
	return &SlogAWSAdapter{
		logger: logger.With("component", "aws-sdk"),
	}
}

// Logf implements the smithy-go logging.Logger interface
func (a *SlogAWSAdapter) Logf(classification logging.Classification, format string, v ...interface{}) {
	message := fmt.Sprintf(format, v...)

	switch classification {
	case logging.Warn:
		a.logger.Warn("AWS SDK", "message", message)
	case logging.Debug:
		a.logger.Debug("AWS SDK", "message", message)
	default:
		a.logger.Info("AWS SDK", "message", message, "classification", string(classification))
	}
}

// S3ClientConfig contains configuration for creating an S3 client
type S3ClientConfig struct {
	AccessKey string
	SecretKey string
	Endpoint  string
	Region    string
	Logger    *slog.Logger
}

// CreateS3Client creates an S3 client with consistent logging integration and configuration
func CreateS3Client(ctx context.Context, cfg S3ClientConfig) (*s3.Client, error) {
	// Set default region if not provided
	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	// Normalize endpoint to ensure it has proper protocol scheme
	endpoint := cfg.Endpoint
	if endpoint != "" && !regexp.MustCompile(`^https?://`).MatchString(endpoint) {
		// If no scheme provided, default to http (non-TLS)
		endpoint = "http://" + endpoint
	}

	var configOptions []func(*config.LoadOptions) error

	// Add credentials if provided
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		configOptions = append(configOptions,
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				cfg.AccessKey,
				cfg.SecretKey,
				"", // token
			)),
		)
	}

	// Set region
	configOptions = append(configOptions, config.WithRegion(region))

	// Set HTTP client with timeout
	configOptions = append(configOptions,
		config.WithHTTPClient(&http.Client{
			Timeout: 30 * time.Second, // 30 second timeout for all HTTP operations
		}),
	)

	// Add logging if logger is provided
	if cfg.Logger != nil {
		awsLogger := NewSlogAWSAdapter(cfg.Logger)
		configOptions = append(configOptions, config.WithLogger(awsLogger))
	}

	// Create AWS config with structured logging
	awsCfg, err := config.LoadDefaultConfig(ctx, configOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client
	var s3Options []func(*s3.Options)

	// Set custom endpoint if provided
	if endpoint != "" {
		s3Options = append(s3Options, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // Use path-style addressing for custom S3 endpoints
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, s3Options...)
	return s3Client, nil
}
