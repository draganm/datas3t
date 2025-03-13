# CursorRules for AWS SDK v2 

This document explains the CursorRules designed to prevent generating deprecated code patterns when working with AWS SDK v2, specifically for S3 client configuration.

## Overview

The AWS SDK for Go v2 has specific patterns for configuring S3 clients with custom endpoints. Using incorrect patterns can lead to compilation errors and runtime issues. The `.cursorrules.json` file in this repository contains rules to guide Cursor in generating correct code patterns.

## Rules for S3 Client Configuration

### Patterns to Avoid

1. **Using `EndpointOptions` directly**
   ```go
   s3Options := s3.Options{
       Region: s3Config.Region,
       EndpointOptions: s3.EndpointOptions{
           URL: s3Config.Endpoint,
       },
   }
   ```
   This pattern doesn't work because `EndpointOptions` was removed from AWS SDK v2.

2. **Using `s3.NewFromConfig` without endpoint configuration**
   ```go
   s3Client = s3.NewFromConfig(cfg)
   ```
   When using custom endpoints, always provide endpoint configuration.

3. **Using `EndpointResolverWithOptions`**
   ```go
   EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(...)
   ```
   This pattern causes type conversion errors in AWS SDK v2.

4. **Using `aws.EndpointResolverWithOptionsFunc`**
   ```go
   aws.EndpointResolverWithOptionsFunc(func(service, region string, options interface{}) ...)
   ```
   This also causes type conversion errors with S3 client.

### Recommended Pattern

Always use the following pattern for configuring AWS SDK v2 S3 client with custom endpoints:

```go
// 1. Create the AWS config with credentials and region
cfg := aws.Config{
    Region: s3Config.Region,
    Credentials: credentials.NewStaticCredentialsProvider(
        s3Config.AccessKeyID,
        s3Config.SecretAccessKey,
        "",
    ),
}

// 2. Create the S3 client with functional options
s3Client = s3.NewFromConfig(cfg, func(o *s3.Options) {
    // Enable path-style addressing (bucket.s3.amazonaws.com vs. s3.amazonaws.com/bucket)
    o.UsePathStyle = true
    
    // Set custom endpoint resolver
    o.EndpointResolver = s3.EndpointResolverFunc(
        func(region string, options s3.EndpointResolverOptions) (aws.Endpoint, error) {
            return aws.Endpoint{
                URL: s3Config.Endpoint,
            }, nil
        }
    )
})
```

## Usage

1. Keep the `.cursorrules.json` file in your repository root.
2. Cursor will use these rules when generating code related to AWS SDK v2 S3 client.
3. When a deprecated pattern is about to be generated, Cursor will suggest the recommended pattern instead.

## References

- [AWS SDK for Go V2 Developer Guide](https://aws.github.io/aws-sdk-go-v2/docs/configuring-sdk/)
- [S3 Client Configuration](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/s3) 