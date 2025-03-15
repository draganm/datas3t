# Datas3t

A Go service for managing datasets with S3 storage integration.

## Features

- Create and manage datasets
- Store dataset data in S3-compatible storage
- REST API for dataset operations
- Integration with MinIO for development and testing

## Data Dictionary

For detailed information about the key terms and concepts used in this project, see [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

## Development

### Prerequisites

- Go 1.23.6 or higher
- Access to an S3-compatible storage service (e.g., MinIO, AWS S3)

### Setup

1. Clone the repository
2. Install dependencies: `go mod download`
3. Configure environment variables for S3 access

### Running the server

```bash
go run cmd/server/server_main.go --db-url <sqlite-db-url> --addr <listen-address> --s3-endpoint <s3-endpoint> --s3-region <region> --s3-access-key-id <access-key> --s3-secret-key <secret-key> --s3-bucket-name <bucket> --s3-use-ssl <true/false> --uploads-path <path>
```

## Testing

```bash
go test ./...
```
