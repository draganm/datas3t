# Datas3t

A Go service for managing datasets with S3 storage integration.

## Features

- Create and manage datasets
- Store dataset data in S3-compatible storage
- REST API for dataset operations including data range queries
- Integration with MinIO for development and testing
- Client library for programmatic access
- Command-line interface (CLI) for easy interaction
- Automatic database restoration from S3 metadata if database is empty
- Atomic aggregation of multiple dataranges into a single consolidated range

## Components

### Server

The server provides a REST API with the following endpoints:
- `GET /api/v1/datas3t` - List all datasets
- `PUT /api/v1/datas3t/{id}` - Create a dataset
- `GET /api/v1/datas3t/{id}` - Get dataset information
- `POST /api/v1/datas3t/{id}` - Upload data to a dataset
- `GET /api/v1/datas3t/{id}/dataranges` - Get all data ranges for a dataset
- `GET /api/v1/datas3t/{id}/datarange/{start}/{end}` - Get specific data range with start/end keys
- `POST /api/v1/datas3t/{id}/aggregate/{start}/{end}` - Aggregate multiple dataranges into a single consolidated range

### Client Library

The client library (`pkg/client`) provides a Go interface for interacting with the Datas3t server:
- List all available datasets
- Create datasets
- Retrieve dataset information
- Upload data ranges
- Get data ranges (all or specific range)
- Retrieve individual data points
- Aggregate multiple dataranges into a single consolidated range

### Command-Line Interface (CLI)

The CLI (`cmd/datas3t-cli`) provides commands for:
- Creating datasets
- Getting dataset information
- Uploading data ranges
- Listing datasets
- Querying specific data ranges
- Aggregating multiple dataranges into a single consolidated range

### Restore Package

The restore package (`pkg/restore`) provides functionality to:
- Automatically detect if the database is empty
- Discover datasets and dataranges from S3 storage
- Restore database records from S3 metadata
- Rebuild the complete database state in a single transaction

## Data Dictionary

For detailed information about the key terms and concepts used in this project, see [DATA_DICTIONARY.md](DATA_DICTIONARY.md).

## S3 Storage Layout

For information about how datasets, dataranges, and metadata are stored in S3, see [S3_LAYOUT.md](S3_LAYOUT.md).

## Development

### Prerequisites

- Go 1.23.6 or higher
- Access to an S3-compatible storage service (e.g., MinIO, AWS S3)
- SQLite database for metadata storage

### Setup

1. Clone the repository
2. Install dependencies: `go mod download`
3. Configure environment variables for S3 access
4. Ensure SQLite database path is writable

### Running the server

```bash
go run cmd/server/server_main.go --db-url <sqlite-db-url> --addr <listen-address> --s3-endpoint <s3-endpoint> --s3-region <region> --s3-access-key-id <access-key> --s3-secret-key <secret-key> --s3-bucket-name <bucket> --s3-use-ssl <true/false> --uploads-path <path>
```

## Testing

```bash
go test ./...
```
