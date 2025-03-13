# Datas3t

A Go service for managing datasets with S3 storage integration.

## Features

- Create and manage datasets
- Store dataset data in S3-compatible storage
- REST API for dataset operations
- Integration with MinIO for development and testing

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

## CursorRules

This project includes a `.cursorrules.json` file with rules to prevent generating deprecated code patterns when working with AWS SDK v2, specifically for S3 client configuration.

For detailed information about these rules, see [CURSOR_RULES.md](CURSOR_RULES.md).

### Key Benefits

- Prevents compilation errors related to AWS SDK v2 S3 client configuration
- Enforces best practices for working with S3-compatible storage services
- Ensures consistent code patterns across the project

## Testing

```bash
go test ./...
```

## License

[MIT License](LICENSE) 