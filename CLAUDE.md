# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Core Commands
- `nix develop` - Enter development environment with all dependencies
- `nix develop -c go test ./...` - Run all tests
- `nix develop -c go generate ./...` - Generate code (includes sqlc for database queries)
- `nix develop -c go build -o datas3t ./cmd/datas3t` - Build CLI binary
- `nix develop -c go run ./cmd/datas3t server` - Run server directly

### Testing
- `nix develop -c go test ./...` - Run all tests
- `nix develop -c go test -v ./server/...` - Run server tests with verbose output
- Tests use Ginkgo/Gomega framework and testcontainers for integration testing

### Database Operations
- `nix develop -c go generate ./postgresstore` - Generate database code with sqlc
- Database migrations are embedded in `postgresstore/migrations/`

## Architecture Overview

### Core Components

**Server Architecture (server/)**
- `server.go` - Main server composition combining all sub-servers
- `bucket/` - S3 bucket configuration management with encrypted credentials
- `datas3t/` - Dataset metadata and lifecycle management
- `dataranges/` - TAR archive upload/download operations and aggregation
- `download/` - Presigned URL generation for direct S3 access

**HTTP API (httpapi/)**
- REST API layer that wraps the server components
- Endpoints follow `/api/v1/` pattern
- Handles JSON serialization and HTTP concerns

**Client Library (client/)**
- Go SDK for programmatic access
- Mirrors server functionality with proper error handling
- Supports both sync operations and streaming/iterators

**Database Layer (postgresstore/)**
- PostgreSQL-based storage using pgx/v5
- Code generation via sqlc for type-safe queries
- Embedded migrations for schema management

**TAR Indexing (tarindex/)**
- Memory-mapped index files for fast random access
- Binary format: 16-byte entries per file (position, header blocks, size)
- Disk caching system for frequently accessed indices

### Key Concepts

**Datapoints**: Individual files numbered sequentially (e.g., `00000000000000000001.txt`)
**Dataranges**: Contiguous chunks of datapoints stored as TAR archives
**Datas3ts**: Named collections of datapoints with associated S3 bucket configuration
**Aggregation**: Server-side process to combine multiple dataranges into larger TAR files

### Data Flow

1. **Upload**: Client uploads TAR → Server validates → Stored in S3 with index
2. **Download**: Client requests range → Server generates presigned URLs → Direct S3 access
3. **Aggregation**: Server combines small dataranges into larger ones for efficiency

## Important Patterns

### Error Handling
- Use Go's standard error handling patterns
- Wrap errors with context using `fmt.Errorf`
- Server components return structured errors for HTTP API translation

### Database Operations
- All database queries are in `postgresstore/query.sql`
- Use `sqlc generate` to update generated code after query changes
- Transactions are handled at the server layer, not in store methods

### S3 Operations
- All S3 credentials are encrypted at rest using AES-256-GCM
- TLS usage determined by endpoint protocol (https:// vs http://)
- Presigned URLs used for direct client-to-S3 transfers

### Testing
- Integration tests use testcontainers for real PostgreSQL/MinIO instances
- Each server component has its own test suite
- E2E tests cover full workflow scenarios

## File Naming Conventions

- Datapoints must follow pattern: `%020d.<extension>` (20-digit zero-padded numbers)
- TAR files: `{first_datapoint}-{last_datapoint}.tar`
- Index files: `{first_datapoint}-{last_datapoint}.index.zst`

## Environment Variables

- `DB_URL` - PostgreSQL connection string
- `CACHE_DIR` - Directory for TAR index caching
- `ENCRYPTION_KEY` - Base64-encoded AES-256 key for S3 credential encryption
- `DATAS3T_SERVER_URL` - Server URL for CLI commands

## Security Notes

- S3 credentials are encrypted using AES-256-GCM with unique nonces
- Database connection uses pgx with proper prepared statements
- All file operations validate datapoint naming conventions
- TAR structure validation prevents malicious archive uploads