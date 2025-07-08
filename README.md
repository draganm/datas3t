# datas3t

A high-performance data management system for storing, indexing, and retrieving large-scale datas3ts in S3-compatible object storage.

## Overview

datas3t is designed for efficiently managing datas3ts containing millions of individual files (called "datapoints"). It stores files as indexed TAR archives in S3-compatible storage, enabling fast random access without the overhead of extracting entire archives.

## Key Features

### 🗜️ **Efficient Storage**
- Packs individual files into TAR archives
- Eliminates S3 object overhead for small files
- Supports datas3ts with millions of datapoints

### ⚡ **Fast Random Access**
- Creates lightweight indices for TAR archives
- Enables direct access to specific files without extraction
- Disk-based caching for frequently accessed indices

### 🔒 **Flexible TLS Configuration**
- TLS usage determined by endpoint protocol (https:// vs http://)
- No separate TLS flags needed - follows standard URL conventions
- Seamless integration with various S3-compatible services

### 📦 **Range-based Operations**
- Upload and download data in configurable chunks (dataranges)
- Supports partial datas3t retrieval
- Parallel processing of multiple ranges

### 🔗 **Direct Client-to-Storage Transfer**
- Uses S3 presigned URLs for efficient data transfer
- Bypasses server for large file operations
- Supports multipart uploads for large datas3ts

### 🔄 **Datarange Aggregation**
- Combines multiple small dataranges into larger ones
- Reduces S3 object count and improves download performance
- Validates continuous datapoint coverage before aggregation
- Atomic operations with automatic cleanup on failure

### 📦 **Datas3t Import**
- Discovers and imports existing datas3ts from S3 buckets
- Scans for objects matching datas3t patterns automatically
- Disaster recovery and migration support
- Maintains upload counter consistency
- Prevents duplicate imports with idempotent operations

### 🛡️ **Data Integrity**
- Validates TAR structure and file naming conventions
- Ensures datapoint consistency across operations
- Transactional database operations

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Client/CLI    │───▶│   HTTP API       │───▶│   PostgreSQL        │
│                 │    │   Server         │    │   (Metadata)        │
└─────────────────┘    └──────────────────┘    └─────────────────────┘
                                │
                                ▼
                       ┌──────────────────┐
                       │  S3-Compatible   │
                       │  Object Storage  │
                       │  (TAR Archives)  │
                       └──────────────────┘
```

### Components

- **HTTP API Server**: REST API for datas3t management
- **Client Library**: Go SDK for programmatic access  
- **PostgreSQL Database**: Stores metadata and indices
- **S3-Compatible Storage**: Stores TAR archives and indices
- **TAR Indexing Engine**: Creates fast-access indices
- **Disk Cache**: Local caching for performance

## Core Concepts

### Datas3ts
Named collections of related datapoints. Each datas3t is associated with an S3 bucket configuration.

### Datapoints
Individual files within a datas3t, numbered sequentially:
- `00000000000000000001.txt`
- `00000000000000000002.jpg`
- `00000000000000000003.json`

### Dataranges
Contiguous chunks of datapoints stored as TAR archives:
- `datas3t/my-datas3t/dataranges/00000000000000000001-00000000000000001000-000000000001.tar`
- `datas3t/my-datas3t/dataranges/00000000000000001001-00000000000000002000-000000000002.tar`

### TAR Indices
Lightweight index files enabling fast random access:
- `datas3t/my-datas3t/dataranges/00000000000000000001-00000000000000001000-000000000001.index`

### Import Operations
Process of discovering and importing existing datas3ts from S3 buckets:
- **Pattern Recognition**: Automatically detects objects matching datas3t naming conventions
- **Duplicate Prevention**: Skips existing dataranges to prevent conflicts
- **Upload Counter Management**: Maintains counter consistency for future uploads
- **Transaction Safety**: All imports are performed atomically per datas3t

### Aggregation Operations
Process of combining multiple small dataranges into larger ones for improved efficiency:
- **Coverage Validation**: Ensures continuous datapoint coverage with no gaps
- **Atomic Replacement**: Original dataranges are replaced atomically after successful aggregation
- **Parallel Processing**: Downloads and uploads are performed in parallel for optimal performance
- **Multipart Support**: Large aggregates use multipart uploads for reliability

## Quick Start

### Prerequisites

- [Nix with flakes enabled](https://nixos.wiki/wiki/Flakes) (recommended)
- Go 1.24.3+
- PostgreSQL 12+
- S3-compatible storage (AWS S3, MinIO, etc.)

### Development Setup

```bash
# Clone the repository
git clone https://github.com/draganm/datas3t.git
cd datas3t

# Enter development environment
nix develop

# Run tests
nix develop -c go test ./...

# Generate code
nix develop -c go generate ./...
```

### Running the Server

```bash
# Set environment variables
export DB_URL="postgres://user:password@localhost:5432/datas3t"
export ADDR=":8765"

# Run the server
nix develop -c go run ./cmd/datas3t server

# Server will start on http://localhost:8765
```

## API Usage

### 1. Configure S3 Bucket

```bash
curl -X POST http://localhost:8765/api/bucket \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-bucket-config",
    "endpoint": "https://s3.amazonaws.com",
    "bucket": "my-data-bucket",
    "access_key": "ACCESS_KEY",
    "secret_key": "SECRET_KEY"
  }'
```

### 2. Create Datas3t

```bash
curl -X POST http://localhost:8765/api/datas3t \
  -H "Content-Type: application/json" \
  -d '{
    "name": "my-datas3t",
    "bucket": "my-bucket-config"
  }'
```

### 3. Upload Datarange

```bash
# Start upload
curl -X POST http://localhost:8765/api/datarange/upload/start \
  -H "Content-Type: application/json" \
  -d '{
    "datas3t_name": "my-datas3t",
    "first_datapoint_index": 1,
    "number_of_datapoints": 1000,
    "data_size": 1048576
  }'

# Use returned presigned URLs to upload TAR archive and index
# Then complete the upload
curl -X POST http://localhost:8765/api/datarange/upload/complete \
  -H "Content-Type: application/json" \
  -d '{
    "datarange_upload_id": 123
  }'
```

### 4. Download Datapoints

```bash
curl -X POST http://localhost:8765/api/download/presign \
  -H "Content-Type: application/json" \
  -d '{
    "datas3t_name": "my-datas3t",
    "first_datapoint": 100,
    "last_datapoint": 200
  }'
```

### 5. Import Existing Datas3ts

```bash
# Import datas3ts from S3 bucket
curl -X POST http://localhost:8765/api/v1/datas3ts/import \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "my-bucket-config"
  }'
```

### 6. Aggregate Dataranges

```bash
# Start aggregation
curl -X POST http://localhost:8765/api/v1/aggregate \
  -H "Content-Type: application/json" \
  -d '{
    "datas3t_name": "my-datas3t",
    "first_datapoint_index": 1,
    "last_datapoint_index": 5000
  }'

# Complete aggregation (after processing returned URLs)
curl -X POST http://localhost:8765/api/v1/aggregate/complete \
  -H "Content-Type: application/json" \
  -d '{
    "aggregate_upload_id": 456
  }'
```

## Client Library Usage

```go
package main

import (
    "context"
    "github.com/draganm/datas3t/client"
)

func main() {
    // Create client
    c := client.New("http://localhost:8765")
    
    // List datas3ts
datas3ts, err := c.ListDatas3ts(context.Background())
    if err != nil {
        panic(err)
    }
    
    // Download specific datapoints
    response, err := c.PreSignDownloadForDatapoints(context.Background(), &client.PreSignDownloadForDatapointsRequest{
        Datas3tName:    "my-datas3t",
        FirstDatapoint: 1,
        LastDatapoint:  100,
    })
    if err != nil {
        panic(err)
    }
    
    // Use presigned URLs to download data directly from S3
    for _, segment := range response.DownloadSegments {
        // Download using segment.PresignedURL and segment.Range
    }
    
    // Import existing datas3ts from S3 bucket
    importResponse, err := c.ImportDatas3t(context.Background(), &client.ImportDatas3tRequest{
        BucketName: "my-bucket-config",
    })
    if err != nil {
        panic(err)
    }
    fmt.Printf("Imported %d datas3ts: %v\n", importResponse.ImportedCount, importResponse.ImportedDatas3ts)
    
    // Aggregate multiple dataranges into a single larger one
    err = c.AggregateDataRanges(context.Background(), "my-datas3t", 1, 5000, &client.AggregateOptions{
        MaxParallelism: 8,
        MaxRetries:     3,
        ProgressCallback: func(phase string, current, total int64) {
            fmt.Printf("Phase %s: %d/%d\n", phase, current, total)
        },
    })
    if err != nil {
        panic(err)
    }
}
```

## CLI Usage

The datas3t CLI provides a comprehensive command-line interface for managing buckets, datas3ts, datarange operations, and aggregation.

### Building the CLI

```bash
# Build the CLI binary
nix develop -c go build -o datas3t ./cmd/datas3t

# Or run directly
nix develop -c go run ./cmd/datas3t [command]
```

### Global Options

All commands support:
- `--server-url` - Server URL (default: `http://localhost:8765`, env: `DATAS3T_SERVER_URL`)

### Server Management

#### Start the Server
```bash
# Start the datas3t server
./datas3t server \
  --db-url "postgres://user:password@localhost:5432/datas3t" \
  --cache-dir "/path/to/cache" \
  --encryption-key "your-base64-encoded-key"

# Using environment variables
export DB_URL="postgres://user:password@localhost:5432/datas3t"
export CACHE_DIR="/path/to/cache"
export ENCRYPTION_KEY="your-encryption-key"
./datas3t server
```

#### Generate Encryption Key
```bash
# Generate a new AES-256 encryption key
./datas3t keygen
```

### Bucket Management

#### Add S3 Bucket Configuration
```bash
./datas3t bucket add \
  --name my-bucket-config \
  --endpoint https://s3.amazonaws.com \
  --bucket my-data-bucket \
  --access-key ACCESS_KEY \
  --secret-key SECRET_KEY
```

**Options:**
- `--name` - Bucket configuration name (required)
- `--endpoint` - S3 endpoint (include https:// for TLS) (required)
- `--bucket` - S3 bucket name (required)
- `--access-key` - S3 access key (required)
- `--secret-key` - S3 secret key (required)

#### List Bucket Configurations
```bash
# List all bucket configurations
./datas3t bucket list

# Output as JSON
./datas3t bucket list --json
```

### Datas3t Management

#### Add New Datas3t
```bash
./datas3t datas3t add \
  --name my-dataset \
  --bucket my-bucket-config
```

**Options:**
- `--name` - Datas3t name (required)
- `--bucket` - Bucket configuration name (required)

#### List Datas3ts
```bash
# List all datas3ts with statistics
./datas3t datas3t list

# Output as JSON
./datas3t datas3t list --json
```

#### Import Existing Datas3ts
```bash
# Import datas3ts from S3 bucket
./datas3t datas3t import \
  --bucket my-bucket-config

# Output as JSON
./datas3t datas3t import \
  --bucket my-bucket-config \
  --json
```

**Options:**
- `--bucket` - Bucket configuration name to scan for existing datas3ts (required)
- `--json` - Output results as JSON

### Datarange Operations

#### Upload TAR File
```bash
./datas3t datarange upload-tar \
  --datas3t my-dataset \
  --file /path/to/data.tar \
  --max-parallelism 8 \
  --max-retries 5
```

**Options:**
- `--datas3t` - Datas3t name (required)
- `--file` - Path to TAR file to upload (required)
- `--max-parallelism` - Maximum concurrent uploads (default: 4)
- `--max-retries` - Maximum retry attempts per chunk (default: 3)

#### Download Datapoints as TAR
```bash
./datas3t datarange download-tar \
  --datas3t my-dataset \
  --first-datapoint 1 \
  --last-datapoint 1000 \
  --output /path/to/downloaded.tar \
  --max-parallelism 8 \
  --max-retries 5 \
  --chunk-size 10485760
```

**Options:**
- `--datas3t` - Datas3t name (required)
- `--first-datapoint` - First datapoint to download (required)
- `--last-datapoint` - Last datapoint to download (required)
- `--output` - Output TAR file path (required)
- `--max-parallelism` - Maximum concurrent downloads (default: 4)
- `--max-retries` - Maximum retry attempts per chunk (default: 3)
- `--chunk-size` - Download chunk size in bytes (default: 5MB)

### Aggregation Operations

#### Aggregate Multiple Dataranges
```bash
./datas3t aggregate \
  --datas3t my-dataset \
  --first-datapoint 1 \
  --last-datapoint 5000 \
  --max-parallelism 4 \
  --max-retries 3
```

**Options:**
- `--datas3t` - Datas3t name (required)
- `--first-datapoint` - First datapoint index to include in aggregate (required)
- `--last-datapoint` - Last datapoint index to include in aggregate (required)
- `--max-parallelism` - Maximum number of concurrent operations (default: 4)
- `--max-retries` - Maximum number of retry attempts per operation (default: 3)

**What it does:**
- Downloads all source dataranges in the specified range
- Merges them into a single TAR archive with continuous datapoint numbering
- Uploads the merged archive to S3
- Atomically replaces the original dataranges with the new aggregate
- Validates that the datapoint range is fully covered by existing dataranges with no gaps

**Example Usage:**
```bash
# Example: You have uploaded multiple small TAR files and want to consolidate them
# First, check your current dataranges
./datas3t datas3t list

# Aggregate the first 10,000 datapoints into a single larger datarange
./datas3t aggregate \
  --datas3t my-dataset \
  --first-datapoint 1 \
  --last-datapoint 10000 \
  --max-parallelism 6

# Check the result - you should see fewer, larger dataranges
./datas3t datas3t list
```

**Benefits:**
- Reduces the number of S3 objects (lower storage costs)
- Improves download performance for large ranges
- Maintains all data integrity and accessibility
- Can be run multiple times to further consolidate data

### Complete Workflow Example

```bash
# 1. Generate encryption key
./datas3t keygen
export ENCRYPTION_KEY="generated-key-here"

# 2. Start server
./datas3t server &

# 3. Add bucket configuration
./datas3t bucket add \
  --name production-bucket \
  --endpoint https://s3.amazonaws.com \
  --bucket my-production-data \
  --access-key "$AWS_ACCESS_KEY" \
  --secret-key "$AWS_SECRET_KEY"

# 4. Create datas3t
./datas3t datas3t add \
  --name image-dataset \
  --bucket production-bucket

# 5. Upload data
./datas3t datarange upload-tar \
  --datas3t image-dataset \
  --file ./images-batch-1.tar

# 6. List datasets
./datas3t datas3t list

# 7. Import existing datas3ts (disaster recovery/migration)
./datas3t datas3t import \
  --bucket production-bucket

# 8. Download specific range
./datas3t datarange download-tar \
  --datas3t image-dataset \
  --first-datapoint 100 \
  --last-datapoint 200 \
  --output ./images-100-200.tar

# 9. Aggregate small dataranges for better efficiency
./datas3t aggregate \
  --datas3t image-dataset \
  --first-datapoint 1 \
  --last-datapoint 10000

# 10. Check results after aggregation
./datas3t datas3t list
```

### Environment Variables

All CLI commands support these environment variables:
- `DATAS3T_SERVER_URL` - Default server URL for all commands
- `DB_URL` - Database connection string (server command)
- `CACHE_DIR` - Cache directory path (server command)
- `ENCRYPTION_KEY` - Base64-encoded encryption key (server command)

## File Naming Convention

Datapoints must follow the naming pattern `%020d.<extension>`:
- ✅ `00000000000000000001.txt`
- ✅ `00000000000000000042.jpg`  
- ✅ `00000000000000001337.json`
- ❌ `file1.txt`
- ❌ `1.txt`
- ❌ `001.txt`

## Performance Characteristics

### Storage Efficiency
- **Small Files**: 99%+ storage efficiency vs individual S3 objects
- **Large Datas3ts**: Linear scaling with datas3t size

### Access Performance
- **Index Lookup**: O(1) file location within TAR
- **Range Queries**: Optimized byte-range requests
- **Caching**: Local disk cache for frequently accessed indices

### Scalability
- **Concurrent Operations**: Supports parallel uploads/downloads
- **Large Datas3ts**: Tested with millions of datapoints
- **Distributed**: Stateless server design for horizontal scaling

## Contributing

Currently we are not accepting contributions to this project.

## Architecture Details

### Database Schema
- **s3_buckets**: S3 configuration storage
- **datas3ts**: Datas3t metadata
- **dataranges**: TAR archive metadata and byte ranges
- **datarange_uploads**: Temporary upload state management
- **aggregate_uploads**: Aggregation operation tracking and state management
- **keys_to_delete**: Scheduled deletion of S3 objects

### TAR Index Format
Binary format with 16-byte entries per file:
- Bytes 0-7: File position in TAR (big-endian uint64)
- Bytes 8-9: Header blocks count (big-endian uint16)  
- Bytes 10-15: File size (big-endian, 48-bit)

### Caching Strategy
- **Memory**: In-memory index objects during operations
- **Disk**: Persistent cache for TAR indices
- **LRU Eviction**: Automatic cleanup based on access patterns
- **Cache Keys**: SHA-256 hash of datarange metadata

## License

This project is licensed under the AGPLV3 License - see the [LICENSE](LICENSE) file for details.

## Support

For questions, issues, or contributions:
- Open an issue on GitHub
- Check existing documentation
- Review test files for usage examples

## Installation

```bash
git clone https://github.com/draganm/datas3t.git
cd datas3t
nix develop -c make build
```

## Configuration

### Database Setup

Create a PostgreSQL database and set the connection string:

```bash
export DB_URL="postgres://user:password@localhost:5432/datas3t"
export CACHE_DIR="/path/to/cache"
```

### S3 Credential Encryption

**Important: S3 credentials are encrypted at rest using AES-256-GCM with unique random nonces.**

The encryption system provides the following security features:
- **AES-256-GCM encryption**: Industry-standard authenticated encryption
- **Unique nonces**: Each encryption uses a random nonce, so identical credentials produce different encrypted values
- **Key derivation**: Input keys are SHA-256 hashed to ensure proper 32-byte key size
- **Authenticated encryption**: Protects against tampering and ensures data integrity
- **Transparent operation**: All S3 operations automatically encrypt/decrypt credentials

#### Key Generation

Generate a cryptographically secure 256-bit encryption key:

```bash
nix develop -c go run ./cmd/datas3t/main.go keygen
```

This generates a 32-byte (256-bit) random key encoded as base64. Store this key securely and set it as an environment variable:

```bash
export ENCRYPTION_KEY="your-generated-key-here"
```

#### Alternative Key Generation

You can also use `datas3t keygen` if you have built the binary:

```bash
./datas3t keygen
```

**Critical Security Notes:**
- Keep this key secure and backed up! If you lose it, you won't be able to decrypt your stored S3 credentials
- The same key must be used consistently across server restarts
- Changing the key will make existing encrypted credentials unreadable
- Store the key separately from your database backups for additional security

### Starting the Server

```bash
./datas3t server --db-url "$DB_URL" --cache-dir "$CACHE_DIR" --encryption-key "$ENCRYPTION_KEY"
```

Or using environment variables:

```bash
export DB_URL="postgres://user:password@localhost:5432/datas3t"
export CACHE_DIR="/path/to/cache"  
export ENCRYPTION_KEY="your-encryption-key"
./datas3t server
```
