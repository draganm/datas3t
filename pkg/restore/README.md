# Database Restoration Package

The `restore` package provides functionality to restore database records from S3 storage when the database is empty.

## Purpose

This package is designed to handle scenarios where the database has been lost or reset, but the S3 data still exists. It allows for automatic recovery of metadata by examining the S3 bucket contents.

## Features

- Checks if the database is empty
- Discovers datasets and dataranges by analyzing S3 object keys
- Downloads and decodes metadata files for each datarange
- Reconstructs database records for datasets, dataranges, and datapoints
- All restoration happens in a single transaction for atomicity
- Smart handling of overlapping dataranges:
  - Keeps larger dataranges that fully contain smaller ones
  - Schedules redundant smaller dataranges for deletion
  - Detects and errors on partial overlaps between dataranges
  - Ensures data consistency by validating datarange boundaries

## Main Function

The package provides a primary function:

```go
func RestoreIfNeeded(ctx context.Context, config Config) error
```

This function checks if the database is empty and, if so, performs the restoration process.

## Configuration

The restoration process requires configuration via the `Config` struct:

```go
type Config struct {
    Logger   *slog.Logger
    DB       *sql.DB
    S3Client *s3.Client
    Bucket   string
}
```

## Recovery Process

1. Check if the database is empty by counting datasets
2. If empty, discover all datasets and dataranges from S3 objects
3. Begin a database transaction
4. For each dataset:
   - Create the dataset in the database
   - Filter overlapping dataranges:
     - Check for partial overlaps (return error if detected)
     - Keep larger dataranges that fully contain smaller ones
     - Schedule redundant smaller dataranges for deletion
   - For each kept datarange:
     - Extract metadata from object key (min/max keys)
     - Get object size via HeadObject
     - Insert datarange into database
     - Download and decode metadata file
     - Insert datapoints from metadata
5. Commit the transaction

## Overlap Handling

The package handles different types of overlaps between dataranges:

- **Full containment**: When one datarange completely contains another (e.g., range 1-6 contains 3-4), the larger range is kept and the smaller one is scheduled for deletion.
- **Partial overlap**: When dataranges partially overlap (e.g., range 1-4 and 3-6), the restoration process will fail with an error, as this indicates inconsistent or corrupted data.
- **No overlap**: Independent dataranges are all preserved.

This ensures that the restored database only contains the most efficient set of dataranges while maintaining data integrity.

## Integration

This package is integrated into the server initialization process in `pkg/server/server.go`. The server calls `RestoreIfNeeded` during startup after initializing the S3 client. 