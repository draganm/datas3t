# S3 Storage Layout for datas3t

This document describes how datasets, dataranges, and metadata are organized and stored in the S3 bucket.

## Overall Structure

The S3 bucket is organized with the following hierarchical structure:

```
bucket/
├── dataset/
│   ├── <dataset_name>/
│   │   ├── datapoints/
│   │   │   ├── <from>-<to>.tar
│   │   │   └── <from>-<to>.tar.metadata
```

## Key Components

### Datasets

- Each dataset is identified by a unique name (`<dataset_name>`)
- All data related to a specific dataset is stored under `dataset/<dataset_name>/`

### Datapoint Ranges

- Datapoints are grouped into ranges and stored in tar archives
- Each tar archive is located at `dataset/<dataset_name>/datapoints/<from>-<to>.tar`
- The `<from>` and `<to>` are 20-digit zero-padded sequence numbers representing the minimum and maximum datapoint keys in the archive
- Example: `dataset/my_dataset/datapoints/00000000000000000001-00000000000000000100.tar` (datapoints 1-100)

### Datapoint Structure

- Within each tar archive, individual datapoints are stored as files
- Each file follows the naming convention: `<20-digit-sequence-number>.<extension>`
- The sequence numbers are continuous within each archive with no gaps
- The tar archive structure preserves file metadata, including offsets for efficient access

### Metadata Files

- For each datapoint range archive, a corresponding metadata file is stored
- Metadata files are located at `dataset/<dataset_name>/datapoints/<from>-<to>.tar.metadata`
- The metadata contains:
  - Datapoint IDs
  - Begin and end offsets within the tar archive
  - Data hash for each datapoint (xxHash)
- Metadata is compressed using zstd with best compression
- Content-Type is set to "application/zstd"

## Implementation Notes

- The system validates that uploaded datapoint ranges do not overlap with existing ranges
- Datapoints must be sequential with no gaps within each tar archive
- Each tar archive can contain one or more datapoints
- The database maintains references to the S3 objects for efficient retrieval 