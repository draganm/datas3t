# Data Dictionary

This document defines the key terms and concepts used in the Datas3t project.

## Core Concepts

### Dataset
A named collection of data that is stored and managed by the Datas3t service. Datasets serve as the top-level organizational unit in the system. Each dataset:
- Has a unique identifier (name)
- Contains multiple datapoints
- Can be organized into multiple dataranges
- Is stored in S3-compatible storage
- Has metadata tracking creation and update timestamps

### Datapoint
An individual piece of data within a dataset. Each datapoint:
- Is uniquely identified by a datapoint key (a numerical identifier)
- Has a specific location in storage defined by begin and end offsets
- Contains arbitrary data (could be text, binary, etc.)
- Belongs to a specific datarange
- Is accessed via APIs that abstract the underlying storage details

### Datapoint Range (Datarange)
A contiguous collection of datapoints within a dataset. Dataranges help organize and optimize access to datapoints. Each datarange:
- Contains multiple datapoints with sequential keys
- Has a defined minimum and maximum datapoint key
- Maps to a specific object in S3-compatible storage
- Enables efficient retrieval of sequential data
- Helps partition large datasets into manageable chunks

### Object Key
A unique identifier for objects stored in S3-compatible storage. Each datarange is associated with an object key that specifies its location in storage.

## Storage Concepts

### S3-Compatible Storage
The underlying storage system used by Datas3t. This can be:
- AWS S3
- MinIO (for development and testing)
- Any other S3-compatible object storage service

### TAR Format
The archive format used to store datapoints within a datarange. The TAR format:
- Packages multiple datapoints into a single object
- Preserves file metadata
- Allows for efficient storage and retrieval of datapoints

## Data Operations

### Upload Datarange
The process of adding a new datarange to a dataset. This operation:
- Accepts a TAR file containing multiple datapoints
- Validates the datapoint format and sequence
- Stores the data in S3-compatible storage
- Updates the database with metadata about the datapoints and datarange

### Retrieve Datapoint
The process of accessing a specific datapoint within a dataset. This operation:
- Locates the appropriate datarange based on the datapoint key
- Retrieves the datapoint from S3-compatible storage
- Returns the datapoint data to the client

## Database Schema

### datasets Table
Stores metadata about datasets:
- name: Unique identifier for the dataset
- created_at: Timestamp when the dataset was created
- updated_at: Timestamp when the dataset was last updated

### dataranges Table
Stores information about dataranges:
- id: Unique identifier for the datarange
- dataset_name: Name of the dataset this datarange belongs to
- object_key: S3 object key where the datarange is stored
- min_datapoint_key: Smallest datapoint key in this datarange
- max_datapoint_key: Largest datapoint key in this datarange

### datapoints Table
Stores information about individual datapoints:
- id: Unique identifier for the datapoint record
- datarange_id: ID of the datarange this datapoint belongs to
- datapoint_key: Unique key for the datapoint within the dataset
- begin_offset: Start position of the datapoint within the datarange tar file
- end_offset: End position of the datapoint within the datarange tar file


