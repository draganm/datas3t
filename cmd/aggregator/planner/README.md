# Data Aggregation Planner

The planner package implements a system for planning data aggregation operations based on the size of data ranges. It helps organize data into appropriate aggregation tiers to maintain efficient data storage and retrieval.

## Overview

The planner takes a list of data ranges and creates aggregation plans based on predefined size tiers. This helps in managing data at different levels of granularity, from small chunks to larger aggregated blocks.

## Key Components

### AggregationOperation

An `AggregationOperation` represents a sequence of data ranges that should be aggregated together. It provides several methods:

- `SizeBytes()`: Calculates the total size of all data ranges in the operation
- `StartKey()`: Returns the minimum datapoint key from the first range
- `EndKey()`: Returns the maximum datapoint key from the last range
- `Level()`: Determines the appropriate aggregation tier based on the total size
- `NumberOfDatapoints()`: Calculates the total number of datapoints across all ranges

### Size Tiers

The system defines four different aggregation tiers:

1. Tier 0: < 10MB
2. Tier 1: < 1GB
3. Tier 2: < 100GB
4. Tier 3: ≥ 100GB (top tier)

## How It Works

1. The `CreatePlans` function takes a list of data ranges and returns a list of aggregation operations.

2. The process:
   - Sorts data ranges by their minimum datapoint key
   - Iteratively creates aggregation operations by:
     - Starting with all remaining ranges
     - Reducing the operation size until it fits within the appropriate tier
     - Ensuring the aggregation tier is not higher than the previous operation
     - Creating a new plan entry when an appropriate aggregation is found

3. The planner ensures that:
   - Aggregations are created in order of increasing size
   - Each aggregation operation contains ranges that are contiguous in time
   - The size of each aggregation fits within the appropriate tier threshold
   - Ranges in the highest tier are never merged since they can't be promoted further

4. Special handling for small datasets:
   - When dealing with 1000 or more datasets, the planner checks the average number of datapoints per dataset
   - If the average is less than 10 datapoints per dataset, all datasets are aggregated together regardless of size
   - This helps prevent fragmentation when dealing with many small datasets

This approach offers several benefits:
- **Targeted Optimization**: Only performs aggregation when it results in tier promotion
- **Reduced S3 Overhead**: Minimizes small object transfers to S3
- **Hierarchical Growth**: Gradually moves objects up the tier hierarchy
- **Efficient Storage**: Avoids unnecessary merges that don't improve storage characteristics
- **Respects Data Continuity**: Only merges adjacent ranges with sequential key spaces

## Usage Example

```go
dataranges := []client.DataRange{
    // ... your data ranges ...
}
plans := planner.CreatePlans(dataranges)
```

Each plan in the returned slice represents a set of data ranges that should be aggregated together based on their size and temporal proximity.
