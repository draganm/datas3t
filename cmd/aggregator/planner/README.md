# Data Aggregation Planner

The planner package implements a system for planning data aggregation operations based on the size of data ranges. It helps organize data into appropriate aggregation levels to maintain efficient data storage and retrieval.

## Overview

The planner takes a list of data ranges and creates aggregation plans based on predefined size thresholds. This helps in managing data at different levels of granularity, from small chunks to larger aggregated blocks.

## Key Components

### AggregationOperation

An `AggregationOperation` represents a sequence of data ranges that should be aggregated together. It provides several methods:

- `SizeBytes()`: Calculates the total size of all data ranges in the operation
- `StartKey()`: Returns the minimum datapoint key from the first range
- `EndKey()`: Returns the maximum datapoint key from the last range
- `Level()`: Determines the appropriate aggregation level based on the total size

### Size Thresholds

The system defines four different aggregation levels:

1. Level 0: < 10MB
2. Level 1: < 1GB
3. Level 2: < 100GB
4. Level 3: ≥ 100GB (top level)

## How It Works

1. The `CreatePlans` function takes a list of data ranges and returns a list of aggregation operations.

2. The process:
   - Sorts data ranges by their minimum datapoint key
   - Iteratively creates aggregation operations by:
     - Starting with all remaining ranges
     - Reducing the operation size until it fits within the appropriate level
     - Ensuring the aggregation level is not higher than the previous operation
     - Creating a new plan entry when an appropriate aggregation is found

3. The planner ensures that:
   - Aggregations are created in order of increasing size
   - Each aggregation operation contains ranges that are contiguous in time
   - The size of each aggregation fits within the appropriate level threshold

## Usage Example

```go
dataranges := []client.DataRange{
    // ... your data ranges ...
}
plans := planner.CreatePlans(dataranges)
```

Each plan in the returned slice represents a set of data ranges that should be aggregated together based on their size and temporal proximity.
