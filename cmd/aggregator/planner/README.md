# Datas3t Aggregator Planner

The planner package is responsible for generating optimal aggregation plans for dataranges. It uses a hierarchical size-tiered approach that only combines ranges when the resulting aggregate would promote to a higher tier.

## Overview

The planner analyzes a set of dataranges and determines how they should be combined for optimal aggregation. It groups ranges into size tiers and merges adjacent ranges within each tier, taking into account:

- Size of dataranges
- Key adjacency (to ensure continuous data)
- Tier promotion potential (only aggregating when it moves data to a higher tier)

## Size Tiers

Dataranges are organized into size tiers using a logarithmic scale (powers of 10):

| Tier | Size Range |
|------|------------|
| 0    | 0B - 10KB |
| 1    | 10KB - 100KB |
| 2    | 100KB - 1MB |
| 3    | 1MB - 10MB |
| 4    | 10MB - 100MB |
| 5    | 100MB - 1GB |
| 6    | 1GB+ |

This tiered approach ensures that similarly sized ranges are grouped together, leading to more balanced aggregations and more efficient storage in S3. The first tier starts at 0 bytes to ensure that even very small dataranges are properly categorized.

## Usage

```go
import (
	"log/slog"
	
	"github.com/draganm/datas3t/cmd/aggregator/planner"
	"github.com/draganm/datas3t/pkg/client"
)

// Get dataranges from somewhere (e.g., client)
dataranges := []client.DataRange{...}

// Configure parameters
targetSize := int64(1 * 1024 * 1024 * 1024) // 1GB
logger := slog.Default()

// Generate aggregation plans
plans := planner.CreatePlans(dataranges, logger, targetSize)

// Process each plan
for _, plan := range plans {
	// Work with plan.StartKey, plan.EndKey, and plan.Ranges
	// Execute aggregation based on this plan
}
```

## Types

### AggregationPlan

```go
type AggregationPlan struct {
	StartKey int64             // First datapoint key in the plan
	EndKey   int64             // Last datapoint key in the plan
	Ranges   []client.DataRange // Dataranges included in this plan
}
```

## Functions

### CreatePlans

```go
func CreatePlans(
	dataranges []client.DataRange,
	log *slog.Logger,
	targetSize int64,
) []AggregationPlan
```

Generates aggregation plans based on the input dataranges.

Parameters:
- `dataranges`: The input dataranges to be aggregated
- `log`: Logger for debug/info output (can be nil)
- `targetSize`: Target size for aggregated data (used for tier boundaries)

Returns a slice of `AggregationPlan`s that can be executed.

## How the Algorithm Works

1. **Initial Distribution**: Dataranges are assigned to tiers based on their size
2. **Within-Tier Promotion**: Each tier is processed separately, looking for adjacent ranges to merge. Only if the combined size would promote to a higher tier will these ranges be merged.
3. **Cross-Tier Promotion**: If no within-tier plans are found, the algorithm then examines the entire dataset across all tiers:
   - It looks for any adjacent ranges, regardless of their original tier
   - It merges these ranges if their combined size would result in a tier higher than the highest tier of any individual range in the group
   - This allows for optimization across tier boundaries when beneficial
4. **Adjacency Requirements**: Only consecutive ranges (where EndKey+1 = next StartKey) are merged to maintain data continuity
5. **Highest Tier Exception**: Dataranges in the highest tier are never merged since they can't be promoted further

This approach ensures that aggregation only occurs when it would result in meaningful consolidation that moves data up the tier hierarchy, reducing S3 overhead and improving query performance.

## Testing

The planner package includes comprehensive tests covering various scenarios:

- Empty or single dataranges (no plans created)
- Adjacent ranges in the same tier that don't promote (no plans created)
- Adjacent ranges in the same tier that would promote (merged into a plan)
- Non-adjacent ranges (no plans created)
- Multiple adjacent ranges in the same tier that promote (merged into a single plan)
- Ranges in different tiers that promote when combined (merged across tier boundaries)
- Ranges in the highest tier (no plans created)

To run the tests:

```bash
go test ./cmd/aggregator/planner -v
``` 