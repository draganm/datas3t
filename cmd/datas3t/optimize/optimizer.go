package optimize

import (
	"sort"

	"github.com/draganm/datas3t/client"
)

const (
	MB = 1024 * 1024
	GB = 1024 * MB
	
	// Size thresholds from PLAN.md
	SmallDatapointLimit = 100       // datapoints
	SmallSizeLimit      = 10 * MB   // 10MB
	MediumSizeLimit     = 100 * MB  // 100MB
	LargeSizeLimit      = 1 * GB    // 1GB
	MaxAggregateSize    = 5 * GB    // 5GB
	
	// Minimum dataranges to aggregate for small datapoints
	MinDatarangesForSmallAggregation = 10
)

// OptimizationType describes the type of optimization
type OptimizationType string

const (
	OptimizationSmallDatapoints  OptimizationType = "Small Datapoints Aggregation"
	OptimizationSmallToMedium    OptimizationType = "Small to Medium Size Aggregation"
	OptimizationMediumToLarge    OptimizationType = "Medium to Large Size Aggregation"
	OptimizationLargeToVeryLarge OptimizationType = "Large to Very Large Size Aggregation"
)

// Datarange represents a datarange with its metadata
type Datarange struct {
	ID              int64
	MinDatapointKey int64
	MaxDatapointKey int64
	SizeBytes       int64
	DatapointCount  int64
}

// Operation represents an optimization operation to perform
type Operation struct {
	Type           OptimizationType
	DatarangeIDs   []int64
	FirstDatapoint uint64
	LastDatapoint  uint64
	TotalSize      int64
	Reason         string
}

// Optimizer handles optimization logic
type Optimizer struct {
	dataranges []Datarange
	sorted     []Datarange // sorted by MinDatapointKey
}

// NewOptimizer creates a new optimizer from datarange info
func NewOptimizer(dataranges []client.DatarangeInfo) *Optimizer {
	drs := make([]Datarange, len(dataranges))
	for i, dr := range dataranges {
		drs[i] = Datarange{
			ID:              dr.DatarangeID,
			MinDatapointKey: dr.MinDatapointKey,
			MaxDatapointKey: dr.MaxDatapointKey,
			SizeBytes:       dr.SizeBytes,
			DatapointCount:  dr.MaxDatapointKey - dr.MinDatapointKey + 1,
		}
	}
	
	// Sort by MinDatapointKey for easier consecutive range detection
	sorted := make([]Datarange, len(drs))
	copy(sorted, drs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].MinDatapointKey < sorted[j].MinDatapointKey
	})
	
	return &Optimizer{
		dataranges: drs,
		sorted:     sorted,
	}
}

// FindBestOptimization finds the best optimization according to priority rules
func (o *Optimizer) FindBestOptimization() *Operation {
	// Priority 1: Small datapoints aggregation
	if op := o.findSmallDatapointsAggregation(); op != nil {
		return op
	}
	
	// Priority 2: Small to medium size aggregation
	if op := o.findSmallToMediumAggregation(); op != nil {
		return op
	}
	
	// Priority 3: Medium to large size aggregation
	if op := o.findMediumToLargeAggregation(); op != nil {
		return op
	}
	
	// Priority 4: Large to very large size aggregation
	if op := o.findLargeToVeryLargeAggregation(); op != nil {
		return op
	}
	
	return nil
}

// findSmallDatapointsAggregation finds sequences of small dataranges to aggregate
func (o *Optimizer) findSmallDatapointsAggregation() *Operation {
	// Find consecutive dataranges with <100 datapoints
	var candidates []Datarange
	var totalSize int64
	
	for i := 0; i < len(o.sorted); i++ {
		dr := o.sorted[i]
		
		// Check if this datarange has small number of datapoints
		if dr.DatapointCount >= SmallDatapointLimit {
			// If we have enough candidates, check if we can aggregate them
			if len(candidates) >= MinDatarangesForSmallAggregation && totalSize < SmallSizeLimit {
				return o.createOperation(candidates, OptimizationSmallDatapoints,
					"Aggregating small dataranges with few datapoints")
			}
			// Reset candidates
			candidates = []Datarange{}
			totalSize = 0
			continue
		}
		
		// Check if adding this would exceed size limit
		if totalSize+dr.SizeBytes > SmallSizeLimit {
			// If we have enough candidates, aggregate what we have
			if len(candidates) >= MinDatarangesForSmallAggregation {
				return o.createOperation(candidates, OptimizationSmallDatapoints,
					"Aggregating small dataranges with few datapoints")
			}
			// Reset candidates
			candidates = []Datarange{}
			totalSize = 0
		}
		
		// Add to candidates if consecutive or first
		if len(candidates) == 0 || o.isConsecutive(candidates[len(candidates)-1], dr) {
			candidates = append(candidates, dr)
			totalSize += dr.SizeBytes
		} else {
			// Not consecutive - check if we can aggregate what we have
			if len(candidates) >= MinDatarangesForSmallAggregation && totalSize < SmallSizeLimit {
				return o.createOperation(candidates, OptimizationSmallDatapoints,
					"Aggregating small dataranges with few datapoints")
			}
			// Start new sequence
			candidates = []Datarange{dr}
			totalSize = dr.SizeBytes
		}
	}
	
	// Check remaining candidates
	if len(candidates) >= MinDatarangesForSmallAggregation && totalSize < SmallSizeLimit {
		return o.createOperation(candidates, OptimizationSmallDatapoints,
			"Aggregating small dataranges with few datapoints")
	}
	
	return nil
}

// findSmallToMediumAggregation finds small dataranges to aggregate into medium size
func (o *Optimizer) findSmallToMediumAggregation() *Operation {
	var candidates []Datarange
	var totalSize int64
	
	for i := 0; i < len(o.sorted); i++ {
		dr := o.sorted[i]
		
		// Only consider small dataranges
		if dr.SizeBytes >= SmallSizeLimit {
			// Check if we can aggregate what we have
			if len(candidates) > 1 && totalSize > SmallSizeLimit && totalSize <= MediumSizeLimit {
				return o.createOperation(candidates, OptimizationSmallToMedium,
					"Aggregating small files into medium-sized datarange")
			}
			candidates = []Datarange{}
			totalSize = 0
			continue
		}
		
		// Check if adding this would exceed medium size limit
		if totalSize+dr.SizeBytes > MediumSizeLimit {
			// Aggregate what we have if it's worth it
			if len(candidates) > 1 && totalSize > SmallSizeLimit {
				return o.createOperation(candidates, OptimizationSmallToMedium,
					"Aggregating small files into medium-sized datarange")
			}
			candidates = []Datarange{dr}
			totalSize = dr.SizeBytes
			continue
		}
		
		// Add if consecutive or first
		if len(candidates) == 0 || o.isConsecutive(candidates[len(candidates)-1], dr) {
			candidates = append(candidates, dr)
			totalSize += dr.SizeBytes
		} else {
			// Not consecutive - check if we can aggregate what we have
			if len(candidates) > 1 && totalSize > SmallSizeLimit && totalSize <= MediumSizeLimit {
				return o.createOperation(candidates, OptimizationSmallToMedium,
					"Aggregating small files into medium-sized datarange")
			}
			candidates = []Datarange{dr}
			totalSize = dr.SizeBytes
		}
	}
	
	// Check remaining candidates
	if len(candidates) > 1 && totalSize > SmallSizeLimit && totalSize <= MediumSizeLimit {
		return o.createOperation(candidates, OptimizationSmallToMedium,
			"Aggregating small files into medium-sized datarange")
	}
	
	return nil
}

// findMediumToLargeAggregation finds medium dataranges to aggregate into large size
func (o *Optimizer) findMediumToLargeAggregation() *Operation {
	var candidates []Datarange
	var totalSize int64
	
	for i := 0; i < len(o.sorted); i++ {
		dr := o.sorted[i]
		
		// Only consider medium-sized dataranges
		if dr.SizeBytes < SmallSizeLimit || dr.SizeBytes >= MediumSizeLimit {
			// Check if we can aggregate what we have
			if len(candidates) > 1 && totalSize > MediumSizeLimit {
				return o.createOperation(candidates, OptimizationMediumToLarge,
					"Aggregating medium files into large datarange")
			}
			candidates = []Datarange{}
			totalSize = 0
			continue
		}
		
		// Check if adding this would exceed large size limit
		if totalSize+dr.SizeBytes > LargeSizeLimit {
			// Aggregate what we have if it's worth it
			if len(candidates) > 1 && totalSize > MediumSizeLimit {
				return o.createOperation(candidates, OptimizationMediumToLarge,
					"Aggregating medium files into large datarange")
			}
			candidates = []Datarange{dr}
			totalSize = dr.SizeBytes
			continue
		}
		
		// Add if consecutive or first
		if len(candidates) == 0 || o.isConsecutive(candidates[len(candidates)-1], dr) {
			candidates = append(candidates, dr)
			totalSize += dr.SizeBytes
		} else {
			// Not consecutive - check if we can aggregate what we have
			if len(candidates) > 1 && totalSize > MediumSizeLimit {
				return o.createOperation(candidates, OptimizationMediumToLarge,
					"Aggregating medium files into large datarange")
			}
			candidates = []Datarange{dr}
			totalSize = dr.SizeBytes
		}
	}
	
	// Check remaining candidates
	if len(candidates) > 1 && totalSize > MediumSizeLimit {
		return o.createOperation(candidates, OptimizationMediumToLarge,
			"Aggregating medium files into large datarange")
	}
	
	return nil
}

// findLargeToVeryLargeAggregation finds large dataranges to aggregate into very large size
func (o *Optimizer) findLargeToVeryLargeAggregation() *Operation {
	var candidates []Datarange
	var totalSize int64
	
	for i := 0; i < len(o.sorted); i++ {
		dr := o.sorted[i]
		
		// Only consider large dataranges (100MB-1GB)
		if dr.SizeBytes < MediumSizeLimit || dr.SizeBytes >= LargeSizeLimit {
			// Check if we can aggregate what we have
			if len(candidates) > 1 && totalSize > LargeSizeLimit && totalSize <= MaxAggregateSize {
				return o.createOperation(candidates, OptimizationLargeToVeryLarge,
					"Aggregating large files into very large datarange")
			}
			candidates = []Datarange{}
			totalSize = 0
			continue
		}
		
		// Check if adding this would exceed max aggregate size
		if totalSize+dr.SizeBytes > MaxAggregateSize {
			// Aggregate what we have if it's worth it
			if len(candidates) > 1 && totalSize > LargeSizeLimit {
				return o.createOperation(candidates, OptimizationLargeToVeryLarge,
					"Aggregating large files into very large datarange")
			}
			candidates = []Datarange{dr}
			totalSize = dr.SizeBytes
			continue
		}
		
		// Add if consecutive or first
		if len(candidates) == 0 || o.isConsecutive(candidates[len(candidates)-1], dr) {
			candidates = append(candidates, dr)
			totalSize += dr.SizeBytes
		} else {
			// Not consecutive - check if we can aggregate what we have
			if len(candidates) > 1 && totalSize > LargeSizeLimit && totalSize <= MaxAggregateSize {
				return o.createOperation(candidates, OptimizationLargeToVeryLarge,
					"Aggregating large files into very large datarange")
			}
			candidates = []Datarange{dr}
			totalSize = dr.SizeBytes
		}
	}
	
	// Check remaining candidates
	if len(candidates) > 1 && totalSize > LargeSizeLimit && totalSize <= MaxAggregateSize {
		return o.createOperation(candidates, OptimizationLargeToVeryLarge,
			"Aggregating large files into very large datarange")
	}
	
	return nil
}

// isConsecutive checks if two dataranges are consecutive
func (o *Optimizer) isConsecutive(a, b Datarange) bool {
	return a.MaxDatapointKey+1 == b.MinDatapointKey
}

// createOperation creates an operation from candidate dataranges
func (o *Optimizer) createOperation(candidates []Datarange, opType OptimizationType, reason string) *Operation {
	if len(candidates) < 2 {
		return nil
	}
	
	var ids []int64
	var totalSize int64
	minDatapoint := candidates[0].MinDatapointKey
	maxDatapoint := candidates[0].MaxDatapointKey
	
	for _, dr := range candidates {
		ids = append(ids, dr.ID)
		totalSize += dr.SizeBytes
		if dr.MinDatapointKey < minDatapoint {
			minDatapoint = dr.MinDatapointKey
		}
		if dr.MaxDatapointKey > maxDatapoint {
			maxDatapoint = dr.MaxDatapointKey
		}
	}
	
	return &Operation{
		Type:           opType,
		DatarangeIDs:   ids,
		FirstDatapoint: uint64(minDatapoint),
		LastDatapoint:  uint64(maxDatapoint),
		TotalSize:      totalSize,
		Reason:         reason,
	}
}