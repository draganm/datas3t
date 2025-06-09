package optimizer

import (
	"fmt"
	"sort"

	"github.com/draganm/datas3t/pkg/client"
)

const (
	MaxChunkSize = 2 * 1024 * 1024 * 1024 // 2GB in bytes
)

// MergeProposal represents a proposed merge operation
type MergeProposal struct {
	DataRangeIndices []int
	ResultSize       uint64
	Cost             uint64
	Efficiency       float64 // Cost efficiency metric
	Description      string
}

// DataRangeMergeOptimizer handles the optimization logic
type DataRangeMergeOptimizer struct {
	dataRanges []client.DataRange
}

// NewDataRangeMergeOptimizer creates a new optimizer instance
func NewDataRangeMergeOptimizer(dataRanges []client.DataRange) *DataRangeMergeOptimizer {
	return &DataRangeMergeOptimizer{dataRanges: dataRanges}
}

// areDataRangesContinuous checks if data ranges can be merged (continuous range)
func (drmo *DataRangeMergeOptimizer) areDataRangesContinuous(rangeIndices []int) bool {
	if len(rangeIndices) < 2 {
		return false
	}

	// Sort data ranges by min datapoint key
	sortedRanges := make([]client.DataRange, len(rangeIndices))
	for i, idx := range rangeIndices {
		sortedRanges[i] = drmo.dataRanges[idx]
	}

	sort.Slice(sortedRanges, func(i, j int) bool {
		return sortedRanges[i].MinDatapointKey < sortedRanges[j].MinDatapointKey
	})

	// Check if data ranges form a continuous sequence
	for i := 1; i < len(sortedRanges); i++ {
		if sortedRanges[i].MinDatapointKey != sortedRanges[i-1].MaxDatapointKey+1 {
			return false
		}
	}

	return true
}

// calculateMergedSize calculates total size of merged data ranges
func (drmo *DataRangeMergeOptimizer) calculateMergedSize(rangeIndices []int) uint64 {
	var totalSize uint64
	for _, idx := range rangeIndices {
		totalSize += drmo.dataRanges[idx].SizeBytes
	}
	return totalSize
}

// calculateMergeCost calculates the cost of merging data ranges
// Cost is proportional to the size of the resulting range
func (drmo *DataRangeMergeOptimizer) calculateMergeCost(resultSize uint64) uint64 {
	return resultSize
}

var MaxChunkSize80Percent = float64(MaxChunkSize) * 0.8

// calculateEfficiency calculates merge efficiency considering future operations
func (drmo *DataRangeMergeOptimizer) calculateEfficiency(rangeIndices []int, resultSize uint64) float64 {
	if len(rangeIndices) < 2 {
		return 0
	}

	// Base efficiency: more ranges merged = better
	baseEfficiency := float64(len(rangeIndices))

	// Size efficiency: prefer merging similar-sized ranges
	sizes := make([]uint64, len(rangeIndices))
	for i, idx := range rangeIndices {
		sizes[i] = drmo.dataRanges[idx].SizeBytes
	}

	// Calculate size variance penalty
	var mean float64
	for _, size := range sizes {
		mean += float64(size)
	}
	mean /= float64(len(sizes))

	var variance float64
	for _, size := range sizes {
		variance += (float64(size) - mean) * (float64(size) - mean)
	}
	variance /= float64(len(sizes))

	// Lower variance = higher efficiency
	sizeEfficiency := 1.0 / (1.0 + variance/1000000) // Normalize variance

	// Future potential: penalize creating very large ranges that can't be merged further
	futurePenalty := 1.0
	if resultSize > uint64(MaxChunkSize80Percent) { // If result is > 80% of max size
		futurePenalty = 0.5
	}

	return baseEfficiency * sizeEfficiency * futurePenalty
}

// findAdjacentDataRanges finds all groups of adjacent data ranges that can be merged
func (drmo *DataRangeMergeOptimizer) findAdjacentDataRanges() [][]int {
	var adjacentGroups [][]int

	// Sort range indices by min datapoint key
	indices := make([]int, len(drmo.dataRanges))
	for i := range indices {
		indices[i] = i
	}

	sort.Slice(indices, func(i, j int) bool {
		return drmo.dataRanges[indices[i]].MinDatapointKey < drmo.dataRanges[indices[j]].MinDatapointKey
	})

	// Find continuous sequences
	for i := 0; i < len(indices); i++ {
		currentGroup := []int{indices[i]}

		// Extend group as far as possible
		for j := i + 1; j < len(indices); j++ {
			testGroup := append(currentGroup, indices[j])
			if drmo.areDataRangesContinuous(testGroup) &&
				drmo.calculateMergedSize(testGroup) <= uint64(MaxChunkSize) {
				currentGroup = testGroup
			} else {
				break
			}
		}

		// Add group if it contains more than one range
		if len(currentGroup) > 1 {
			adjacentGroups = append(adjacentGroups, currentGroup)
		}

		// Skip processed indices
		i += len(currentGroup) - 1
	}

	return adjacentGroups
}

// ProposeNextMerge analyzes data ranges and proposes the best merge operation (optimized version)
func (drmo *DataRangeMergeOptimizer) ProposeNextMerge() *MergeProposal {
	if len(drmo.dataRanges) < 2 {
		return nil // Nothing to merge
	}

	// Pre-sort indices by min datapoint key for efficient processing
	indices := make([]int, len(drmo.dataRanges))
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return drmo.dataRanges[indices[i]].MinDatapointKey < drmo.dataRanges[indices[j]].MinDatapointKey
	})

	// Find the first and longest sequence of consecutive ranges that fit within size limit
	var bestProposal *MergeProposal
	bestEfficiency := 0.0

	for i := 0; i < len(indices)-1; i++ {
		// Start a new sequence from this position
		currentIndices := []int{indices[i]}
		currentSize := drmo.dataRanges[indices[i]].SizeBytes

		// Extend the sequence as far as possible
		for j := i + 1; j < len(indices); j++ {
			currentIdx := indices[j]
			prevIdx := indices[j-1]

			// Check if ranges are consecutive
			if drmo.dataRanges[currentIdx].MinDatapointKey != drmo.dataRanges[prevIdx].MaxDatapointKey+1 {
				break // Not consecutive
			}

			// Check if adding this range would exceed size limit
			newSize := currentSize + drmo.dataRanges[currentIdx].SizeBytes
			if newSize > uint64(MaxChunkSize) {
				break // Would exceed size limit
			}

			// Add to current sequence
			currentIndices = append(currentIndices, currentIdx)
			currentSize = newSize

			// If we have at least 2 ranges, this is a valid merge proposal
			if len(currentIndices) >= 2 {
				// Simple efficiency metric: prefer merging more ranges
				efficiency := float64(len(currentIndices))

				// Prefer larger merges and penalize very large results
				if currentSize > uint64(MaxChunkSize80Percent) {
					efficiency *= 0.5
				}

				if efficiency > bestEfficiency {
					bestEfficiency = efficiency

					// Create proposal
					proposal := &MergeProposal{
						DataRangeIndices: make([]int, len(currentIndices)),
						ResultSize:       currentSize,
						Cost:             currentSize,
						Efficiency:       efficiency,
					}
					copy(proposal.DataRangeIndices, currentIndices)

					// Create description (limited to avoid performance issues)
					if len(currentIndices) <= 10 {
						objectKeys := make([]string, len(currentIndices))
						for k, idx := range currentIndices {
							objectKeys[k] = drmo.dataRanges[idx].ObjectKey
						}
						proposal.Description = fmt.Sprintf("Merge %d data ranges (Objects: %v) -> %d bytes (efficiency: %.2f)",
							len(currentIndices), objectKeys, currentSize, efficiency)
					} else {
						proposal.Description = fmt.Sprintf("Merge %d data ranges -> %d bytes (efficiency: %.2f)",
							len(currentIndices), currentSize, efficiency)
					}

					bestProposal = proposal
				}
			}
		}
	}

	return bestProposal
}

// Helper function to create a merged data range from multiple ranges
func NewMergedDataRange(objectKey string, dataRanges []client.DataRange) client.DataRange {
	if len(dataRanges) == 0 {
		return client.DataRange{ObjectKey: objectKey}
	}

	// Sort data ranges by min datapoint key to ensure continuity
	sort.Slice(dataRanges, func(i, j int) bool {
		return dataRanges[i].MinDatapointKey < dataRanges[j].MinDatapointKey
	})

	var totalSize uint64
	for _, dr := range dataRanges {
		totalSize += dr.SizeBytes
	}

	return client.DataRange{
		ObjectKey:       objectKey,
		MinDatapointKey: dataRanges[0].MinDatapointKey,
		MaxDatapointKey: dataRanges[len(dataRanges)-1].MaxDatapointKey,
		SizeBytes:       totalSize,
	}
}
