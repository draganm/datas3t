package optimize

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/draganm/datas3t/client"
)

const (
	MaxAggregateSize = 5 * 1024 * 1024 * 1024 // 5GB in bytes
	TargetSize       = 1 * 1024 * 1024 * 1024 // 1GB target size
	MinThreshold     = 1.0                    // Minimum AVS score to perform aggregation
	OperationCostBase = 0.1                   // Base cost per operation
)

// TarFile represents a tar file with metadata (adapted from client.DatarangeInfo)
type TarFile struct {
	ID     string
	Size   int64
	MinID  uint64 // Minimum datapoint ID in the file
	MaxID  uint64 // Maximum datapoint ID in the file
	S3Key  string
	Created time.Time
}

// AggregationOperation represents a potential aggregation
type AggregationOperation struct {
	Files []TarFile
	Score float64
	FirstDatapoint uint64
	LastDatapoint  uint64
}

// AggregationOptimizer handles the aggregation logic
type AggregationOptimizer struct {
	files []TarFile
	minScore float64
	targetSize int64
	maxAggregateSize int64
	operationCostBase float64
	
	// Balance control parameters
	minFilesForSmall int
	maxSizeRatio     int
	allowMixedSizes  bool
}

// NewAggregationOptimizer creates a new optimizer
func NewAggregationOptimizer(files []TarFile) *AggregationOptimizer {
	return &AggregationOptimizer{
		files: files,
		minScore: MinThreshold,
		targetSize: TargetSize,
		maxAggregateSize: MaxAggregateSize,
		operationCostBase: OperationCostBase,
		
		// Default balance parameters
		minFilesForSmall: 5,
		maxSizeRatio:     100,
		allowMixedSizes:  false,
	}
}

// SetThresholds allows customizing optimization thresholds
func (ao *AggregationOptimizer) SetThresholds(minScore float64, targetSize int64, maxAggregateSize int64) {
	ao.minScore = minScore
	ao.targetSize = targetSize
	ao.maxAggregateSize = maxAggregateSize
}

// SetBalanceParameters allows customizing balance validation parameters
func (ao *AggregationOptimizer) SetBalanceParameters(minFilesForSmall int, maxSizeRatio int, allowMixedSizes bool) {
	ao.minFilesForSmall = minFilesForSmall
	ao.maxSizeRatio = maxSizeRatio
	ao.allowMixedSizes = allowMixedSizes
}

// ConvertFromDatarangeInfo converts client.DatarangeInfo to TarFile
func ConvertFromDatarangeInfo(dataranges []client.DatarangeInfo) []TarFile {
	tarFiles := make([]TarFile, len(dataranges))
	for i, dr := range dataranges {
		tarFiles[i] = TarFile{
			ID:      fmt.Sprintf("datarange-%d", dr.DatarangeID),
			Size:    dr.SizeBytes,
			MinID:   uint64(dr.MinDatapointKey),
			MaxID:   uint64(dr.MaxDatapointKey),
			S3Key:   dr.DataObjectKey,
			Created: time.Now(), // We don't have creation time from DatarangeInfo
		}
	}
	return tarFiles
}

// FindBestAggregation finds the optimal aggregation operation
func (ao *AggregationOptimizer) FindBestAggregation() *AggregationOperation {
	bestScore := 0.0
	var bestOperation *AggregationOperation

	// Generate candidate groups
	candidates := ao.generateCandidateGroups()
	for _, group := range candidates {
		if len(group) < 2 {
			continue
		}

		score := ao.calculateAVS(group)
		if score > bestScore {
			bestScore = score
			
			// Calculate the range for this group
			minDatapoint := uint64(math.MaxUint64)
			maxDatapoint := uint64(0)
			for _, file := range group {
				if file.MinID < minDatapoint {
					minDatapoint = file.MinID
				}
				if file.MaxID > maxDatapoint {
					maxDatapoint = file.MaxID
				}
			}
			
			bestOperation = &AggregationOperation{
				Files: group,
				Score: score,
				FirstDatapoint: minDatapoint,
				LastDatapoint:  maxDatapoint,
			}
		}
	}

	if bestScore > ao.minScore {
		return bestOperation
	}

	return nil
}

// FindAllBeneficialAggregations finds all aggregations above the threshold
func (ao *AggregationOptimizer) FindAllBeneficialAggregations() []*AggregationOperation {
	var operations []*AggregationOperation

	// Generate candidate groups
	candidates := ao.generateCandidateGroups()
	for _, group := range candidates {
		if len(group) < 2 {
			continue
		}

		score := ao.calculateAVS(group)
		if score > ao.minScore {
			// Calculate the range for this group
			minDatapoint := uint64(math.MaxUint64)
			maxDatapoint := uint64(0)
			for _, file := range group {
				if file.MinID < minDatapoint {
					minDatapoint = file.MinID
				}
				if file.MaxID > maxDatapoint {
					maxDatapoint = file.MaxID
				}
			}
			
			operations = append(operations, &AggregationOperation{
				Files: group,
				Score: score,
				FirstDatapoint: minDatapoint,
				LastDatapoint:  maxDatapoint,
			})
		}
	}

	// Sort by score (highest first)
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].Score > operations[j].Score
	})

	// Remove conflicting operations to ensure no overlaps
	return ao.removeConflictingOperations(operations)
}

// removeConflictingOperations filters out operations that would conflict with each other
// Operations conflict if they share any files (TarFile IDs) or have overlapping datapoint ranges
func (ao *AggregationOptimizer) removeConflictingOperations(operations []*AggregationOperation) []*AggregationOperation {
	if len(operations) == 0 {
		return operations
	}

	var nonConflictingOps []*AggregationOperation
	usedFileIDs := make(map[string]bool)
	
	for _, op := range operations {
		hasConflict := false
		
		// Check if any file in this operation is already used
		for _, file := range op.Files {
			if usedFileIDs[file.ID] {
				hasConflict = true
				break
			}
		}
		
		// If no conflict, add this operation and mark its files as used
		if !hasConflict {
			nonConflictingOps = append(nonConflictingOps, op)
			
			// Mark all files in this operation as used
			for _, file := range op.Files {
				usedFileIDs[file.ID] = true
			}
		}
	}
	
	return nonConflictingOps
}

// calculateAVS calculates the Aggregation Value Score
func (ao *AggregationOptimizer) calculateAVS(files []TarFile) float64 {
	if len(files) < 2 {
		return 0
	}

	objectsReduced := float64(len(files) - 1)
	totalSize := int64(0)
	for _, f := range files {
		totalSize += f.Size
	}

	// Don't aggregate if it would exceed max size
	if totalSize > ao.maxAggregateSize {
		return 0
	}

	// Don't aggregate if the group is too unbalanced
	if !ao.isBalancedGroup(files) {
		return 0
	}

	sizeFactor := ao.calculateSizeFactor(totalSize)
	consecutiveBonus := ao.calculateConsecutiveBonus(files)
	operationCost := ao.estimateOperationCost(files)

	return (objectsReduced * sizeFactor * consecutiveBonus) - operationCost
}

// isBalancedGroup checks if a group of files is balanced enough for aggregation
func (ao *AggregationOptimizer) isBalancedGroup(files []TarFile) bool {
	if len(files) < 2 {
		return true
	}

	// Calculate size statistics
	var sizes []int64
	totalSize := int64(0)
	for _, f := range files {
		sizes = append(sizes, f.Size)
		totalSize += f.Size
	}

	// Sort sizes to find min and max
	sort.Slice(sizes, func(i, j int) bool {
		return sizes[i] < sizes[j]
	})

	minSize := sizes[0]
	maxSize := sizes[len(sizes)-1]

	// Rule 1: Reject if largest file is more than configured ratio bigger than smallest
	if maxSize > minSize*int64(ao.maxSizeRatio) {
		return false
	}

	// Rule 2: If we have very small files (< 1MB), only aggregate with other small files
	const smallFileThreshold = 1024 * 1024 // 1MB
	const mediumFileThreshold = 50 * 1024 * 1024 // 50MB
	
	hasSmallFiles := false
	hasLargeFiles := false
	for _, size := range sizes {
		if size < smallFileThreshold {
			hasSmallFiles = true
		}
		if size > mediumFileThreshold {
			hasLargeFiles = true
		}
	}

	// Don't mix very small files with large files (unless explicitly allowed)
	if hasSmallFiles && hasLargeFiles && !ao.allowMixedSizes {
		return false
	}

	// Rule 3: For small files, require at least configured minimum files to make aggregation worthwhile
	if maxSize < smallFileThreshold && len(files) < ao.minFilesForSmall {
		return false
	}

	// Rule 4: Size ratio check - largest should not be more than 10x the average
	avgSize := totalSize / int64(len(files))
	if maxSize > avgSize*10 {
		return false
	}

	return true
}

// calculateSizeFactor calculates the size-based factor
func (ao *AggregationOptimizer) calculateSizeFactor(totalSize int64) float64 {
	ratio := float64(totalSize) / float64(ao.targetSize)
	if ratio <= 0 {
		return 0.1
	}
	
	// For small files (ratio < 1), we want to encourage aggregation
	// For files approaching target size (ratio ~= 1), we want maximum benefit
	// For files much larger than target (ratio > 1), we want to discourage
	if ratio < 1.0 {
		// Small files get progressively better scores as they approach target size
		// This ensures small files are still attractive for aggregation
		return 0.5 + (ratio * 0.5) // Range: 0.5 to 1.0
	} else {
		// Use log2 for files at or above target size
		return math.Log2(ratio)
	}
}

// calculateConsecutiveBonus calculates bonus for consecutive ID ranges
func (ao *AggregationOptimizer) calculateConsecutiveBonus(files []TarFile) float64 {
	if len(files) <= 1 {
		return 1.0
	}

	// Sort files by MinID
	sortedFiles := make([]TarFile, len(files))
	copy(sortedFiles, files)
	sort.Slice(sortedFiles, func(i, j int) bool {
		return sortedFiles[i].MinID < sortedFiles[j].MinID
	})

	// Calculate how much of the total range is consecutive
	totalDatapoints := uint64(0)
	consecutiveRanges := uint64(0)
	for i, file := range sortedFiles {
		fileRange := file.MaxID - file.MinID + 1
		totalDatapoints += fileRange
		
		if i > 0 {
			prevFile := sortedFiles[i-1]
			// Check if this file is consecutive with the previous one
			if file.MinID == prevFile.MaxID+1 {
				consecutiveRanges += fileRange
			}
		}
	}

	if totalDatapoints == 0 {
		return 1.0
	}

	// Bonus ranges from 1.0 to 2.0 based on consecutiveness
	return 1.0 + (float64(consecutiveRanges)/float64(totalDatapoints))
}

// estimateOperationCost estimates the cost of performing the aggregation
func (ao *AggregationOptimizer) estimateOperationCost(files []TarFile) float64 {
	totalSize := int64(0)
	for _, f := range files {
		totalSize += f.Size
	}

	// Cost scales with size (download + upload + processing)
	sizeCost := float64(totalSize) / float64(1024*1024*1024) // Cost per GB
	return ao.operationCostBase + sizeCost*0.1
}

// generateCandidateGroups generates candidate groups for aggregation
func (ao *AggregationOptimizer) generateCandidateGroups() [][]TarFile {
	var candidates [][]TarFile

	// Sort files by size (smallest first) for small file aggregation
	sortedBySize := make([]TarFile, len(ao.files))
	copy(sortedBySize, ao.files)
	sort.Slice(sortedBySize, func(i, j int) bool {
		return sortedBySize[i].Size < sortedBySize[j].Size
	})

	// Sort files by MinID for consecutive aggregation
	sortedByID := make([]TarFile, len(ao.files))
	copy(sortedByID, ao.files)
	sort.Slice(sortedByID, func(i, j int) bool {
		return sortedByID[i].MinID < sortedByID[j].MinID
	})

	// Strategy 1: Small file aggregation
	candidates = append(candidates, ao.generateSmallFileGroups(sortedBySize)...)

	// Strategy 2: Adjacent ID range aggregation
	candidates = append(candidates, ao.generateAdjacentIDGroups(sortedByID)...)

	// Strategy 3: Size bucket aggregation
	candidates = append(candidates, ao.generateSizeBucketGroups(sortedBySize)...)

	return candidates
}

// generateSmallFileGroups creates groups of small files
func (ao *AggregationOptimizer) generateSmallFileGroups(sortedFiles []TarFile) [][]TarFile {
	var groups [][]TarFile

	// Try different group sizes, starting with pairs
	for groupSize := 2; groupSize <= 10 && groupSize <= len(sortedFiles); groupSize++ {
		for i := 0; i <= len(sortedFiles)-groupSize; i++ {
			group := sortedFiles[i : i+groupSize]
			totalSize := int64(0)
			for _, f := range group {
				totalSize += f.Size
			}
			if totalSize <= ao.maxAggregateSize {
				groups = append(groups, group)
			}
		}
	}

	return groups
}

// generateAdjacentIDGroups creates groups of files with adjacent ID ranges
func (ao *AggregationOptimizer) generateAdjacentIDGroups(sortedFiles []TarFile) [][]TarFile {
	var groups [][]TarFile

	// Find consecutive sequences
	for i := 0; i < len(sortedFiles)-1; i++ {
		var group []TarFile
		group = append(group, sortedFiles[i])
		totalSize := sortedFiles[i].Size

		for j := i + 1; j < len(sortedFiles); j++ {
			// Check if consecutive
			if sortedFiles[j].MinID == group[len(group)-1].MaxID+1 {
				if totalSize+sortedFiles[j].Size <= ao.maxAggregateSize {
					group = append(group, sortedFiles[j])
					totalSize += sortedFiles[j].Size
				} else {
					break
				}
			} else {
				break
			}
		}

		if len(group) >= 2 {
			groups = append(groups, group)
		}
	}

	return groups
}

// generateSizeBucketGroups creates groups of similarly sized files
func (ao *AggregationOptimizer) generateSizeBucketGroups(sortedFiles []TarFile) [][]TarFile {
	var groups [][]TarFile

	// Group files in size buckets
	buckets := make(map[int][]TarFile)
	for _, file := range sortedFiles {
		// Create buckets based on order of magnitude
		bucket := int(math.Log10(float64(file.Size)))
		buckets[bucket] = append(buckets[bucket], file)
	}

	// Create groups within each bucket
	for _, bucket := range buckets {
		if len(bucket) < 2 {
			continue
		}

		// Try different combinations within the bucket
		for groupSize := 2; groupSize <= len(bucket) && groupSize <= 5; groupSize++ {
			for i := 0; i <= len(bucket)-groupSize; i++ {
				group := bucket[i : i+groupSize]
				totalSize := int64(0)
				for _, f := range group {
					totalSize += f.Size
				}
				if totalSize <= ao.maxAggregateSize {
					groups = append(groups, group)
				}
			}
		}
	}

	return groups
}