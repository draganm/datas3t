package optimizer_test

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/draganm/datas3t/cmd/aggregator/optimizer"
	"github.com/draganm/datas3t/pkg/client"
)

//go:embed testdata/beacon-attestation-rewards-dataranges.json
var beaconAttestationRewardsData []byte

// Smaller test to verify optimization mechanism and understand performance
func TestOptimizingSmallDataRanges(t *testing.T) {
	startTime := time.Now()

	const (
		numDataRanges      = 1000 // Much smaller for testing
		datapointsPerRange = 32
		bytesPerDatapoint  = 250
		bytesPerRange      = datapointsPerRange * bytesPerDatapoint // 8000 bytes
	)

	// Create initial data ranges - each range contains 32 consecutive datapoints
	initialDataRanges := make([]client.DataRange, numDataRanges)
	for i := 0; i < numDataRanges; i++ {
		startKey := uint64(i * datapointsPerRange)
		endKey := startKey + datapointsPerRange - 1

		initialDataRanges[i] = client.DataRange{
			ObjectKey:       fmt.Sprintf("object_%d", i),
			MinDatapointKey: startKey,
			MaxDatapointKey: endKey,
			SizeBytes:       bytesPerRange,
		}
	}

	// Calculate initial totals for data integrity verification
	initialTotalDatapoints := calculateTotalDatapoints(initialDataRanges)
	initialTotalBytes := calculateTotalBytes(initialDataRanges)

	t.Logf("Initial state: %d ranges, %d total datapoints, %d total bytes",
		len(initialDataRanges), initialTotalDatapoints, initialTotalBytes)

	// Track optimization metrics
	var totalBytesUploaded uint64
	var optimizationRounds int

	// Create optimizer and apply optimizations iteratively
	currentRanges := make([]client.DataRange, len(initialDataRanges))
	copy(currentRanges, initialDataRanges)

	for {
		optimizationRounds++
		optimizer := optimizer.NewDataRangeMergeOptimizer(currentRanges)

		// Get the next best merge proposal
		proposal := optimizer.ProposeNextMerge()
		if proposal == nil {
			t.Logf("No more optimization possible after %d rounds", optimizationRounds-1)
			break
		}

		t.Logf("Round %d: %s", optimizationRounds, proposal.Description)

		// Track bytes that would be uploaded for this merge
		totalBytesUploaded += proposal.ResultSize

		// Apply the merge
		newRanges, err := applyMergeProposal(currentRanges, proposal)
		if err != nil {
			t.Fatalf("Failed to apply merge proposal: %v", err)
		}

		// Verify data integrity after merge
		newTotalDatapoints := calculateTotalDatapoints(newRanges)
		newTotalBytes := calculateTotalBytes(newRanges)

		if newTotalDatapoints != initialTotalDatapoints {
			t.Fatalf("Data loss detected! Initial datapoints: %d, after merge: %d",
				initialTotalDatapoints, newTotalDatapoints)
		}

		if newTotalBytes != initialTotalBytes {
			t.Fatalf("Byte count mismatch! Initial bytes: %d, after merge: %d",
				initialTotalBytes, newTotalBytes)
		}

		currentRanges = newRanges

		// Log progress every 10 rounds for smaller test
		if optimizationRounds%10 == 0 {
			elapsed := time.Since(startTime)
			t.Logf("Progress: Round %d, %d ranges remaining, %.2f MB uploaded so far, elapsed: %v",
				optimizationRounds, len(currentRanges), float64(totalBytesUploaded)/(1024*1024), elapsed)
		}

		// Safety check to prevent infinite loops
		if optimizationRounds > 10000 {
			t.Logf("Maximum rounds reached, stopping optimization")
			break
		}
	}

	// Final verification
	finalTotalDatapoints := calculateTotalDatapoints(currentRanges)
	finalTotalBytes := calculateTotalBytes(currentRanges)

	if finalTotalDatapoints != initialTotalDatapoints {
		t.Fatalf("Final data integrity check failed! Initial datapoints: %d, final: %d",
			initialTotalDatapoints, finalTotalDatapoints)
	}

	if finalTotalBytes != initialTotalBytes {
		t.Fatalf("Final byte integrity check failed! Initial bytes: %d, final: %d",
			initialTotalBytes, finalTotalBytes)
	}

	// Calculate compression ratio
	compressionRatio := float64(len(initialDataRanges)) / float64(len(currentRanges))
	elapsed := time.Since(startTime)

	// Report final results
	t.Logf("=== OPTIMIZATION COMPLETE ===")
	t.Logf("Total execution time: %v", elapsed)
	t.Logf("Initial ranges: %d", len(initialDataRanges))
	t.Logf("Final ranges: %d", len(currentRanges))
	t.Logf("Compression ratio: %.2fx", compressionRatio)
	t.Logf("Optimization rounds: %d", optimizationRounds-1)
	t.Logf("Total bytes uploaded: %.2f MB", float64(totalBytesUploaded)/(1024*1024))
	t.Logf("Upload overhead ratio: %.2f%%", float64(totalBytesUploaded)/float64(initialTotalBytes)*100)

	// Verify final ranges are still properly ordered and continuous
	verifyRangesContinuity(t, currentRanges)
}

// Test with 100 ranges to verify termination behavior
func TestOptimizing100DataRanges(t *testing.T) {
	startTime := time.Now()

	const (
		numDataRanges      = 100 // Medium size for testing termination
		datapointsPerRange = 32
		bytesPerDatapoint  = 250
		bytesPerRange      = datapointsPerRange * bytesPerDatapoint // 8000 bytes
	)

	// Create initial data ranges - each range contains 32 consecutive datapoints
	initialDataRanges := make([]client.DataRange, numDataRanges)
	for i := 0; i < numDataRanges; i++ {
		startKey := uint64(i * datapointsPerRange)
		endKey := startKey + datapointsPerRange - 1

		initialDataRanges[i] = client.DataRange{
			ObjectKey:       fmt.Sprintf("object_%d", i),
			MinDatapointKey: startKey,
			MaxDatapointKey: endKey,
			SizeBytes:       bytesPerRange,
		}
	}

	// Calculate initial totals for data integrity verification
	initialTotalDatapoints := calculateTotalDatapoints(initialDataRanges)
	initialTotalBytes := calculateTotalBytes(initialDataRanges)

	t.Logf("Initial state: %d ranges, %d total datapoints, %d total bytes",
		len(initialDataRanges), initialTotalDatapoints, initialTotalBytes)

	// Track optimization metrics
	var totalBytesUploaded uint64
	var optimizationRounds int
	var terminationReason string

	// Create optimizer and apply optimizations iteratively
	currentRanges := make([]client.DataRange, len(initialDataRanges))
	copy(currentRanges, initialDataRanges)

	for {
		optimizationRounds++
		roundStart := time.Now()
		optimizer := optimizer.NewDataRangeMergeOptimizer(currentRanges)

		// Get the next best merge proposal
		proposal := optimizer.ProposeNextMerge()
		if proposal == nil {
			terminationReason = "no_more_merges"
			t.Logf("No more optimization possible after %d rounds", optimizationRounds-1)
			break
		}

		roundTime := time.Since(roundStart)
		t.Logf("Round %d (took %v): Merging %d ranges -> %.2f KB (efficiency: %.2f)",
			optimizationRounds, roundTime, len(proposal.DataRangeIndices),
			float64(proposal.ResultSize)/1024, proposal.Efficiency)

		// Track bytes that would be uploaded for this merge
		totalBytesUploaded += proposal.ResultSize

		// Apply the merge
		newRanges, err := applyMergeProposal(currentRanges, proposal)
		if err != nil {
			t.Fatalf("Failed to apply merge proposal: %v", err)
		}

		// Verify data integrity after each merge
		newTotalDatapoints := calculateTotalDatapoints(newRanges)
		newTotalBytes := calculateTotalBytes(newRanges)

		if newTotalDatapoints != initialTotalDatapoints {
			t.Fatalf("Data loss detected! Initial datapoints: %d, after merge: %d",
				initialTotalDatapoints, newTotalDatapoints)
		}

		if newTotalBytes != initialTotalBytes {
			t.Fatalf("Byte count mismatch! Initial bytes: %d, after merge: %d",
				initialTotalBytes, newTotalBytes)
		}

		currentRanges = newRanges

		// Log progress after each round
		elapsed := time.Since(startTime)
		t.Logf("Progress: Round %d, %d ranges remaining, %.2f KB uploaded so far, elapsed: %v",
			optimizationRounds, len(currentRanges), float64(totalBytesUploaded)/1024, elapsed)

		// Safety check to prevent infinite loops
		if optimizationRounds > 1000 {
			terminationReason = "max_rounds"
			t.Logf("Maximum rounds reached, stopping optimization")
			break
		}
	}

	// Final verification
	finalTotalDatapoints := calculateTotalDatapoints(currentRanges)
	finalTotalBytes := calculateTotalBytes(currentRanges)

	if finalTotalDatapoints != initialTotalDatapoints {
		t.Fatalf("Final data integrity check failed! Initial datapoints: %d, final: %d",
			initialTotalDatapoints, finalTotalDatapoints)
	}

	if finalTotalBytes != initialTotalBytes {
		t.Fatalf("Final byte integrity check failed! Initial bytes: %d, final: %d",
			initialTotalBytes, finalTotalBytes)
	}

	// Calculate compression ratio
	compressionRatio := float64(len(initialDataRanges)) / float64(len(currentRanges))
	elapsed := time.Since(startTime)

	// Report final results
	t.Logf("=== OPTIMIZATION COMPLETE ===")
	t.Logf("Termination reason: %s", terminationReason)
	t.Logf("Total execution time: %v", elapsed)
	t.Logf("Initial ranges: %d", len(initialDataRanges))
	t.Logf("Final ranges: %d", len(currentRanges))
	t.Logf("Compression ratio: %.2fx", compressionRatio)
	t.Logf("Optimization rounds: %d", optimizationRounds-1)
	t.Logf("Total bytes uploaded: %.2f KB", float64(totalBytesUploaded)/1024)
	t.Logf("Upload overhead ratio: %.2f%%", float64(totalBytesUploaded)/float64(initialTotalBytes)*100)

	// Verify final ranges are still properly ordered and continuous
	verifyRangesContinuity(t, currentRanges)

	// Assert proper termination
	if terminationReason == "timeout" {
		t.Logf("WARNING: Test terminated due to timeout, may not have reached optimal compression")
	} else if terminationReason == "max_rounds" {
		t.Fatalf("Test terminated due to max rounds limit - possible infinite loop")
	} else if terminationReason == "no_more_merges" {
		t.Logf("SUCCESS: Optimization terminated naturally - no more merges possible")
	}
}

// Test with 3000 ranges to understand larger scale performance
func TestOptimizing3000DataRanges(t *testing.T) {
	startTime := time.Now()

	const (
		numDataRanges      = 3000 // Larger size for performance testing
		datapointsPerRange = 32
		bytesPerDatapoint  = 250
		bytesPerRange      = datapointsPerRange * bytesPerDatapoint // 8000 bytes
	)

	t.Logf("Creating %d initial data ranges...", numDataRanges)

	// Create initial data ranges - each range contains 32 consecutive datapoints
	initialDataRanges := make([]client.DataRange, numDataRanges)
	for i := 0; i < numDataRanges; i++ {
		startKey := uint64(i * datapointsPerRange)
		endKey := startKey + datapointsPerRange - 1

		initialDataRanges[i] = client.DataRange{
			ObjectKey:       fmt.Sprintf("object_%d", i),
			MinDatapointKey: startKey,
			MaxDatapointKey: endKey,
			SizeBytes:       bytesPerRange,
		}
	}

	// Calculate initial totals for data integrity verification
	initialTotalDatapoints := calculateTotalDatapoints(initialDataRanges)
	initialTotalBytes := calculateTotalBytes(initialDataRanges)

	t.Logf("Initial state: %d ranges, %d total datapoints, %.2f MB total bytes",
		len(initialDataRanges), initialTotalDatapoints, float64(initialTotalBytes)/(1024*1024))

	// Track optimization metrics
	var totalBytesUploaded uint64
	var optimizationRounds int
	var terminationReason string

	// Create optimizer and apply optimizations iteratively
	currentRanges := make([]client.DataRange, len(initialDataRanges))
	copy(currentRanges, initialDataRanges)

	for {
		optimizationRounds++
		roundStart := time.Now()
		optimizer := optimizer.NewDataRangeMergeOptimizer(currentRanges)

		// Get the next best merge proposal
		proposal := optimizer.ProposeNextMerge()
		if proposal == nil {
			terminationReason = "no_more_merges"
			t.Logf("No more optimization possible after %d rounds", optimizationRounds-1)
			break
		}

		roundTime := time.Since(roundStart)
		t.Logf("Round %d (took %v): Merging %d ranges -> %.2f MB (efficiency: %.2f)",
			optimizationRounds, roundTime, len(proposal.DataRangeIndices),
			float64(proposal.ResultSize)/(1024*1024), proposal.Efficiency)

		// Track bytes that would be uploaded for this merge
		totalBytesUploaded += proposal.ResultSize

		// Apply the merge
		newRanges, err := applyMergeProposal(currentRanges, proposal)
		if err != nil {
			t.Fatalf("Failed to apply merge proposal: %v", err)
		}

		// Verify data integrity after merge (every 5 rounds for performance)
		if optimizationRounds%5 == 1 {
			newTotalDatapoints := calculateTotalDatapoints(newRanges)
			newTotalBytes := calculateTotalBytes(newRanges)

			if newTotalDatapoints != initialTotalDatapoints {
				t.Fatalf("Data loss detected! Initial datapoints: %d, after merge: %d",
					initialTotalDatapoints, newTotalDatapoints)
			}

			if newTotalBytes != initialTotalBytes {
				t.Fatalf("Byte count mismatch! Initial bytes: %d, after merge: %d",
					initialTotalBytes, newTotalBytes)
			}
		}

		currentRanges = newRanges

		// Log progress every round for first 10, then every 10 rounds
		logProgress := optimizationRounds <= 10 || optimizationRounds%10 == 0
		if logProgress {
			elapsed := time.Since(startTime)
			t.Logf("Progress: Round %d, %d ranges remaining, %.2f MB uploaded so far, elapsed: %v",
				optimizationRounds, len(currentRanges), float64(totalBytesUploaded)/(1024*1024), elapsed)
		}

		// Safety check to prevent infinite loops
		if optimizationRounds > 1000 {
			terminationReason = "max_rounds"
			t.Logf("Maximum rounds reached, stopping optimization")
			break
		}
	}

	// Final verification
	finalTotalDatapoints := calculateTotalDatapoints(currentRanges)
	finalTotalBytes := calculateTotalBytes(currentRanges)

	if finalTotalDatapoints != initialTotalDatapoints {
		t.Fatalf("Final data integrity check failed! Initial datapoints: %d, final: %d",
			initialTotalDatapoints, finalTotalDatapoints)
	}

	if finalTotalBytes != initialTotalBytes {
		t.Fatalf("Final byte integrity check failed! Initial bytes: %d, final: %d",
			initialTotalBytes, finalTotalBytes)
	}

	// Calculate compression ratio
	compressionRatio := float64(len(initialDataRanges)) / float64(len(currentRanges))
	elapsed := time.Since(startTime)

	// Report final results
	t.Logf("=== OPTIMIZATION COMPLETE ===")
	t.Logf("Termination reason: %s", terminationReason)
	t.Logf("Total execution time: %v", elapsed)
	t.Logf("Initial ranges: %d", len(initialDataRanges))
	t.Logf("Final ranges: %d", len(currentRanges))
	t.Logf("Compression ratio: %.2fx", compressionRatio)
	t.Logf("Optimization rounds: %d", optimizationRounds-1)
	t.Logf("Total bytes uploaded: %.2f MB", float64(totalBytesUploaded)/(1024*1024))
	t.Logf("Upload overhead ratio: %.2f%%", float64(totalBytesUploaded)/float64(initialTotalBytes)*100)

	// Verify final ranges are still properly ordered and continuous
	verifyRangesContinuity(t, currentRanges)

	// Assert proper termination
	if terminationReason == "timeout" {
		t.Logf("WARNING: Test terminated due to timeout, may not have reached optimal compression")
	} else if terminationReason == "max_rounds" {
		t.Fatalf("Test terminated due to max rounds limit - possible infinite loop")
	} else if terminationReason == "no_more_merges" {
		t.Logf("SUCCESS: Optimization terminated naturally - no more merges possible")
	}
}

// Test with 50,000 ranges to test the performance limits of the optimized algorithm
func TestOptimizing50kDataRanges(t *testing.T) {
	startTime := time.Now()

	const (
		numDataRanges      = 50000 // Large scale performance test
		datapointsPerRange = 32
		bytesPerDatapoint  = 250
		bytesPerRange      = datapointsPerRange * bytesPerDatapoint // 8000 bytes
	)

	t.Logf("Creating %d initial data ranges...", numDataRanges)

	// Create initial data ranges - each range contains 32 consecutive datapoints
	initialDataRanges := make([]client.DataRange, numDataRanges)
	for i := 0; i < numDataRanges; i++ {
		startKey := uint64(i * datapointsPerRange)
		endKey := startKey + datapointsPerRange - 1

		initialDataRanges[i] = client.DataRange{
			ObjectKey:       fmt.Sprintf("object_%d", i),
			MinDatapointKey: startKey,
			MaxDatapointKey: endKey,
			SizeBytes:       bytesPerRange,
		}
	}

	// Calculate initial totals for data integrity verification
	initialTotalDatapoints := calculateTotalDatapoints(initialDataRanges)
	initialTotalBytes := calculateTotalBytes(initialDataRanges)

	t.Logf("Initial state: %d ranges, %d total datapoints, %.2f GB total bytes",
		len(initialDataRanges), initialTotalDatapoints, float64(initialTotalBytes)/(1024*1024*1024))

	// Track optimization metrics
	var totalBytesUploaded uint64
	var optimizationRounds int
	var terminationReason string

	// Create optimizer and apply optimizations iteratively
	currentRanges := make([]client.DataRange, len(initialDataRanges))
	copy(currentRanges, initialDataRanges)

	for {
		optimizationRounds++
		roundStart := time.Now()
		optimizer := optimizer.NewDataRangeMergeOptimizer(currentRanges)

		// Get the next best merge proposal
		proposal := optimizer.ProposeNextMerge()
		if proposal == nil {
			terminationReason = "no_more_merges"
			t.Logf("No more optimization possible after %d rounds", optimizationRounds-1)
			break
		}

		roundTime := time.Since(roundStart)
		t.Logf("Round %d (took %v): Merging %d ranges -> %.2f GB (efficiency: %.2f)",
			optimizationRounds, roundTime, len(proposal.DataRangeIndices),
			float64(proposal.ResultSize)/(1024*1024*1024), proposal.Efficiency)

		// Track bytes that would be uploaded for this merge
		totalBytesUploaded += proposal.ResultSize

		// Apply the merge
		newRanges, err := applyMergeProposal(currentRanges, proposal)
		if err != nil {
			t.Fatalf("Failed to apply merge proposal: %v", err)
		}

		// Verify data integrity after merge (only on first round for performance)
		if optimizationRounds == 1 {
			newTotalDatapoints := calculateTotalDatapoints(newRanges)
			newTotalBytes := calculateTotalBytes(newRanges)

			if newTotalDatapoints != initialTotalDatapoints {
				t.Fatalf("Data loss detected! Initial datapoints: %d, after merge: %d",
					initialTotalDatapoints, newTotalDatapoints)
			}

			if newTotalBytes != initialTotalBytes {
				t.Fatalf("Byte count mismatch! Initial bytes: %d, after merge: %d",
					initialTotalBytes, newTotalBytes)
			}
		}

		currentRanges = newRanges

		// Log progress after each round
		elapsed := time.Since(startTime)
		t.Logf("Progress: Round %d, %d ranges remaining, %.2f GB uploaded so far, elapsed: %v",
			optimizationRounds, len(currentRanges), float64(totalBytesUploaded)/(1024*1024*1024), elapsed)

		// Safety check to prevent infinite loops
		if optimizationRounds > 500 {
			terminationReason = "max_rounds"
			t.Logf("Maximum rounds reached, stopping optimization")
			break
		}
	}

	// Final verification
	finalTotalDatapoints := calculateTotalDatapoints(currentRanges)
	finalTotalBytes := calculateTotalBytes(currentRanges)

	if finalTotalDatapoints != initialTotalDatapoints {
		t.Fatalf("Final data integrity check failed! Initial datapoints: %d, final: %d",
			initialTotalDatapoints, finalTotalDatapoints)
	}

	if finalTotalBytes != initialTotalBytes {
		t.Fatalf("Final byte integrity check failed! Initial bytes: %d, final: %d",
			initialTotalBytes, finalTotalBytes)
	}

	// Calculate compression ratio
	compressionRatio := float64(len(initialDataRanges)) / float64(len(currentRanges))
	elapsed := time.Since(startTime)

	// Report final results
	t.Logf("=== OPTIMIZATION COMPLETE ===")
	t.Logf("Termination reason: %s", terminationReason)
	t.Logf("Total execution time: %v", elapsed)
	t.Logf("Initial ranges: %d", len(initialDataRanges))
	t.Logf("Final ranges: %d", len(currentRanges))
	t.Logf("Compression ratio: %.2fx", compressionRatio)
	t.Logf("Optimization rounds: %d", optimizationRounds-1)
	t.Logf("Total bytes uploaded: %.2f GB", float64(totalBytesUploaded)/(1024*1024*1024))
	t.Logf("Upload overhead ratio: %.2f%%", float64(totalBytesUploaded)/float64(initialTotalBytes)*100)

	// Skip continuity verification for very large datasets (performance)
	if len(currentRanges) <= 10 {
		verifyRangesContinuity(t, currentRanges)
	} else {
		t.Logf("Skipping continuity verification due to large dataset size")
	}

	// Assert proper termination
	if terminationReason == "timeout" {
		t.Logf("WARNING: Test terminated due to timeout, may not have reached optimal compression")
	} else if terminationReason == "max_rounds" {
		t.Fatalf("Test terminated due to max rounds limit - possible infinite loop")
	} else if terminationReason == "no_more_merges" {
		t.Logf("SUCCESS: Optimization terminated naturally - no more merges possible")
	}
}

// Test with real beacon-attestation-rewards data from deployed datas3t server
func TestOptimizingRealBeaconAttestationRewards(t *testing.T) {
	startTime := time.Now()

	// Load real data from embedded JSON
	var realDataRanges []client.DataRange
	err := json.Unmarshal(beaconAttestationRewardsData, &realDataRanges)
	if err != nil {
		t.Fatalf("Failed to unmarshal embedded data: %v", err)
	}

	t.Logf("Loaded %d real data ranges from beacon-attestation-rewards dataset", len(realDataRanges))

	// Calculate initial totals for data integrity verification
	initialTotalDatapoints := calculateTotalDatapoints(realDataRanges)
	initialTotalBytes := calculateTotalBytes(realDataRanges)

	t.Logf("Initial state: %d ranges, %d total datapoints, %.2f GB total bytes",
		len(realDataRanges), initialTotalDatapoints, float64(initialTotalBytes)/(1024*1024*1024))

	// Analyze data characteristics
	analyzeDataCharacteristics(t, realDataRanges)

	// Track optimization metrics
	var totalBytesUploaded uint64
	var optimizationRounds int
	var terminationReason string

	// Create optimizer and apply optimizations iteratively
	currentRanges := make([]client.DataRange, len(realDataRanges))
	copy(currentRanges, realDataRanges)

	for {
		optimizationRounds++
		roundStart := time.Now()
		optimizer := optimizer.NewDataRangeMergeOptimizer(currentRanges)

		// Get the next best merge proposal
		proposal := optimizer.ProposeNextMerge()
		if proposal == nil {
			terminationReason = "no_more_merges"
			t.Logf("No more optimization possible after %d rounds", optimizationRounds-1)
			break
		}

		roundTime := time.Since(roundStart)
		t.Logf("Round %d (took %v): Merging %d ranges -> %.2f MB (efficiency: %.2f)",
			optimizationRounds, roundTime, len(proposal.DataRangeIndices),
			float64(proposal.ResultSize)/(1024*1024), proposal.Efficiency)

		// Track bytes that would be uploaded for this merge
		totalBytesUploaded += proposal.ResultSize

		// Apply the merge
		newRanges, err := applyMergeProposal(currentRanges, proposal)
		if err != nil {
			t.Fatalf("Failed to apply merge proposal: %v", err)
		}

		// Verify data integrity after merge (only on first round for performance)
		if optimizationRounds == 1 {
			newTotalDatapoints := calculateTotalDatapoints(newRanges)
			newTotalBytes := calculateTotalBytes(newRanges)

			if newTotalDatapoints != initialTotalDatapoints {
				t.Fatalf("Data loss detected! Initial datapoints: %d, after merge: %d",
					initialTotalDatapoints, newTotalDatapoints)
			}

			if newTotalBytes != initialTotalBytes {
				t.Fatalf("Byte count mismatch! Initial bytes: %d, after merge: %d",
					initialTotalBytes, newTotalBytes)
			}
		}

		currentRanges = newRanges

		// Log progress after each round
		elapsed := time.Since(startTime)
		t.Logf("Progress: Round %d, %d ranges remaining, %.2f GB uploaded so far, elapsed: %v",
			optimizationRounds, len(currentRanges), float64(totalBytesUploaded)/(1024*1024*1024), elapsed)

		// Safety check to prevent infinite loops
		if optimizationRounds > 300 {
			terminationReason = "max_rounds"
			t.Logf("Maximum rounds reached, stopping optimization")
			break
		}
	}

	// Final verification
	finalTotalDatapoints := calculateTotalDatapoints(currentRanges)
	finalTotalBytes := calculateTotalBytes(currentRanges)

	if finalTotalDatapoints != initialTotalDatapoints {
		t.Fatalf("Final data integrity check failed! Initial datapoints: %d, final: %d",
			initialTotalDatapoints, finalTotalDatapoints)
	}

	if finalTotalBytes != initialTotalBytes {
		t.Fatalf("Final byte integrity check failed! Initial bytes: %d, final: %d",
			initialTotalBytes, finalTotalBytes)
	}

	// Calculate compression ratio
	compressionRatio := float64(len(realDataRanges)) / float64(len(currentRanges))
	elapsed := time.Since(startTime)

	// Report final results
	t.Logf("=== REAL DATA OPTIMIZATION COMPLETE ===")
	t.Logf("Dataset: beacon-attestation-rewards")
	t.Logf("Termination reason: %s", terminationReason)
	t.Logf("Total execution time: %v", elapsed)
	t.Logf("Initial ranges: %d", len(realDataRanges))
	t.Logf("Final ranges: %d", len(currentRanges))
	t.Logf("Compression ratio: %.2fx", compressionRatio)
	t.Logf("Optimization rounds: %d", optimizationRounds-1)
	t.Logf("Total bytes uploaded: %.2f GB", float64(totalBytesUploaded)/(1024*1024*1024))
	t.Logf("Upload overhead ratio: %.2f%%", float64(totalBytesUploaded)/float64(initialTotalBytes)*100)

	// Skip continuity verification for real data (may have gaps)
	t.Logf("Skipping continuity verification for real dataset")

	// Assert proper termination
	if terminationReason == "timeout" {
		t.Logf("WARNING: Test terminated due to timeout, may not have reached optimal compression")
	} else if terminationReason == "max_rounds" {
		if compressionRatio < 1.5 {
			t.Fatalf("Test terminated due to max rounds with poor compression ratio: %.2fx", compressionRatio)
		} else {
			t.Logf("WARNING: Test terminated due to max rounds limit, but achieved good compression: %.2fx", compressionRatio)
		}
	} else if terminationReason == "no_more_merges" {
		t.Logf("SUCCESS: Optimization terminated naturally - no more merges possible")
	}
}

// analyzeDataCharacteristics analyzes the characteristics of the real data
func analyzeDataCharacteristics(t *testing.T, ranges []client.DataRange) {
	if len(ranges) == 0 {
		return
	}

	var totalSize uint64
	var totalDatapoints uint64
	var consecutiveCount int
	var gapCount int
	var totalGapSize uint64

	rangeSizes := make([]uint64, len(ranges))
	datapointCounts := make([]uint64, len(ranges))

	for i, dr := range ranges {
		totalSize += dr.SizeBytes
		totalDatapoints += dr.NumberOfDatapoints()
		rangeSizes[i] = dr.SizeBytes
		datapointCounts[i] = dr.NumberOfDatapoints()

		// Check for gaps (if not the first range)
		if i > 0 {
			prevRange := ranges[i-1]
			if dr.MinDatapointKey == prevRange.MaxDatapointKey+1 {
				consecutiveCount++
			} else if dr.MinDatapointKey > prevRange.MaxDatapointKey+1 {
				gapCount++
				totalGapSize += dr.MinDatapointKey - prevRange.MaxDatapointKey - 1
			}
		}
	}

	// Calculate statistics
	minSize := rangeSizes[0]
	maxSize := rangeSizes[0]
	minDatapoints := datapointCounts[0]
	maxDatapoints := datapointCounts[0]

	for _, size := range rangeSizes {
		if size < minSize {
			minSize = size
		}
		if size > maxSize {
			maxSize = size
		}
	}

	for _, count := range datapointCounts {
		if count < minDatapoints {
			minDatapoints = count
		}
		if count > maxDatapoints {
			maxDatapoints = count
		}
	}

	t.Logf("=== DATA CHARACTERISTICS ANALYSIS ===")
	t.Logf("Range size stats: min=%.2f KB, max=%.2f MB, avg=%.2f KB",
		float64(minSize)/1024, float64(maxSize)/(1024*1024), float64(totalSize)/float64(len(ranges))/1024)
	t.Logf("Datapoints per range: min=%d, max=%d, avg=%.1f",
		minDatapoints, maxDatapoints, float64(totalDatapoints)/float64(len(ranges)))
	t.Logf("Consecutive ranges: %d/%d (%.1f%%)",
		consecutiveCount, len(ranges)-1, float64(consecutiveCount)/float64(len(ranges)-1)*100)
	t.Logf("Gaps in data: %d gaps totaling %d missing datapoints",
		gapCount, totalGapSize)

	if len(ranges) > 0 {
		coveragePercent := float64(totalDatapoints) / float64(ranges[len(ranges)-1].MaxDatapointKey-ranges[0].MinDatapointKey+1) * 100
		t.Logf("Data coverage: %.1f%% of total range [%d-%d]",
			coveragePercent, ranges[0].MinDatapointKey, ranges[len(ranges)-1].MaxDatapointKey)
	}
}

// calculateTotalDatapoints calculates the total number of datapoints across all ranges
func calculateTotalDatapoints(ranges []client.DataRange) uint64 {
	var total uint64
	for _, r := range ranges {
		total += r.NumberOfDatapoints()
	}
	return total
}

// calculateTotalBytes calculates the total bytes across all ranges
func calculateTotalBytes(ranges []client.DataRange) uint64 {
	var total uint64
	for _, r := range ranges {
		total += r.SizeBytes
	}
	return total
}

// applyMergeProposal applies a merge proposal to the current set of ranges
func applyMergeProposal(currentRanges []client.DataRange, proposal *optimizer.MergeProposal) ([]client.DataRange, error) {
	if len(proposal.DataRangeIndices) < 2 {
		return nil, fmt.Errorf("merge proposal must include at least 2 ranges")
	}

	// Extract ranges to be merged
	rangesToMerge := make([]client.DataRange, len(proposal.DataRangeIndices))
	for i, idx := range proposal.DataRangeIndices {
		if idx < 0 || idx >= len(currentRanges) {
			return nil, fmt.Errorf("invalid range index: %d", idx)
		}
		rangesToMerge[i] = currentRanges[idx]
	}

	// Create merged range
	mergedObjectKey := fmt.Sprintf("merged_%d_ranges", len(rangesToMerge))
	mergedRange := optimizer.NewMergedDataRange(mergedObjectKey, rangesToMerge)

	// Create new slice with merged range, excluding the original ranges
	var newRanges []client.DataRange
	mergedIndicesSet := make(map[int]bool)
	for _, idx := range proposal.DataRangeIndices {
		mergedIndicesSet[idx] = true
	}

	// Add all ranges except the ones being merged
	for i, r := range currentRanges {
		if !mergedIndicesSet[i] {
			newRanges = append(newRanges, r)
		}
	}

	// Add the merged range
	newRanges = append(newRanges, mergedRange)

	return newRanges, nil
}

// verifyRangesContinuity verifies that the final ranges maintain data continuity
func verifyRangesContinuity(t *testing.T, ranges []client.DataRange) {
	if len(ranges) == 0 {
		return
	}

	// Create a map to track all covered datapoints
	coveredPoints := make(map[uint64]bool)
	var minDatapoint, maxDatapoint uint64
	minDatapoint = ^uint64(0) // Max uint64 value

	for _, r := range ranges {
		if r.MinDatapointKey < minDatapoint {
			minDatapoint = r.MinDatapointKey
		}
		if r.MaxDatapointKey > maxDatapoint {
			maxDatapoint = r.MaxDatapointKey
		}

		for dp := r.MinDatapointKey; dp <= r.MaxDatapointKey; dp++ {
			if coveredPoints[dp] {
				t.Errorf("Datapoint %d is covered by multiple ranges", dp)
			}
			coveredPoints[dp] = true
		}
	}

	// Verify we have a continuous sequence from min to max datapoint
	for dp := minDatapoint; dp <= maxDatapoint; dp++ {
		if !coveredPoints[dp] {
			t.Errorf("Datapoint %d is not covered by any range", dp)
			break // Don't spam too many errors
		}
	}

	t.Logf("Continuity verification passed: %d datapoints from %d to %d",
		maxDatapoint-minDatapoint+1, minDatapoint, maxDatapoint)
}
