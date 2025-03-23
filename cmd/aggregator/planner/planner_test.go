package planner

import (
	"math"
	"testing"

	"github.com/draganm/datas3t/pkg/client"
	"github.com/stretchr/testify/assert"
)

func TestCreatePlans(t *testing.T) {
	tests := []struct {
		name       string
		dataranges []client.DataRange
		targetSize int64
		expected   []AggregationPlan
	}{
		{
			name:       "empty dataranges",
			dataranges: []client.DataRange{},
			targetSize: 1024 * 1024 * 1024, // 1 GB
			expected:   []AggregationPlan{},
		},
		{
			name: "single datarange",
			dataranges: []client.DataRange{
				{
					MinDatapointKey: 1,
					MaxDatapointKey: 100,
					SizeBytes:       50 * 1024 * 1024,
				},
			},
			targetSize: 1024 * 1024 * 1024, // 1 GB
			expected:   []AggregationPlan{},
		},
		{
			name: "two adjacent small dataranges in same tier that don't promote",
			dataranges: []client.DataRange{
				{
					MinDatapointKey: 1,
					MaxDatapointKey: 100,
					SizeBytes:       5 * 1024, // 5 KB (Tier 0: 1KB-10KB)
				},
				{
					MinDatapointKey: 101,
					MaxDatapointKey: 200,
					SizeBytes:       2 * 1024, // 2 KB (also Tier 0)
				},
			},
			targetSize: 1024 * 1024 * 1024,  // 1 GB
			expected:   []AggregationPlan{}, // Should not merge as total size (7KB) doesn't promote to higher tier
		},
		{
			name: "two adjacent dataranges that promote to higher tier",
			dataranges: []client.DataRange{
				{
					MinDatapointKey: 1,
					MaxDatapointKey: 100,
					SizeBytes:       9 * 1024, // 9 KB (Tier 0: 1KB-10KB)
				},
				{
					MinDatapointKey: 101,
					MaxDatapointKey: 200,
					SizeBytes:       8 * 1024, // 8 KB (also Tier 0)
				},
			},
			targetSize: 1024 * 1024 * 1024, // 1 GB
			expected: []AggregationPlan{
				{
					StartKey: 1,
					EndKey:   200,
					Ranges: []client.DataRange{
						{
							MinDatapointKey: 1,
							MaxDatapointKey: 100,
							SizeBytes:       9 * 1024,
						},
						{
							MinDatapointKey: 101,
							MaxDatapointKey: 200,
							SizeBytes:       8 * 1024,
						},
					},
				},
			}, // Should merge as total size (17KB) promotes to Tier 1 (10KB-100KB)
		},
		{
			name: "non-adjacent ranges should not be combined",
			dataranges: []client.DataRange{
				{
					MinDatapointKey: 1,
					MaxDatapointKey: 100,
					SizeBytes:       9 * 1024, // 9 KB
				},
				{
					MinDatapointKey: 201, // Gap in key sequence
					MaxDatapointKey: 300,
					SizeBytes:       8 * 1024, // 8 KB
				},
			},
			targetSize: 1024 * 1024 * 1024,
			expected:   []AggregationPlan{}, // Should not create any plans
		},
		{
			name: "multiple adjacent ranges that promote to higher tier",
			dataranges: []client.DataRange{
				{
					MinDatapointKey: 1,
					MaxDatapointKey: 100,
					SizeBytes:       30 * 1024, // 30 KB (Tier 1: 10KB-100KB)
				},
				{
					MinDatapointKey: 101,
					MaxDatapointKey: 200,
					SizeBytes:       25 * 1024, // 25 KB (Tier 1)
				},
				{
					MinDatapointKey: 201,
					MaxDatapointKey: 300,
					SizeBytes:       35 * 1024, // 35 KB (Tier 1)
				},
				{
					MinDatapointKey: 301,
					MaxDatapointKey: 400,
					SizeBytes:       20 * 1024, // 20 KB (Tier 1)
				},
			},
			targetSize: 1024 * 1024 * 1024, // 1 GB
			expected: []AggregationPlan{
				{
					StartKey: 1,
					EndKey:   400,
					Ranges: []client.DataRange{
						{MinDatapointKey: 1, MaxDatapointKey: 100, SizeBytes: 30 * 1024},
						{MinDatapointKey: 101, MaxDatapointKey: 200, SizeBytes: 25 * 1024},
						{MinDatapointKey: 201, MaxDatapointKey: 300, SizeBytes: 35 * 1024},
						{MinDatapointKey: 301, MaxDatapointKey: 400, SizeBytes: 20 * 1024},
					},
				},
			}, // Should merge as total size (110KB) promotes to Tier 2 (100KB-1MB)
		},
		{
			name: "ranges in different tiers that promote when combined",
			dataranges: []client.DataRange{
				{
					MinDatapointKey: 1,
					MaxDatapointKey: 100,
					SizeBytes:       80 * 1024, // 80 KB (Tier 1: 10KB-100KB)
				},
				{
					MinDatapointKey: 101,
					MaxDatapointKey: 200,
					SizeBytes:       150 * 1024, // 150 KB (Tier 2: 100KB-1MB)
				},
				{
					MinDatapointKey: 201,
					MaxDatapointKey: 300,
					SizeBytes:       300 * 1024, // 300 KB (Tier 2)
				},
			},
			targetSize: 1024 * 1024 * 1024,
			expected: []AggregationPlan{
				{
					StartKey: 1,
					EndKey:   300,
					Ranges: []client.DataRange{
						{MinDatapointKey: 1, MaxDatapointKey: 100, SizeBytes: 80 * 1024},
						{MinDatapointKey: 101, MaxDatapointKey: 200, SizeBytes: 150 * 1024},
						{MinDatapointKey: 201, MaxDatapointKey: 300, SizeBytes: 300 * 1024},
					},
				},
			}, // Should merge as cross-tier since total (530KB) is still in Tier 2 but larger than any individual range
		},
		{
			name: "ranges in highest tier shouldn't be merged",
			dataranges: []client.DataRange{
				{
					MinDatapointKey: 1,
					MaxDatapointKey: 100,
					SizeBytes:       2 * 1024 * 1024 * 1024, // 2 GB (Highest tier)
				},
				{
					MinDatapointKey: 101,
					MaxDatapointKey: 200,
					SizeBytes:       3 * 1024 * 1024 * 1024, // 3 GB (Highest tier)
				},
			},
			targetSize: 1024 * 1024 * 1024,  // 1 GB
			expected:   []AggregationPlan{}, // No merging in highest tier
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Pass nil logger for tests
			actual := CreatePlans(tt.dataranges, nil, tt.targetSize)

			assert.Equal(t, len(tt.expected), len(actual), "number of plans should match")

			// If no plans expected, skip further comparison
			if len(tt.expected) == 0 {
				return
			}

			// Sort plans and ranges to ensure consistent comparison
			for i := range tt.expected {
				if i < len(actual) {
					assert.Equal(t, tt.expected[i].StartKey, actual[i].StartKey, "StartKey should match for plan %d", i)
					assert.Equal(t, tt.expected[i].EndKey, actual[i].EndKey, "EndKey should match for plan %d", i)
					assert.Equal(t, len(tt.expected[i].Ranges), len(actual[i].Ranges), "number of ranges should match for plan %d", i)

					// Verify range keys match
					if len(tt.expected[i].Ranges) == len(actual[i].Ranges) {
						for j := range tt.expected[i].Ranges {
							assert.Equal(t, tt.expected[i].Ranges[j].MinDatapointKey, actual[i].Ranges[j].MinDatapointKey,
								"Range %d MinDatapointKey should match in plan %d", j, i)
							assert.Equal(t, tt.expected[i].Ranges[j].MaxDatapointKey, actual[i].Ranges[j].MaxDatapointKey,
								"Range %d MaxDatapointKey should match in plan %d", j, i)
						}
					}
				}
			}
		})
	}
}

func TestInitializeTiers(t *testing.T) {
	tests := []struct {
		name       string
		targetSize int64
		expected   int // expected number of tiers
	}{
		{
			name:       "1GB target",
			targetSize: 1024 * 1024 * 1024, // 1 GB
			expected:   7,                  // 1KB, 10KB, 100KB, 1MB, 10MB, 100MB, 1GB+
		},
		{
			name:       "100MB target",
			targetSize: 100 * 1024 * 1024, // 100 MB
			expected:   6,                 // 1KB, 10KB, 100KB, 1MB, 10MB, 100MB+
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tiers := initializeTiers(tt.targetSize)
			assert.Equal(t, tt.expected, len(tiers), "number of tiers should match")

			// Verify tier sizes
			for i := 0; i < len(tiers)-1; i++ {
				assert.Equal(t, tiers[i].maxSize, tiers[i+1].minSize, "Tier boundaries should be contiguous")
			}

			// Verify last tier extends to max int64
			assert.Equal(t, int64(math.MaxInt64), tiers[len(tiers)-1].maxSize, "Last tier should extend to MaxInt64")
		})
	}
}

func TestGetTierForSize(t *testing.T) {
	// Create test tiers
	tiers := []tier{
		{minSize: 0, maxSize: 10240},                   // 0B - 10KB
		{minSize: 10240, maxSize: 102400},              // 10KB - 100KB
		{minSize: 102400, maxSize: 1024 * 1024},        // 100KB - 1MB
		{minSize: 1024 * 1024, maxSize: math.MaxInt64}, // 1MB+
	}

	tests := []struct {
		name      string
		sizeBytes int64
		expected  int // expected tier index
	}{
		{"0 bytes (smallest possible)", 0, 0},
		{"500 bytes (below smallest tier's max)", 500, 0},
		{"1KB (still in smallest tier)", 1024, 0},
		{"5KB (in smallest tier)", 5 * 1024, 0},
		{"10KB (second tier lower bound)", 10 * 1024, 1},
		{"50KB (in second tier)", 50 * 1024, 1},
		{"100KB (third tier lower bound)", 100 * 1024, 2},
		{"500KB (in third tier)", 500 * 1024, 2},
		{"1MB (highest tier lower bound)", 1024 * 1024, 3},
		{"10MB (in highest tier)", 10 * 1024 * 1024, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tierIndex := getTierForSize(tt.sizeBytes, tiers)
			assert.Equal(t, tt.expected, tierIndex, "tier index should match")
		})
	}
}
