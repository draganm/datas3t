package planner_test

import (
	"testing"

	"github.com/draganm/datas3t/cmd/aggregator/planner"
	"github.com/draganm/datas3t/pkg/client"
	"github.com/stretchr/testify/assert"
)

func TestAggregationOperationSizeBytes(t *testing.T) {
	tests := []struct {
		name     string
		op       planner.AggregationOperation
		expected uint64
	}{
		{
			name: "empty operation",
			op:   planner.AggregationOperation{},
		},
		{
			name: "single datarange",
			op: planner.AggregationOperation{
				{SizeBytes: 100},
			},
			expected: 100,
		},
		{
			name: "multiple dataranges",
			op: planner.AggregationOperation{
				{SizeBytes: 100},
				{SizeBytes: 200},
				{SizeBytes: 300},
			},
			expected: 600,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.op.SizeBytes())
		})
	}
}

func TestAggregationOperationStartKey(t *testing.T) {
	tests := []struct {
		name     string
		op       planner.AggregationOperation
		expected uint64
	}{
		{
			name: "single datarange",
			op: planner.AggregationOperation{
				{MinDatapointKey: 100},
			},
			expected: 100,
		},
		{
			name: "multiple dataranges",
			op: planner.AggregationOperation{
				{MinDatapointKey: 100},
				{MinDatapointKey: 200},
				{MinDatapointKey: 300},
			},
			expected: 100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.op.StartKey())
		})
	}
}

func TestAggregationOperationEndKey(t *testing.T) {
	tests := []struct {
		name     string
		op       planner.AggregationOperation
		expected uint64
	}{
		{
			name: "single datarange",
			op: planner.AggregationOperation{
				{MaxDatapointKey: 100},
			},
			expected: 100,
		},
		{
			name: "multiple dataranges",
			op: planner.AggregationOperation{
				{MaxDatapointKey: 100},
				{MaxDatapointKey: 200},
				{MaxDatapointKey: 300},
			},
			expected: 300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.op.EndKey())
		})
	}
}

func TestAggregationOperationLevel(t *testing.T) {
	tests := []struct {
		name     string
		op       planner.AggregationOperation
		expected int
	}{
		{
			name: "level 0 - under 10MB",
			op: planner.AggregationOperation{
				{SizeBytes: 5 * 1024 * 1024}, // 5MB
			},
			expected: 0,
		},
		{
			name: "level 1 - between 10MB and 1GB",
			op: planner.AggregationOperation{
				{SizeBytes: 500 * 1024 * 1024}, // 500MB
			},
			expected: 1,
		},
		{
			name: "level 2 - between 1GB and 100GB",
			op: planner.AggregationOperation{
				{SizeBytes: 50 * 1024 * 1024 * 1024}, // 50GB
			},
			expected: 2,
		},
		{
			name: "level 3 - over 100GB",
			op: planner.AggregationOperation{
				{SizeBytes: 200 * 1024 * 1024 * 1024}, // 200GB
			},
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.op.Level())
		})
	}
}

func TestDatarangeLevel(t *testing.T) {
	tests := []struct {
		name     string
		dr       client.DataRange
		expected int
	}{
		{
			name:     "level 0 - under 10MB",
			dr:       client.DataRange{SizeBytes: 5 * 1024 * 1024}, // 5MB
			expected: 0,
		},
		{
			name:     "level 1 - between 10MB and 1GB",
			dr:       client.DataRange{SizeBytes: 500 * 1024 * 1024}, // 500MB
			expected: 1,
		},
		{
			name:     "level 2 - between 1GB and 100GB",
			dr:       client.DataRange{SizeBytes: 50 * 1024 * 1024 * 1024}, // 50GB
			expected: 2,
		},
		{
			name:     "level 3 - over 100GB",
			dr:       client.DataRange{SizeBytes: 200 * 1024 * 1024 * 1024}, // 200GB
			expected: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, planner.DatarangeLevel(tt.dr))
		})
	}
}

func TestCreatePlans(t *testing.T) {
	tests := []struct {
		name          string
		dataranges    []client.DataRange
		expectedPlans []planner.AggregationOperation
	}{
		{
			name:          "empty dataranges",
			dataranges:    []client.DataRange{},
			expectedPlans: []planner.AggregationOperation{},
		},
		{
			name: "single datarange",
			dataranges: []client.DataRange{
				{SizeBytes: 100},
			},
			expectedPlans: []planner.AggregationOperation{},
		},
		{
			name: "multiple dataranges - same level",
			dataranges: []client.DataRange{
				{MinDatapointKey: 1, MaxDatapointKey: 10, SizeBytes: 5 * 1024 * 1024},  // 5MB
				{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 6 * 1024 * 1024}, // 6MB
				{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 7 * 1024 * 1024}, // 7MB
			},
			expectedPlans: []planner.AggregationOperation{
				{
					{MinDatapointKey: 1, MaxDatapointKey: 10, SizeBytes: 5 * 1024 * 1024},
					{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 6 * 1024 * 1024},
					{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 7 * 1024 * 1024},
				},
			},
		},
		{
			name: "multiple dataranges - increasing levels",
			dataranges: []client.DataRange{
				{MinDatapointKey: 1, MaxDatapointKey: 10, SizeBytes: 5 * 1024 * 1024},          // 5MB
				{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 500 * 1024 * 1024},       // 500MB
				{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 50 * 1024 * 1024 * 1024}, // 50GB
			},
			expectedPlans: []planner.AggregationOperation{
				{
					{MinDatapointKey: 1, MaxDatapointKey: 10, SizeBytes: 5 * 1024 * 1024},
					{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 500 * 1024 * 1024},
					{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 50 * 1024 * 1024 * 1024}, // 50GB
				},
			},
		},
		{
			name: "below level 1",
			dataranges: []client.DataRange{
				{MinDatapointKey: 1, MaxDatapointKey: 10, SizeBytes: 1 * 1024 * 1024},  // 1MB
				{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 1 * 1024 * 1024}, // 1MB
				{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 2 * 1024 * 1024}, // 2GB
			},
			expectedPlans: []planner.AggregationOperation{},
		},
		{
			name: "below level 1",
			dataranges: []client.DataRange{
				{MinDatapointKey: 1, MaxDatapointKey: 10, SizeBytes: 1 * 1024 * 1024},  // 1MB
				{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 1 * 1024 * 1024}, // 1MB
				{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 2 * 1024 * 1024}, // 2GB
			},
			expectedPlans: []planner.AggregationOperation{},
		},

		{
			name: "skip top level",
			dataranges: []client.DataRange{
				{MinDatapointKey: 1, MaxDatapointKey: 10, SizeBytes: 101 * 1024 * 1024 * 1024}, // 101GB
				{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 1 * 1024 * 1024},         // 1MB
				{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 9 * 1024 * 1024},         // 2GB
			},
			expectedPlans: []planner.AggregationOperation{
				{
					{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 1 * 1024 * 1024}, // 1MB
					{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 9 * 1024 * 1024}, // 9GB
				},
			},
		},

		{
			name: "don't change anything if no level change",
			dataranges: []client.DataRange{
				{MinDatapointKey: 1, MaxDatapointKey: 10, SizeBytes: 101 * 1024 * 1024 * 1024}, // 101GB
				{MinDatapointKey: 11, MaxDatapointKey: 20, SizeBytes: 1 * 1024 * 1024},         // 1MB
				{MinDatapointKey: 21, MaxDatapointKey: 30, SizeBytes: 8 * 1024 * 1024},         // 8GB
			},
			expectedPlans: []planner.AggregationOperation{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plans := planner.CreatePlan(tt.dataranges)
			assert.Equal(t, tt.expectedPlans, plans)
		})
	}
}

func TestCreatePlansWithManySmallDatasets(t *testing.T) {
	// Create 1000 datasets, each with exactly one datapoint
	dataranges := make([]client.DataRange, 1000)
	for i := range 1000 {
		dataranges[i] = client.DataRange{
			MinDatapointKey: uint64(i),
			MaxDatapointKey: uint64(i + 1),
			SizeBytes:       1024, // 1KB each
		}
	}

	// Create expected plan with all 1000 datasets in one operation
	expectedOperation := make(planner.AggregationOperation, 1000)
	for i := 0; i < 1000; i++ {
		expectedOperation[i] = client.DataRange{
			MinDatapointKey: uint64(i),
			MaxDatapointKey: uint64(i + 1),
			SizeBytes:       1024,
		}
	}

	plans := planner.CreatePlan(dataranges)
	assert.Equal(t, []planner.AggregationOperation{expectedOperation}, plans)

	// Verify the aggregation operation properties
	assert.Equal(t, uint64(1000), expectedOperation.NumberOfDatapoints())
	assert.Equal(t, uint64(1024*1000), expectedOperation.SizeBytes())
	assert.Equal(t, uint64(0), expectedOperation.StartKey())
	assert.Equal(t, uint64(1000), expectedOperation.EndKey())
}
