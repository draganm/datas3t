package restore

import (
	"strings"
	"testing"
)

func TestFilterOverlappingDataranges(t *testing.T) {
	tests := []struct {
		name          string
		dataranges    []string
		wantKept      []DatarangeInfo
		wantDiscarded []DatarangeInfo
		wantError     bool
		errorContains string
	}{
		{
			name: "No overlapping ranges",
			dataranges: []string{
				"dataset/test/datapoints/00000000000000000001-00000000000000000002.tar",
				"dataset/test/datapoints/00000000000000000003-00000000000000000004.tar",
				"dataset/test/datapoints/00000000000000000005-00000000000000000006.tar",
			},
			wantKept: []DatarangeInfo{
				{ObjectKey: "dataset/test/datapoints/00000000000000000001-00000000000000000002.tar", MinKey: 1, MaxKey: 2, Span: 2},
				{ObjectKey: "dataset/test/datapoints/00000000000000000003-00000000000000000004.tar", MinKey: 3, MaxKey: 4, Span: 2},
				{ObjectKey: "dataset/test/datapoints/00000000000000000005-00000000000000000006.tar", MinKey: 5, MaxKey: 6, Span: 2},
			},
			wantDiscarded: []DatarangeInfo{},
			wantError:     false,
		},
		{
			name: "Aggregated range fully covers original ranges",
			dataranges: []string{
				"dataset/test/datapoints/00000000000000000001-00000000000000000006.tar", // Aggregated range
				"dataset/test/datapoints/00000000000000000001-00000000000000000002.tar", // Original range 1
				"dataset/test/datapoints/00000000000000000003-00000000000000000004.tar", // Original range 2
				"dataset/test/datapoints/00000000000000000005-00000000000000000006.tar", // Original range 3
			},
			wantKept: []DatarangeInfo{
				{ObjectKey: "dataset/test/datapoints/00000000000000000001-00000000000000000006.tar", MinKey: 1, MaxKey: 6, Span: 6},
			},
			wantDiscarded: []DatarangeInfo{
				{ObjectKey: "dataset/test/datapoints/00000000000000000001-00000000000000000002.tar", MinKey: 1, MaxKey: 2, Span: 2},
				{ObjectKey: "dataset/test/datapoints/00000000000000000003-00000000000000000004.tar", MinKey: 3, MaxKey: 4, Span: 2},
				{ObjectKey: "dataset/test/datapoints/00000000000000000005-00000000000000000006.tar", MinKey: 5, MaxKey: 6, Span: 2},
			},
			wantError: false,
		},
		{
			name: "Partial overlap - Error expected",
			dataranges: []string{
				"dataset/test/datapoints/00000000000000000001-00000000000000000004.tar", // Covers 1-4
				"dataset/test/datapoints/00000000000000000003-00000000000000000006.tar", // Covers 3-6
			},
			wantKept:      nil,
			wantDiscarded: nil,
			wantError:     true,
			errorContains: "partial overlap detected",
		},
		{
			name: "Partial overlap with multiple ranges - Error expected",
			dataranges: []string{
				"dataset/test/datapoints/00000000000000000001-00000000000000000003.tar",
				"dataset/test/datapoints/00000000000000000002-00000000000000000004.tar",
				"dataset/test/datapoints/00000000000000000005-00000000000000000006.tar",
			},
			wantKept:      nil,
			wantDiscarded: nil,
			wantError:     true,
			errorContains: "partial overlap detected",
		},
		{
			name: "Invalid dataranges are ignored",
			dataranges: []string{
				"dataset/test/datapoints/00000000000000000001-00000000000000000006.tar",
				"invalid-key-format",
				"dataset/test/invalid-format/key.tar",
			},
			wantKept: []DatarangeInfo{
				{ObjectKey: "dataset/test/datapoints/00000000000000000001-00000000000000000006.tar", MinKey: 1, MaxKey: 6, Span: 6},
			},
			wantDiscarded: []DatarangeInfo{},
			wantError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKept, gotDiscarded, err := filterOverlappingDataranges(tt.dataranges)

			// Check error status
			if (err != nil) != tt.wantError {
				t.Errorf("filterOverlappingDataranges() error = %v, wantError %v", err, tt.wantError)
				return
			}

			// If we expect an error, check that the error message contains what we expect
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("filterOverlappingDataranges() error = %v, want error containing %q", err, tt.errorContains)
				}
				return // No need to check results if we expected an error
			}

			// Use a better comparison for DatarangeInfo slices
			compareDatarangeSlices := func(got, want []DatarangeInfo) bool {
				if len(got) != len(want) {
					return false
				}

				// Create maps by ObjectKey for easier comparison
				gotMap := make(map[string]DatarangeInfo)
				wantMap := make(map[string]DatarangeInfo)

				for _, dr := range got {
					gotMap[dr.ObjectKey] = dr
				}

				for _, dr := range want {
					wantMap[dr.ObjectKey] = dr
				}

				// Compare the maps
				for key, wantDR := range wantMap {
					gotDR, ok := gotMap[key]
					if !ok || gotDR.MinKey != wantDR.MinKey ||
						gotDR.MaxKey != wantDR.MaxKey || gotDR.Span != wantDR.Span {
						return false
					}
				}

				return true
			}

			if !compareDatarangeSlices(gotKept, tt.wantKept) {
				t.Errorf("filterOverlappingDataranges() kept = %v, want %v", gotKept, tt.wantKept)
			}

			if !compareDatarangeSlices(gotDiscarded, tt.wantDiscarded) {
				t.Errorf("filterOverlappingDataranges() discarded = %v, want %v", gotDiscarded, tt.wantDiscarded)
			}
		})
	}
}
