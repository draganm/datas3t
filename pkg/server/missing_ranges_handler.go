package server

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// Range represents a range of datapoint keys
type Range struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

// MissingRangesResponse represents the response for the missing ranges endpoint
type MissingRangesResponse struct {
	FirstDatapoint *uint64 `json:"first_datapoint"`
	LastDatapoint  *uint64 `json:"last_datapoint"`
	MissingRanges  []Range `json:"missing_ranges"`
}

// HandleGetMissingRanges handles GET requests to calculate missing datapoint ranges for a dataset
func (s *Server) HandleGetMissingRanges(w http.ResponseWriter, r *http.Request) {
	datasetID := r.PathValue("id")
	store := sqlitestore.New(s.DB)

	// Check if dataset exists
	exists, err := store.DatasetExists(r.Context(), datasetID)
	if err != nil {
		s.logger.Error("failed to check if dataset exists", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}

	// Get the first and last datapoint
	firstAndLast, err := store.GetFirstAndLastDatapoint(r.Context(), datasetID)
	if err != nil {
		s.logger.Error("failed to get first and last datapoint", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// If there are no datapoints, return empty response with nulls
	if !firstAndLast.FirstDatapointKey.Valid || !firstAndLast.LastDatapointKey.Valid {
		response := MissingRangesResponse{
			FirstDatapoint: nil,
			LastDatapoint:  nil,
			MissingRanges:  []Range{},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	// Get all dataranges sorted by min_datapoint_key
	ranges, err := store.GetDatarangesForMissingRanges(r.Context(), datasetID)
	if err != nil {
		s.logger.Error("failed to get dataranges", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Calculate missing ranges
	var missingRanges []Range

	// Check if there's a gap at the beginning
	if len(ranges) > 0 && uint64(ranges[0].MinDatapointKey) > uint64(firstAndLast.FirstDatapointKey.Int64) {
		missingRanges = append(missingRanges, Range{
			Start: uint64(firstAndLast.FirstDatapointKey.Int64),
			End:   uint64(ranges[0].MinDatapointKey) - 1,
		})
	}

	// Check for gaps between ranges
	for i := 0; i < len(ranges)-1; i++ {
		current := ranges[i]
		next := ranges[i+1]

		// If there's a gap between the current range's end and the next range's start
		if uint64(current.MaxDatapointKey)+1 < uint64(next.MinDatapointKey) {
			missingRanges = append(missingRanges, Range{
				Start: uint64(current.MaxDatapointKey) + 1,
				End:   uint64(next.MinDatapointKey) - 1,
			})
		}
	}

	// Check if there's a gap at the end
	if len(ranges) > 0 && uint64(ranges[len(ranges)-1].MaxDatapointKey) < uint64(firstAndLast.LastDatapointKey.Int64) {
		missingRanges = append(missingRanges, Range{
			Start: uint64(ranges[len(ranges)-1].MaxDatapointKey) + 1,
			End:   uint64(firstAndLast.LastDatapointKey.Int64),
		})
	}

	// Convert to pointers for JSON response
	first := uint64(firstAndLast.FirstDatapointKey.Int64)
	last := uint64(firstAndLast.LastDatapointKey.Int64)

	// Prepare response
	response := MissingRangesResponse{
		FirstDatapoint: &first,
		LastDatapoint:  &last,
		MissingRanges:  missingRanges,
	}

	// Return response as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
