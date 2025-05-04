package server

import (
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"time"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// WaitRequest represents the request structure for the wait endpoint
type WaitRequest struct {
	Datasets map[string]uint64 `json:"datasets"` // Map of dataset name to desired datapoint
}

// WaitResponse represents the response structure for the wait endpoint
type WaitResponse struct {
	Datasets map[string]uint64 `json:"datasets"` // Map of dataset name to max datapoint
}

// HandleWaitDatasets handles POST requests to wait for datasets to have specific datapoints
func (s *Server) HandleWaitDatasets(w http.ResponseWriter, r *http.Request) {

	// Parse request body
	var req WaitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.logger.Error("failed to decode request body", "error", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Check if any datasets are missing
	store := sqlitestore.New(s.DB)
	// missingDatasets := []string{}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	// // Return error if any datasets are missing
	// if len(missingDatasets) > 0 {
	// 	response := map[string]interface{}{
	// 		"error":           "One or more datasets do not exist",
	// 		"missingDatasets": missingDatasets,
	// 	}
	// 	w.Header().Set("Content-Type", "application/json")
	// 	w.WriteHeader(http.StatusBadRequest)
	// 	json.NewEncoder(w).Encode(response)
	// 	return
	// }

	// Keep checking until all conditions are met or timeout is reached
	datasetMaxPoints := make(map[string]uint64)
	for {
		allConditionsMet := true

		datapoints, err := store.GetLargestDatapointForDatasets(ctx, slices.Collect(maps.Keys(req.Datasets)))
		if err == context.DeadlineExceeded {
			break
		}
		if err != nil {
			s.logger.Error("failed to get largest datapoint", "error", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		for _, datapoint := range datapoints {
			datasetMaxPoints[datapoint.DatasetName] = uint64(datapoint.LargestDatapointKey)
		}

		missingDatasets := []string{}

		for datasetName, desiredDatapoint := range req.Datasets {

			maxPoint, ok := datasetMaxPoints[datasetName]

			if !ok {
				missingDatasets = append(missingDatasets, datasetName)
				continue
			}

			if maxPoint < desiredDatapoint {
				allConditionsMet = false
				break
			}
		}

		if len(missingDatasets) > 0 {
			response := map[string]interface{}{
				"error":           "One or more datasets do not exist",
				"missingDatasets": missingDatasets,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(response)
			return
		}

		// If all conditions are met, return success
		if allConditionsMet {
			response := WaitResponse{
				Datasets: datasetMaxPoints,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(response)
			return
		}

		// Wait a short time before checking again
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
			// Continue checking
		}
	}

	// // Include current max datapoints for each dataset
	// for datasetName := range req.Datasets {
	// 	maxDatapoint, err := store.GetMaxDatapointForDataset(r.Context(), datasetName)
	// 	if err != nil {
	// 		s.logger.Error("failed to get max datapoint", "dataset", datasetName, "error", err)
	// 		http.Error(w, "Internal server error", http.StatusInternalServerError)
	// 		return
	// 	}
	// 	response.Datasets[datasetName] = maxDatapoint
	// }

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Retry-After", "1") // Suggest client retry after 1 second
	w.WriteHeader(http.StatusAccepted) // 202 Accepted
	json.NewEncoder(w).Encode(
		WaitResponse{
			Datasets: datasetMaxPoints,
		},
	)
}
