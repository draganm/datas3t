package server

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// HandleGetDataranges handles GET requests to retrieve all dataranges for a dataset
func (s *Server) HandleGetDataranges(w http.ResponseWriter, r *http.Request) {
	type DataRange struct {
		ObjectKey       string `json:"object_key"`
		MinDatapointKey int64  `json:"min_datapoint_key"`
		MaxDatapointKey int64  `json:"max_datapoint_key"`
		SizeBytes       int64  `json:"size_bytes"`
	}

	datasetName := r.PathValue("id")
	if datasetName == "" {
		http.Error(w, "dataset name is required", http.StatusBadRequest)
		return
	}

	store := sqlitestore.New(s.DB)

	// Check if dataset exists
	exists, err := store.DatasetExists(r.Context(), datasetName)
	if err != nil {
		s.logger.Error("failed to check if dataset exists", "error", err)
		http.Error(w, "failed to check if dataset exists", http.StatusInternalServerError)
		return
	}

	if !exists {
		s.logger.Error("dataset not found", "id", datasetName)
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}

	dataranges, err := store.GetDatarangesForDataset(r.Context(), datasetName)
	if err != nil {
		s.logger.Error("failed to query dataranges", "error", err)
		http.Error(w, "failed to query dataranges", http.StatusInternalServerError)
		return
	}

	// Convert sqlc types to our response type
	response := make([]DataRange, len(dataranges))
	for i, dr := range dataranges {
		response[i] = DataRange{
			ObjectKey:       dr.ObjectKey,
			MinDatapointKey: dr.MinDatapointKey,
			MaxDatapointKey: dr.MaxDatapointKey,
			SizeBytes:       dr.SizeBytes,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}
