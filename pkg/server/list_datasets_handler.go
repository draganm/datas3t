package server

import (
	"encoding/json"
	"net/http"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// Dataset represents a dataset in the response
type Dataset struct {
	ID             string `json:"id"`
	DatarangeCount int64  `json:"datarange_count"`
	TotalSizeBytes int64  `json:"total_size_bytes"`
}

// HandleListDatasets handles GET requests to list all datasets
func (s *Server) HandleListDatasets(w http.ResponseWriter, r *http.Request) {
	store := sqlitestore.New(s.DB)

	datasets, err := store.GetAllDatasets(r.Context())
	if err != nil {
		s.logger.Error("failed to get datasets", "error", err)
		http.Error(w, "failed to get datasets", http.StatusInternalServerError)
		return
	}

	// Convert SQL result to response format
	response := make([]Dataset, len(datasets))
	for i, ds := range datasets {

		response[i] = Dataset{
			ID:             ds.ID,
			DatarangeCount: ds.DatarangeCount,
			TotalSizeBytes: ds.TotalSizeBytes,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		s.logger.Error("failed to encode response", "error", err)
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
		return
	}
}
