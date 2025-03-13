package server

import (
	"net/http"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// HandleGetDataset handles GET requests to retrieve dataset info
func (s *Server) HandleGetDataset(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	store := sqlitestore.New(s.db)
	dataset, err := store.DatasetExists(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to get dataset", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !dataset {
		s.logger.Error("dataset not found", "id", id)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}
