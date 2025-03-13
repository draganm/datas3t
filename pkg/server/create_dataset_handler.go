package server

import (
	"net/http"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// HandleCreateDataset handles PUT requests to create a new dataset
func (s *Server) HandleCreateDataset(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	store := sqlitestore.New(s.db)
	err := store.CreateDataset(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to create dataset", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
