package server

import (
	"fmt"
	"net/http"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// HandleCreateDataset handles PUT requests to create a new dataset
func (s *Server) HandleCreateDataset(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	store := sqlitestore.New(s.DB)

	// Start transaction for both checking and creating
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		s.logger.Error("failed to begin transaction", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	txStore := store.WithTx(tx)

	// Check if any objects related to this dataset are scheduled for deletion
	// The S3 key pattern is dataset/<dataset_name>/datapoints/<from>-<to>.tar
	pattern := fmt.Sprintf("dataset/%s/%%", id)
	hasPendingDeletions, err := txStore.CheckKeysScheduledForDeletion(r.Context(), pattern)
	if err != nil {
		s.logger.Error("failed to check for pending deletions", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if hasPendingDeletions {
		s.logger.Warn("attempted to create dataset with pending deletions", "dataset", id)
		http.Error(w, "dataset has objects pending deletion", http.StatusConflict)
		return
	}

	// Create the dataset in the same transaction
	err = txStore.CreateDataset(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to create dataset", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Commit the transaction only if both operations succeed
	err = tx.Commit()
	if err != nil {
		s.logger.Error("failed to commit transaction", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
