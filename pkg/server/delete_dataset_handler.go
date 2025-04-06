package server

import (
	"net/http"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
)

// HandleDeleteDataset handles DELETE requests to remove an existing dataset
func (s *Server) HandleDeleteDataset(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	store := sqlitestore.New(s.DB)

	// Check if dataset exists
	exists, err := store.DatasetExists(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to check if dataset exists", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !exists {
		http.Error(w, "dataset not found", http.StatusNotFound)
		return
	}

	// Start a transaction
	tx, err := s.DB.BeginTx(r.Context(), nil)
	if err != nil {
		s.logger.Error("failed to begin transaction", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer tx.Rollback() // Will be ignored if transaction is committed

	txStore := store.WithTx(tx)

	// Get all object keys for the dataset before deleting
	objectKeys, err := txStore.GetDatarangeObjectKeysForDataset(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to get datarange object keys", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Schedule all S3 objects for immediate deletion
	for _, objectKey := range objectKeys {
		// Insert the object key into keys_to_delete with immediate deletion time
		err = txStore.InsertKeyToDeleteImmediately(r.Context(), objectKey)
		if err != nil {
			s.logger.Error("failed to schedule S3 object for immediate deletion", "key", objectKey, "error", err)
			// Continue with other objects
		}
	}

	// Delete the dataset (cascades to dataranges and datapoints)
	err = txStore.DeleteDataset(r.Context(), id)
	if err != nil {
		s.logger.Error("failed to delete dataset", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		s.logger.Error("failed to commit transaction", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
