package keydeletion

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

func (s *KeyDeletionServer) DeleteKeys(ctx context.Context, log *slog.Logger) error {
	// Get up to 20 keys to delete
	keys, err := s.db.Query(ctx, `
		SELECT id, presigned_delete_url
		FROM keys_to_delete
		ORDER BY created_at
		LIMIT 20
	`)
	if err != nil {
		return err
	}
	defer keys.Close()

	var keysToDelete []struct {
		ID  int64
		URL string
	}

	for keys.Next() {
		var key struct {
			ID  int64
			URL string
		}
		err := keys.Scan(&key.ID, &key.URL)
		if err != nil {
			return err
		}
		keysToDelete = append(keysToDelete, key)
	}

	if err := keys.Err(); err != nil {
		return err
	}

	if len(keysToDelete) == 0 {
		return nil
	}

	log.Info("Processing keys for deletion", "count", len(keysToDelete))

	// Delete keys from S3 and track successful deletions
	var successfulDeletions []int64
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	for _, key := range keysToDelete {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, key.URL, nil)
		if err != nil {
			log.Error("Error creating delete request", "error", err, "key_id", key.ID)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			log.Error("Error executing delete request", "error", err, "key_id", key.ID)
			continue
		}
		resp.Body.Close()

		// Consider 2xx and 404 as successful (404 means already deleted)
		if resp.StatusCode >= 200 && resp.StatusCode < 300 || resp.StatusCode == 404 {
			successfulDeletions = append(successfulDeletions, key.ID)
			log.Debug("Successfully deleted key", "key_id", key.ID, "status", resp.StatusCode)
		} else {
			log.Error("Failed to delete key", "key_id", key.ID, "status", resp.StatusCode)
		}
	}

	// Remove successfully deleted keys from database
	if len(successfulDeletions) > 0 {
		tx, err := s.db.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		_, err = tx.Exec(ctx, `DELETE FROM keys_to_delete WHERE id = ANY($1)`, successfulDeletions)
		if err != nil {
			return err
		}

		err = tx.Commit(ctx)
		if err != nil {
			return err
		}

		log.Info("Removed keys from database", "count", len(successfulDeletions))
	}

	return nil
}
