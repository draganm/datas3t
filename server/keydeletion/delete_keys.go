package keydeletion

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// DeleteKeys provides backward compatibility for tests - uses the old HTTP-based deletion method
func (s *KeyDeletionServer) DeleteKeys(ctx context.Context, log *slog.Logger) (int, error) {
	// Get up to 100 keys to delete using the old method (limited for HTTP requests)
	keys, err := s.queries.GetKeysToDelete(ctx, 100)
	if err != nil {
		return 0, err
	}

	if len(keys) == 0 {
		return 0, nil
	}

	log.Info("Processing keys for deletion (compatibility mode)", "count", len(keys))

	// Delete keys from S3 and track successful deletions
	var successfulDeletions []int64
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	for _, key := range keys {
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, key.PresignedDeleteUrl, nil)
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
		err = s.queries.DeleteKeysToDelete(ctx, successfulDeletions)
		if err != nil {
			return 0, err
		}

		log.Info("Removed keys from database", "count", len(successfulDeletions))
	}

	return len(keys), nil
}
