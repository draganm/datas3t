package keydeletion

import (
	"context"
	"log/slog"
	"time"

	"github.com/draganm/datas3t/postgresstore"
	"github.com/jackc/pgx/v5/pgxpool"
)

type KeyDeletionServer struct {
	db        *pgxpool.Pool
	queries   *postgresstore.Queries
	encryptor interface {
		DecryptCredentials(accessKey, secretKey string) (string, string, error)
	}
	concurrency int // Number of concurrent deletion workers
}

func NewServer(db *pgxpool.Pool, encryptor interface {
	DecryptCredentials(accessKey, secretKey string) (string, string, error)
}) *KeyDeletionServer {
	return &KeyDeletionServer{
		db:          db,
		queries:     postgresstore.New(db),
		encryptor:   encryptor,
		concurrency: 5, // Default to 5 concurrent workers
	}
}

// WithConcurrency sets the number of concurrent deletion workers
func (s *KeyDeletionServer) WithConcurrency(concurrency int) *KeyDeletionServer {
	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > 20 {
		concurrency = 20 // Cap at 20 to avoid overwhelming S3
	}
	s.concurrency = concurrency
	return s
}

func (s *KeyDeletionServer) Start(ctx context.Context, log *slog.Logger) {
	go s.deletionWorker(ctx, log)
}

func (s *KeyDeletionServer) deletionWorker(ctx context.Context, log *slog.Logger) {
	log.Info("Object deletion worker started")

	// Adaptive polling parameters
	minInterval := 1 * time.Second  // Minimum polling interval when work is available
	maxInterval := 60 * time.Second // Maximum polling interval when no work
	currentInterval := minInterval

	for {
		select {
		case <-ctx.Done():
			log.Info("Object deletion worker shutting down")
			return
		default:
			objectsProcessed, err := s.DeleteObjects(ctx, log)
			if err != nil {
				log.Error("Error deleting objects", "error", err)
				// On error, use moderate interval
				currentInterval = 10 * time.Second
			} else if objectsProcessed == 0 {
				// No work available, exponentially increase interval up to max
				currentInterval = time.Duration(float64(currentInterval) * 1.5)
				if currentInterval > maxInterval {
					currentInterval = maxInterval
				}
			} else {
				// Work was processed, reset to minimum interval for fast processing
				currentInterval = minInterval
				log.Debug("Processed objects, using fast polling", "objects", objectsProcessed, "interval", currentInterval)
			}

			// Wait for the calculated interval
			if currentInterval > minInterval {
				log.Debug("Waiting before next check", "interval", currentInterval, "processed", objectsProcessed)
			}

			select {
			case <-ctx.Done():
				log.Info("Object deletion worker shutting down")
				return
			case <-time.After(currentInterval):
				continue
			}
		}
	}
}
