package keydeletion

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type KeyDeletionServer struct {
	db *pgxpool.Pool
}

func NewServer(db *pgxpool.Pool) *KeyDeletionServer {
	return &KeyDeletionServer{
		db: db,
	}
}

func (s *KeyDeletionServer) Start(ctx context.Context, log *slog.Logger) {
	go s.deletionWorker(ctx, log)
}

func (s *KeyDeletionServer) deletionWorker(ctx context.Context, log *slog.Logger) {
	log.Info("Key deletion worker started")

	for {
		select {
		case <-ctx.Done():
			log.Info("Key deletion worker shutting down")
			return
		default:
			keysProcessed, err := s.DeleteKeys(ctx, log)
			if err != nil {
				log.Error("Error deleting keys", "error", err)
			}

			// If no keys were processed, wait 1 minute before checking again
			if keysProcessed == 0 {
				select {
				case <-ctx.Done():
					log.Info("Key deletion worker shutting down")
					return
				case <-time.After(60 * time.Second):
					continue
				}
			}
			// If keys were processed, continue immediately to check for more
		}
	}
}
