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
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	log.Info("Key deletion worker started")

	for {
		select {
		case <-ctx.Done():
			log.Info("Key deletion worker shutting down")
			return
		case <-ticker.C:
			err := s.DeleteKeys(ctx, log)
			if err != nil {
				log.Error("Error deleting keys", "error", err)
			}
		}
	}
}
