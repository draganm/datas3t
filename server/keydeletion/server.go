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
}

func NewServer(db *pgxpool.Pool, encryptor interface {
	DecryptCredentials(accessKey, secretKey string) (string, string, error)
}) *KeyDeletionServer {
	return &KeyDeletionServer{
		db:        db,
		queries:   postgresstore.New(db),
		encryptor: encryptor,
	}
}

func (s *KeyDeletionServer) Start(ctx context.Context, log *slog.Logger) {
	go s.deletionWorker(ctx, log)
}

func (s *KeyDeletionServer) deletionWorker(ctx context.Context, log *slog.Logger) {
	log.Info("Object deletion worker started")

	for {
		select {
		case <-ctx.Done():
			log.Info("Object deletion worker shutting down")
			return
		default:
			objectsProcessed, err := s.DeleteObjects(ctx, log)
			if err != nil {
				log.Error("Error deleting objects", "error", err)
			}

			// If no objects were processed, wait 1 minute before checking again
			if objectsProcessed == 0 {
				select {
				case <-ctx.Done():
					log.Info("Object deletion worker shutting down")
					return
				case <-time.After(60 * time.Second):
					continue
				}
			}
			// If objects were processed, continue immediately to check for more
		}
	}
}
