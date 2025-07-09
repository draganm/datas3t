package server

import (
	"context"
	"log/slog"

	"github.com/draganm/datas3t/server/bucket"
	"github.com/draganm/datas3t/server/dataranges"
	"github.com/draganm/datas3t/server/datas3t"
	"github.com/draganm/datas3t/server/download"
	"github.com/draganm/datas3t/server/keydeletion"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Server struct {
	*bucket.BucketServer
	*datas3t.Datas3tServer
	*dataranges.UploadDatarangeServer
	*download.DownloadServer
	*keydeletion.KeyDeletionServer
}

func NewServer(db *pgxpool.Pool, cacheDir string, maxCacheSize int64, encryptionKey string) (*Server, error) {
	bucketServer, err := bucket.NewServer(db, encryptionKey)
	if err != nil {
		return nil, err
	}

	datas3tServer, err := datas3t.NewServer(db, encryptionKey)
	if err != nil {
		return nil, err
	}

	datarangesServer, err := dataranges.NewServer(db, encryptionKey)
	if err != nil {
		return nil, err
	}

	downloadServer, err := download.NewServer(db, cacheDir, maxCacheSize, encryptionKey)
	if err != nil {
		return nil, err
	}

	keyDeletionServer := keydeletion.NewServer(db)

	return &Server{
		BucketServer:          bucketServer,
		Datas3tServer:         datas3tServer,
		UploadDatarangeServer: datarangesServer,
		DownloadServer:        downloadServer,
		KeyDeletionServer:     keyDeletionServer,
	}, nil
}

func (s *Server) StartKeyDeletionWorker(ctx context.Context, log *slog.Logger) {
	s.KeyDeletionServer.Start(ctx, log)
}
