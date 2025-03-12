package serverworld

import (
	"context"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"

	"github.com/draganm/datas3t/pkg/server"
)

type World struct {
	ServerURL          string
	CurrentDatasetID   string
	LastResponseStatus int
}

func New(ctx context.Context) (*World, error) {

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	server, err := server.CreateServer(
		ctx,
		log,
		":memory:",
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create server: %w", err)
	}

	httpServer := httptest.NewServer(server.Handler)
	context.AfterFunc(ctx, func() {
		httpServer.Close()
	})

	return &World{
		ServerURL: httpServer.URL,
	}, nil
}

type worldKey string

const worldContextKey worldKey = "world"

func FromContext(ctx context.Context) (*World, bool) {
	world, ok := ctx.Value(worldContextKey).(*World)
	return world, ok
}

func ToContext(ctx context.Context, world *World) context.Context {
	return context.WithValue(ctx, worldContextKey, world)
}
