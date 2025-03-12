package server

import (
	"context"
	"log/slog"
	"net/http"

	"database/sql"
	"embed"
	"io/fs"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
)

// Plan for the server
// endpoints
// 	create a datas3t:  PUT /api/v1/datas3t/{id}
// 	get a datas3 info: GET /api/v1/datas3t/{id}
// 	put a datas3t: PATCH /api/v1/datas3t/{id}
// 	post data to a datas3t: POST /api/v1/datas3t/{id}
//  get data for a datas3t range: GET /api/v1/datas3t/{id}/data/{start}/{end}
//  get data for a single data: GET /api/v1/datas3t/{id}/data/{id}

//go:embed sqlitestore/migrations/*.sql
var migrationsFS embed.FS

type Server struct {
	db *sql.DB
	http.Handler
}

func CreateServer(
	ctx context.Context,
	log *slog.Logger,
	dbURL string,
) (*Server, error) {

	// Import required packages

	// Open SQLite database
	db, err := sql.Open("sqlite3", dbURL)
	if err != nil {
		return nil, err
	}

	context.AfterFunc(ctx, func() {
		err := db.Close()
		if err != nil {
			log.Error("failed to close database", "error", err)
		}
	})

	// Ensure database connection is working
	if err := db.Ping(); err != nil {
		return nil, err
	}
	log.Info("Connected to SQLite database", "url", dbURL)

	// Prepare migrations from embedded filesystem
	migrationFS, err := fs.Sub(migrationsFS, "sqlitestore/migrations")
	if err != nil {
		return nil, err
	}

	d, err := iofs.New(migrationFS, ".")
	if err != nil {
		return nil, err
	}

	// Initialize database driver for migrations
	driver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return nil, err
	}

	// Create migration instance
	m, err := migrate.NewWithInstance("iofs", d, "sqlite3", driver)
	if err != nil {
		return nil, err
	}

	// Apply migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return nil, err
	}
	log.Info("Applied database migrations")

	// Initialize store
	store := sqlitestore.New(db)

	mux := http.NewServeMux()

	mux.HandleFunc("PUT /api/v1/datas3t/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		// Use the store here to avoid "declared but not used" error
		err := store.CreateDataset(r.Context(), id)
		if err != nil {
			log.Error("failed to create dataset", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	mux.HandleFunc("GET /api/v1/datas3t/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		dataset, err := store.DatasetExists(r.Context(), id)
		if err != nil {
			log.Error("failed to get dataset", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !dataset {
			log.Error("dataset not found", "id", id)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)

	})

	return &Server{
		db:      db,
		Handler: mux,
	}, nil
}
