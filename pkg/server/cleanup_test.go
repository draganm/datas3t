package server

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"github.com/draganm/datas3t/pkg/server/sqlitestore"
	"github.com/stretchr/testify/require"
)

func TestGetKeysToDeleteAndCleanup(t *testing.T) {
	// Create temporary SQLite database
	dbFile, err := os.CreateTemp("", "cleanup-test-*.db")
	require.NoError(t, err)
	defer os.Remove(dbFile.Name())
	dbFile.Close()

	// Open database
	db, err := sql.Open("sqlite3", dbFile.Name())
	require.NoError(t, err)
	defer db.Close()

	// Run migrations to create tables
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS datasets (
			name TEXT PRIMARY KEY,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS keys_to_delete (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			key TEXT NOT NULL UNIQUE,
			delete_at TIMESTAMP NOT NULL
		);
		
		CREATE INDEX IF NOT EXISTS idx_keys_to_delete_delete_at ON keys_to_delete(delete_at);
	`)
	require.NoError(t, err)

	// Create store
	store := sqlitestore.New(db)

	// Insert test keys to delete
	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	// Add keys that should be deleted (already expired)
	pastTime := time.Now().Add(-24 * time.Hour).UTC().Format(time.RFC3339)
	_, err = tx.Exec("INSERT INTO keys_to_delete (key, delete_at) VALUES (?, ?)", "key1.tar", pastTime)
	require.NoError(t, err)
	_, err = tx.Exec("INSERT INTO keys_to_delete (key, delete_at) VALUES (?, ?)", "key2.tar", pastTime)
	require.NoError(t, err)

	// Add a key that should not be deleted yet
	futureTime := time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
	_, err = tx.Exec("INSERT INTO keys_to_delete (key, delete_at) VALUES (?, ?)", "future-key.tar", futureTime)
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	// Test GetKeysToDelete retrieves only expired keys
	keys, err := store.GetKeysToDelete(ctx)
	require.NoError(t, err)
	require.Len(t, keys, 2, "Should find 2 expired keys")

	// Check the keys are the ones we expect
	keySet := make(map[string]bool)
	for _, k := range keys {
		keySet[k.Key] = true
	}
	require.True(t, keySet["key1.tar"], "key1.tar should be in results")
	require.True(t, keySet["key2.tar"], "key2.tar should be in results")
	require.False(t, keySet["future-key.tar"], "future-key.tar should not be in results")

	// Test deleting keys
	for _, key := range keys {
		err = store.DeleteKeyToDeleteById(ctx, key.ID)
		require.NoError(t, err)
	}

	// Verify keys were deleted from database
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM keys_to_delete WHERE key IN ('key1.tar', 'key2.tar')").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count, "Deleted keys should be removed from the database")

	// Check that future key is still in the database
	err = db.QueryRow("SELECT COUNT(*) FROM keys_to_delete WHERE key = 'future-key.tar'").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 1, count, "Future key should still be in the database")
}
