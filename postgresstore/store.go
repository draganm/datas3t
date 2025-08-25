package postgresstore

import "embed"

//go:generate go tool sqlc generate

//go:embed migrations/*.sql
var MigrationsFS embed.FS
