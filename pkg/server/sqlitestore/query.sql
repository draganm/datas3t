-- name: DatasetExists :one
SELECT count(*) > 0 FROM datasets WHERE name = ?;

-- name: CreateDataset :exec
INSERT INTO datasets (name) VALUES (?);

