-- name: DatasetExists :one
SELECT count(*) > 0 FROM datasets WHERE name = ?;

-- name: CreateDataset :exec
INSERT INTO datasets (name) VALUES (?);

-- name: InsertDataRange :exec
INSERT INTO dataranges (dataset_name, object_key, min_datapoint_key, max_datapoint_key) 
VALUES (?, ?, ?, ?);
