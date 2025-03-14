-- name: DatasetExists :one
SELECT count(*) > 0 FROM datasets WHERE name = ?;

-- name: CreateDataset :exec
INSERT INTO datasets (name) VALUES (?);

-- name: InsertDataRange :one
INSERT INTO dataranges (dataset_name, object_key, min_datapoint_key, max_datapoint_key) 
VALUES (?, ?, ?, ?)
RETURNING id;

-- name: InsertDatapoint :exec
INSERT INTO datapoints (datarange_id, datapoint_key, begin_offset, end_offset)
VALUES (?, ?, ?, ?);

-- name: GetDatarangeIDsForDataset :many
SELECT id FROM dataranges WHERE dataset_name = ?;

-- name: GetDatapointsForDataset :many
SELECT d.id, d.datarange_id, d.datapoint_key, d.begin_offset, d.end_offset 
FROM datapoints d
JOIN dataranges dr ON d.datarange_id = dr.id
WHERE dr.dataset_name = ?
ORDER BY d.datapoint_key;
