-- name: DatasetExists :one
SELECT count(*) > 0 FROM datasets WHERE name = ?;

-- name: CreateDataset :exec
INSERT INTO datasets (name) VALUES (?);

-- name: DeleteDataset :exec
DELETE FROM datasets WHERE name = ?;

-- name: GetDatarangeObjectKeysForDataset :many
SELECT object_key FROM dataranges WHERE dataset_name = ?;

-- name: GetDatarangesByDatasetName :many
SELECT id, object_key FROM dataranges WHERE dataset_name = ?;

-- name: InsertDataRange :one
INSERT INTO dataranges (dataset_name, object_key, min_datapoint_key, max_datapoint_key, size_bytes) 
VALUES (?, ?, ?, ?, ?)
RETURNING id;

-- name: InsertDatapoint :exec
INSERT INTO datapoints (datarange_id, datapoint_key, begin_offset, end_offset)
VALUES (?, ?, ?, ?);

-- name: GetDatarangeIDsForDataset :many
SELECT id FROM dataranges WHERE dataset_name = ?;

-- name: GetDatapointsForDataset :many
SELECT d.id, d.datarange_id, d.datapoint_key, d.begin_offset, d.end_offset 
FROM datapoints d
WHERE d.datarange_id IN (
    SELECT id FROM dataranges WHERE dataset_name = ?
)
ORDER BY d.datapoint_key;

-- name: GetSectionsOfDataranges :many
SELECT 
    dr.id,
    dr.object_key,
    CAST((CASE 
        WHEN dr.min_datapoint_key < @start_key THEN 
            (SELECT d.begin_offset 
             FROM datapoints d 
             WHERE d.datarange_id = dr.id 
             AND d.datapoint_key = @start_key 
             LIMIT 1)
        ELSE 
            0
    END) AS UNSIGNED BIGINT) as first_offset,
    CAST((CASE 
        WHEN dr.max_datapoint_key > @end_key THEN 
            (SELECT d.end_offset 
             FROM datapoints d 
             WHERE d.datarange_id = dr.id 
             AND d.datapoint_key <= @end_key 
             ORDER BY d.datapoint_key DESC 
             LIMIT 1)
        ELSE 
            dr.size_bytes-1024
    END) AS UNSIGNED BIGINT) as last_offset,
    dr.size_bytes
FROM dataranges dr
WHERE dr.dataset_name = @dataset_name
AND dr.min_datapoint_key <= @end_key
AND dr.max_datapoint_key >= @start_key
ORDER BY dr.object_key;

-- name: CheckOverlappingDatapointRange :one
SELECT count(*) > 0 FROM dataranges
WHERE dataset_name = @dataset_name
AND (
    (min_datapoint_key <= @new_min AND max_datapoint_key >= @new_min) -- new range start overlaps with existing range
    OR
    (min_datapoint_key <= @new_max AND max_datapoint_key >= @new_max) -- new range end overlaps with existing range
    OR
    (min_datapoint_key >= @new_min AND max_datapoint_key <= @new_max) -- new range contains existing range
);

-- name: GetDatarangesForDataset :many
SELECT object_key, min_datapoint_key, max_datapoint_key, size_bytes 
FROM dataranges 
WHERE dataset_name = ?
ORDER BY min_datapoint_key ASC;

-- name: GetDatarangesForAggregation :many
SELECT id, object_key, min_datapoint_key, max_datapoint_key, size_bytes 
FROM dataranges 
WHERE dataset_name = @dataset_name
AND (
    (min_datapoint_key <= @end_key AND max_datapoint_key >= @start_key)
)
ORDER BY min_datapoint_key ASC;

-- name: GetDatapointsInRange :many
SELECT d.id, d.datarange_id, d.datapoint_key, d.begin_offset, d.end_offset 
FROM datapoints d
JOIN dataranges dr ON d.datarange_id = dr.id
WHERE dr.dataset_name = @dataset_name
AND d.datapoint_key >= @start_key
AND d.datapoint_key <= @end_key
ORDER BY d.datapoint_key;

-- name: UpdateDatapointsDatarangeID :exec
UPDATE datapoints 
SET datarange_id = @new_datarange_id
WHERE datapoint_key >= @start_key
AND datapoint_key <= @end_key
AND datarange_id IN (
    SELECT id FROM dataranges 
    WHERE dataset_name = @dataset_name
);

-- name: DeleteDatarange :exec
DELETE FROM dataranges WHERE id = ?;

-- name: GetAllDatasets :many
SELECT 
    d.name as id,
    (SELECT COUNT(id) FROM dataranges WHERE dataset_name = d.name) as datarange_count,
    CAST(COALESCE((SELECT SUM(size_bytes) FROM dataranges WHERE dataset_name = d.name), 0) AS UNSIGNED BIGINT) as total_size_bytes,
    CAST(COALESCE((SELECT MIN(min_datapoint_key) FROM dataranges WHERE dataset_name = d.name), 0) AS UNSIGNED BIGINT) as min_datapoint_key,
    CAST(COALESCE((SELECT MAX(max_datapoint_key) FROM dataranges WHERE dataset_name = d.name), 0) AS UNSIGNED BIGINT) as max_datapoint_key
FROM datasets d
ORDER BY d.name;

-- name: InsertKeyToDelete :exec
INSERT INTO keys_to_delete (key, delete_at)
VALUES (?, datetime('now', '+1 hour'));

-- name: InsertKeyToDeleteImmediately :exec
INSERT INTO keys_to_delete (key, delete_at)
VALUES (?, datetime('now'));

-- name: GetKeysToDelete :many
SELECT id, key FROM keys_to_delete 
WHERE delete_at <= datetime('now')
LIMIT 100;

-- name: DeleteKeyToDeleteById :exec
DELETE FROM keys_to_delete WHERE id = ?;

-- name: CheckKeysScheduledForDeletion :one
SELECT count(*) > 0 FROM keys_to_delete WHERE key LIKE ?;

-- name: GetFirstAndLastDatapoint :one
SELECT 
    CAST(CASE WHEN COUNT(*) = 0 THEN -1 ELSE MIN(min_datapoint_key) END AS BIGINT) as first_datapoint_key,
    CAST(CASE WHEN COUNT(*) = 0 THEN -1 ELSE MAX(max_datapoint_key) END AS BIGINT) as last_datapoint_key
FROM dataranges
WHERE dataset_name = ?;

-- name: GetDatarangesForMissingRanges :many
SELECT 
    min_datapoint_key, 
    max_datapoint_key
FROM dataranges 
WHERE dataset_name = ?
ORDER BY min_datapoint_key ASC;

-- name: GetLargestDatapointForDatasets :many
SELECT 
    dataset_name,
    CAST(MAX(max_datapoint_key) AS UNSIGNED BIGINT) as largest_datapoint_key
FROM dataranges
WHERE dataset_name IN (sqlc.slice('dataset_names'))
GROUP BY dataset_name
ORDER BY dataset_name;
