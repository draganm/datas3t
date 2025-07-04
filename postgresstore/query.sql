-- name: Datas3tExists :one
SELECT count(*) > 0
FROM datas3ts;

-- name: AllDatas3ts :many
SELECT name
FROM datas3ts;

-- name: ListDatas3ts :many
SELECT 
    d.name as datas3t_name,
    s.name as bucket_name,
    COALESCE(COUNT(dr.id), 0) as datarange_count,
    COALESCE(SUM(dr.max_datapoint_key - dr.min_datapoint_key + 1), 0) as total_datapoints,
    COALESCE(MIN(dr.min_datapoint_key), 0) as lowest_datapoint,
    COALESCE(MAX(dr.max_datapoint_key), 0) as highest_datapoint,
    COALESCE(SUM(dr.size_bytes), 0) as total_bytes
FROM datas3ts d
JOIN s3_buckets s ON d.s3_bucket_id = s.id
LEFT JOIN dataranges dr ON d.id = dr.datas3t_id
GROUP BY d.id, d.name, s.name
ORDER BY d.name;

-- name: BucketExists :one
SELECT count(*) > 0
FROM s3_buckets
WHERE name = $1;

-- name: AllAccessConfigs :many
SELECT DISTINCT name
FROM s3_buckets;

-- name: ListAllBuckets :many
SELECT name, endpoint, bucket
FROM s3_buckets
ORDER BY name;

-- name: GetDatas3tWithBucket :one
SELECT d.id, d.name, d.s3_bucket_id, d.upload_counter,
       s.endpoint, s.bucket, s.access_key, s.secret_key
FROM datas3ts d
JOIN s3_buckets s ON d.s3_bucket_id = s.id
WHERE d.name = $1;

-- name: CheckDatarangeOverlap :one
SELECT count(*) > 0
FROM dataranges
WHERE datas3t_id = $1
  AND min_datapoint_key < $2
  AND max_datapoint_key >= $3;

-- name: CheckDatarangeUploadOverlap :one
SELECT count(*) > 0
FROM datarange_uploads
WHERE datas3t_id = $1
  AND first_datapoint_index < $2
  AND (first_datapoint_index + number_of_datapoints - 1) >= $3;

-- name: CreateDatarange :one
INSERT INTO dataranges (datas3t_id, data_object_key, index_object_key, min_datapoint_key, max_datapoint_key, size_bytes)
VALUES (@datas3t_id, @data_object_key, @index_object_key, @min_datapoint_key, @max_datapoint_key, @size_bytes)
RETURNING id;

-- name: CreateDatarangeUpload :one
INSERT INTO datarange_uploads (
    datas3t_id, 
    upload_id,
    data_object_key,
    index_object_key,
    first_datapoint_index, 
    number_of_datapoints, 
    data_size
)
VALUES (@datas3t_id, @upload_id, @data_object_key, @index_object_key, @first_datapoint_index, @number_of_datapoints, @data_size)
RETURNING id;

-- name: GetDatarangeUploadWithDetails :one
SELECT 
    du.id, 
    du.datas3t_id, 
    du.upload_id, 
    du.first_datapoint_index, 
    du.number_of_datapoints, 
    du.data_size,
    du.data_object_key, 
    du.index_object_key,
    d.name as datas3t_name, 
    d.s3_bucket_id,
    s.endpoint, 
    s.bucket, 
    s.access_key, 
    s.secret_key
FROM datarange_uploads du
JOIN datas3ts d ON du.datas3t_id = d.id
JOIN s3_buckets s ON d.s3_bucket_id = s.id
WHERE du.id = $1;

-- name: ScheduleKeyForDeletion :exec
INSERT INTO keys_to_delete (presigned_delete_url, delete_after)
VALUES ($1, $2);

-- name: DeleteDatarangeUpload :exec
DELETE FROM datarange_uploads WHERE id = $1;

-- name: DeleteDatarange :exec
DELETE FROM dataranges WHERE id = $1;

-- name: AddBucket :exec
INSERT INTO s3_buckets (
        name,
        endpoint,
        bucket,
        access_key,
        secret_key
    )
VALUES ($1, $2, $3, $4, $5);

-- name: AddDatas3t :exec
INSERT INTO datas3ts (name, s3_bucket_id) 
SELECT @datas3t_name, id 
FROM s3_buckets 
WHERE s3_buckets.name = @bucket_name;

-- name: AddDatarangeUpload :one
INSERT INTO datarange_uploads (datas3t_id, first_datapoint_index, number_of_datapoints, data_size)
SELECT d.id, @first_datapoint_index, @number_of_datapoints, @data_size
FROM datas3ts d
WHERE d.name = @datas3t_name
RETURNING id;

-- name: GetAllDataranges :many
SELECT id, datas3t_id, min_datapoint_key, max_datapoint_key, size_bytes
FROM dataranges;

-- name: GetAllDatarangeUploads :many
SELECT id, datas3t_id, upload_id, first_datapoint_index, number_of_datapoints, data_size
FROM datarange_uploads;

-- name: GetDatarangeFields :many
SELECT min_datapoint_key, max_datapoint_key, size_bytes
FROM dataranges;

-- name: GetDatarangeUploadIDs :many
SELECT upload_id
FROM datarange_uploads;

-- name: CountDataranges :one
SELECT count(*)
FROM dataranges;

-- name: CountDatarangeUploads :one
SELECT count(*)
FROM datarange_uploads;

-- name: CountKeysToDelete :one
SELECT count(*)
FROM keys_to_delete;

-- name: GetDatarangesForDatapoints :many
SELECT 
    dr.id,
    dr.data_object_key,
    dr.index_object_key,
    dr.min_datapoint_key,
    dr.max_datapoint_key,
    dr.size_bytes,
    d.name as datas3t_name,
    s.endpoint,
    s.bucket,
    s.access_key,
    s.secret_key
FROM dataranges dr
JOIN datas3ts d ON dr.datas3t_id = d.id
JOIN s3_buckets s ON d.s3_bucket_id = s.id
WHERE d.name = $1
  AND dr.min_datapoint_key <= $2  -- datarange starts before or at our last datapoint
  AND dr.max_datapoint_key >= $3  -- datarange ends after or at our first datapoint
ORDER BY dr.min_datapoint_key;

-- name: IncrementUploadCounter :one
UPDATE datas3ts 
SET upload_counter = upload_counter + 1,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
RETURNING upload_counter;

-- name: GetDatarangeByExactRange :one
SELECT 
    dr.id,
    dr.datas3t_id,
    dr.data_object_key,
    dr.index_object_key,
    dr.min_datapoint_key,
    dr.max_datapoint_key,
    dr.size_bytes,
    d.name as datas3t_name,
    s.endpoint,
    s.bucket,
    s.access_key,
    s.secret_key
FROM dataranges dr
JOIN datas3ts d ON dr.datas3t_id = d.id
JOIN s3_buckets s ON d.s3_bucket_id = s.id
WHERE d.name = $1
  AND dr.min_datapoint_key = $2
  AND dr.max_datapoint_key = $3;

-- name: GetDatarangesForDatas3t :many
SELECT min_datapoint_key, max_datapoint_key
FROM dataranges dr
JOIN datas3ts d ON dr.datas3t_id = d.id
WHERE d.name = $1;