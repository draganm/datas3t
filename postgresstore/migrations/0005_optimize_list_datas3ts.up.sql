-- Add covering index for ListDatas3ts query optimization
-- This index includes all columns needed for the aggregations in the ListDatas3ts query
-- to avoid accessing the heap for aggregate calculations
CREATE INDEX IF NOT EXISTS idx_dataranges_datas3t_aggregates
ON dataranges(datas3t_id)
INCLUDE (min_datapoint_key, max_datapoint_key, size_bytes);

-- Add index on datas3ts.s3_bucket_id for faster joins
CREATE INDEX IF NOT EXISTS idx_datas3ts_s3_bucket_id
ON datas3ts(s3_bucket_id);

-- Analyze tables to update statistics for query planner
ANALYZE dataranges;
ANALYZE datas3ts;
ANALYZE s3_buckets;