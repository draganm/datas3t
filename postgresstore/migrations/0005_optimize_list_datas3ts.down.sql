-- Remove indexes added for ListDatas3ts optimization
DROP INDEX IF EXISTS idx_dataranges_datas3t_aggregates;
DROP INDEX IF EXISTS idx_datas3ts_s3_bucket_id;