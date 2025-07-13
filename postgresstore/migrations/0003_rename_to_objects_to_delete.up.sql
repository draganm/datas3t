-- Rename keys_to_delete table to objects_to_delete and update schema
ALTER TABLE keys_to_delete RENAME TO objects_to_delete;

-- Add new columns for storing bucket reference and object name
ALTER TABLE objects_to_delete 
    ADD COLUMN s3_bucket_id BIGINT,
    ADD COLUMN object_name VARCHAR(1024);

-- Note: We keep presigned_delete_url as NOT NULL for backward compatibility
-- New object deletion will use a sentinel value or empty string when using bucket_id/object_name approach

-- Add foreign key constraint to s3_buckets
ALTER TABLE objects_to_delete 
    ADD CONSTRAINT fk_objects_to_delete_s3_bucket_id 
    FOREIGN KEY (s3_bucket_id) REFERENCES s3_buckets(id) ON DELETE CASCADE;

-- Create index for efficient bucket-based queries
CREATE INDEX IF NOT EXISTS idx_objects_to_delete_s3_bucket_id ON objects_to_delete(s3_bucket_id);

-- Drop the old index since we're changing the table structure
DROP INDEX IF EXISTS idx_keys_to_delete_delete_after;