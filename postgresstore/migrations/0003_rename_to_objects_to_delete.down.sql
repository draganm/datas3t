-- Recreate the old index
CREATE INDEX IF NOT EXISTS idx_keys_to_delete_delete_after ON objects_to_delete(created_at);

-- Drop the new index
DROP INDEX IF EXISTS idx_objects_to_delete_s3_bucket_id;

-- Drop foreign key constraint
ALTER TABLE objects_to_delete 
    DROP CONSTRAINT IF EXISTS fk_objects_to_delete_s3_bucket_id;

-- Remove new columns
ALTER TABLE objects_to_delete 
    DROP COLUMN IF EXISTS object_name,
    DROP COLUMN IF EXISTS s3_bucket_id;

-- Rename back to original table name
ALTER TABLE objects_to_delete RENAME TO keys_to_delete;