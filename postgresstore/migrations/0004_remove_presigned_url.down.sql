-- Re-add the presigned_delete_url column for rollback
ALTER TABLE objects_to_delete ADD COLUMN presigned_delete_url VARCHAR(8192);