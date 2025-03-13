CREATE TABLE IF NOT EXISTS datasets (
    name TEXT PRIMARY KEY,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dataranges (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dataset_name TEXT NOT NULL,
    object_key TEXT NOT NULL,
    min_datapoint_key UNSIGNED BIGINT NOT NULL,
    max_datapoint_key UNSIGNED BIGINT NOT NULL,
    FOREIGN KEY (dataset_name) REFERENCES datasets(name) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_dataranges_dataset_name ON dataranges(dataset_name);
CREATE INDEX IF NOT EXISTS idx_dataranges_key_range ON dataranges(min_datapoint_key, max_datapoint_key);
