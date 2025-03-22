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
    size_bytes UNSIGNED BIGINT NOT NULL,
    FOREIGN KEY (dataset_name) REFERENCES datasets(name) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS datapoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    datarange_id INTEGER NOT NULL,
    datapoint_key UNSIGNED BIGINT NOT NULL,
    begin_offset UNSIGNED BIGINT NOT NULL,
    end_offset UNSIGNED BIGINT NOT NULL,
    FOREIGN KEY (datarange_id) REFERENCES dataranges(id) ON DELETE CASCADE
);


CREATE TABLE IF NOT EXISTS keys_to_delete (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key TEXT NOT NULL UNIQUE,
    delete_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_keys_to_delete_delete_at ON keys_to_delete(delete_at);


CREATE INDEX IF NOT EXISTS idx_dataranges_dataset_name ON dataranges(dataset_name);
CREATE INDEX IF NOT EXISTS idx_dataranges_key_range ON dataranges(min_datapoint_key, max_datapoint_key);
CREATE INDEX IF NOT EXISTS idx_datapoints_datarange_id ON datapoints(datarange_id);
CREATE INDEX IF NOT EXISTS idx_datapoints_datapoint_key ON datapoints(datapoint_key);
