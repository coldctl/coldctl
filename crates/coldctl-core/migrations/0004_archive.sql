CREATE TABLE policies (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    destination_id TEXT NOT NULL REFERENCES destinations(id) ON DELETE RESTRICT,
    config_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE jobs (
    id TEXT PRIMARY KEY NOT NULL,
    policy_name TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    rows_processed INTEGER NOT NULL DEFAULT 0,
    bytes_written INTEGER NOT NULL DEFAULT 0,
    objects_created INTEGER NOT NULL DEFAULT 0,
    last_key INTEGER,
    error TEXT
);
