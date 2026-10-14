CREATE TABLE archive_deletions (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id),
    manifest_sha256 TEXT NOT NULL,
    batches INTEGER NOT NULL DEFAULT 0 CHECK(batches>=0),
    rows_processed INTEGER NOT NULL DEFAULT 0 CHECK(rows_processed>=0),
    rows_deleted INTEGER NOT NULL DEFAULT 0 CHECK(rows_deleted>=0),
    rows_absent INTEGER NOT NULL DEFAULT 0 CHECK(rows_absent>=0),
    completed INTEGER NOT NULL DEFAULT 0 CHECK(completed IN (0,1)),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
