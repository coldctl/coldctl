CREATE TABLE archive_queue (
    id TEXT PRIMARY KEY,
    policy_id TEXT NOT NULL REFERENCES policies(id),
    policy_name TEXT NOT NULL,
    job_id TEXT UNIQUE REFERENCES jobs(id),
    options_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('pending','running','completed','failed','cancelled')),
    due_at INTEGER NOT NULL,
    failures INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE UNIQUE INDEX archive_queue_active_policy ON archive_queue(policy_id)
    WHERE status IN ('pending','running');
CREATE INDEX archive_queue_due ON archive_queue(status,due_at);
