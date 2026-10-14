CREATE TABLE recurring_archives (
 id TEXT PRIMARY KEY,
 policy_id TEXT NOT NULL UNIQUE REFERENCES policies(id),
 policy_name TEXT NOT NULL,
 snapshot_json TEXT NOT NULL,
 options_json TEXT NOT NULL,
 interval_seconds INTEGER NOT NULL CHECK(interval_seconds BETWEEN 60 AND 31536000),
 status TEXT NOT NULL CHECK(status IN ('active','failed','stopped')),
 next_due INTEGER NOT NULL,
 current_queue TEXT REFERENCES archive_queue(id),
 cycles INTEGER NOT NULL DEFAULT 0,
 last_error TEXT,
 created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
ALTER TABLE archive_queue ADD COLUMN recurring_id TEXT REFERENCES recurring_archives(id);
ALTER TABLE archive_queue ADD COLUMN cutoff_utc TEXT;
