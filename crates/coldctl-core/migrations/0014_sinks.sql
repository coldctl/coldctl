CREATE TABLE sink_saved_policies AS SELECT * FROM policies;
CREATE TABLE sink_saved_queue AS SELECT * FROM archive_queue;
DROP TABLE archive_queue;
DROP TABLE policies;
CREATE TABLE destinations_v2 (
 id TEXT PRIMARY KEY NOT NULL,
 name TEXT NOT NULL UNIQUE,
 destination_type TEXT NOT NULL CHECK(destination_type IN ('local','s3')),
 path TEXT NOT NULL,
 created_at TEXT NOT NULL,
 config_json TEXT
);
INSERT INTO destinations_v2 SELECT id,name,destination_type,path,created_at,NULL FROM destinations;
DROP TABLE destinations;
ALTER TABLE destinations_v2 RENAME TO destinations;

CREATE TABLE policies (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    destination_id TEXT NOT NULL REFERENCES destinations(id) ON DELETE RESTRICT,
    config_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
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

INSERT INTO policies SELECT * FROM sink_saved_policies;
INSERT INTO archive_queue SELECT * FROM sink_saved_queue;
DROP TABLE sink_saved_queue;
DROP TABLE sink_saved_policies;
