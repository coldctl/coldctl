-- Rebuild the status constraint while preserving both child journals transactionally.
CREATE TEMP TABLE saved_checkpoints AS SELECT * FROM archive_checkpoints;
CREATE TEMP TABLE saved_objects AS SELECT * FROM archive_objects;
DROP TABLE archive_objects;
DROP TABLE archive_checkpoints;
ALTER TABLE jobs RENAME TO jobs_v5;
CREATE TABLE jobs (
 id TEXT PRIMARY KEY NOT NULL, policy_name TEXT NOT NULL, plan_json TEXT NOT NULL,
 status TEXT NOT NULL CHECK(status IN ('running','completed','failed','cancelled','paused')),
 started_at TEXT NOT NULL, completed_at TEXT,
 rows_processed INTEGER NOT NULL DEFAULT 0, bytes_written INTEGER NOT NULL DEFAULT 0,
 objects_created INTEGER NOT NULL DEFAULT 0, last_key INTEGER, error TEXT,
 cancel_requested INTEGER NOT NULL DEFAULT 0, verified_at TEXT,
 execution_json TEXT NOT NULL DEFAULT '{}'
);
INSERT INTO jobs (id,policy_name,plan_json,status,started_at,completed_at,rows_processed,bytes_written,objects_created,last_key,error,cancel_requested,verified_at) SELECT * FROM jobs_v5;
DROP TABLE jobs_v5;
UPDATE jobs SET execution_json=json_object('minimum_free_bytes',COALESCE(json_extract(plan_json,'$.safety.minimum_free_bytes'),1073741824));
CREATE TABLE archive_checkpoints (
 job_id TEXT PRIMARY KEY NOT NULL REFERENCES jobs(id), source_id TEXT NOT NULL,
 source_identity TEXT NOT NULL, upper_key INTEGER, manifest_json TEXT
);
INSERT INTO archive_checkpoints SELECT * FROM saved_checkpoints;
CREATE TABLE archive_objects (
 job_id TEXT NOT NULL REFERENCES archive_checkpoints(job_id), sequence INTEGER NOT NULL,
 metadata_json TEXT NOT NULL, committed INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(job_id,sequence)
);
INSERT INTO archive_objects SELECT * FROM saved_objects;
DROP TABLE saved_objects;
DROP TABLE saved_checkpoints;
