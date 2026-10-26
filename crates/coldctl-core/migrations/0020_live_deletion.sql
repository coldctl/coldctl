ALTER TABLE archive_deletions ADD COLUMN rows_deferred INTEGER NOT NULL DEFAULT 0 CHECK(rows_deferred>=0);
ALTER TABLE archive_deletions ADD COLUMN rows_absent_uncertain INTEGER NOT NULL DEFAULT 0 CHECK(rows_absent_uncertain>=0);
ALTER TABLE archive_deletions ADD COLUMN deletion_mode TEXT NOT NULL DEFAULT 'legacy' CHECK(deletion_mode IN ('legacy','live_values_v1'));
ALTER TABLE archive_deletions ADD COLUMN attribution_baseline TEXT NOT NULL DEFAULT 'legacy_untracked' CHECK(attribution_baseline IN ('legacy_untracked','tracked_v1'));
CREATE TRIGGER deletion_counts_insert BEFORE INSERT ON archive_deletions
WHEN NEW.rows_processed != NEW.rows_deleted + NEW.rows_absent + NEW.rows_deferred
 OR NEW.rows_absent_uncertain > NEW.rows_absent
BEGIN SELECT RAISE(ABORT,'invalid deletion counts'); END;
CREATE TRIGGER deletion_counts_update BEFORE UPDATE ON archive_deletions
WHEN NEW.rows_processed != NEW.rows_deleted + NEW.rows_absent + NEW.rows_deferred
 OR NEW.rows_absent_uncertain > NEW.rows_absent
BEGIN SELECT RAISE(ABORT,'invalid deletion counts'); END;
CREATE TABLE archive_deletion_batches (
 job_id TEXT NOT NULL REFERENCES archive_deletions(job_id),
 sequence INTEGER NOT NULL CHECK(sequence>0),
 object_sha256 TEXT NOT NULL CHECK(length(object_sha256)=64),
 row_count INTEGER NOT NULL CHECK(row_count BETWEEN 1 AND 1000),
 state TEXT NOT NULL CHECK(state IN ('pending','applied')),
 attempts INTEGER NOT NULL CHECK(attempts>=1),
 recovery_uncertain INTEGER NOT NULL CHECK(recovery_uncertain IN (0,1)),
 deleted INTEGER, absent INTEGER, deferred INTEGER, absent_uncertain INTEGER,
 created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
 updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
 PRIMARY KEY(job_id,sequence),
 CHECK((state='pending' AND deleted IS NULL AND absent IS NULL AND deferred IS NULL AND absent_uncertain IS NULL)
 OR (state='applied' AND deleted IS NOT NULL AND absent IS NOT NULL AND deferred IS NOT NULL AND absent_uncertain IS NOT NULL
 AND deleted>=0 AND absent>=0 AND deferred>=0 AND absent_uncertain>=0 AND absent_uncertain<=absent
 AND deleted+absent+deferred=row_count))
);
CREATE UNIQUE INDEX deletion_one_pending ON archive_deletion_batches(job_id) WHERE state='pending';
