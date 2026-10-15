CREATE TABLE operational_events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, entity TEXT NOT NULL,
 failed INTEGER NOT NULL, occurred_at INTEGER NOT NULL
);
CREATE TRIGGER operational_events_bound AFTER INSERT ON operational_events BEGIN
 DELETE FROM operational_events WHERE seq <= NEW.seq - 10000;
END;
CREATE TRIGGER monitor_jobs_insert AFTER INSERT ON jobs WHEN (NEW.status='failed' OR NEW.failure_json IS NOT NULL) BEGIN
 INSERT INTO operational_events(kind,entity,failed,occurred_at) VALUES('job',NEW.id,1,unixepoch());
END;
CREATE TRIGGER monitor_jobs_update AFTER UPDATE ON jobs WHEN (NEW.status='failed' OR NEW.failure_json IS NOT NULL) != (OLD.status='failed' OR OLD.failure_json IS NOT NULL) BEGIN
 INSERT INTO operational_events(kind,entity,failed,occurred_at) VALUES('job',NEW.id,(NEW.status='failed' OR NEW.failure_json IS NOT NULL),unixepoch());
END;
INSERT INTO operational_events(kind,entity,failed,occurred_at) SELECT 'job',id,1,unixepoch() FROM jobs AS NEW WHERE (NEW.status='failed' OR NEW.failure_json IS NOT NULL);
CREATE TRIGGER monitor_archive_queue_insert AFTER INSERT ON archive_queue WHEN (NEW.status='failed' OR NEW.last_error IS NOT NULL) BEGIN
 INSERT INTO operational_events(kind,entity,failed,occurred_at) VALUES('queue',NEW.id,1,unixepoch());
END;
CREATE TRIGGER monitor_archive_queue_update AFTER UPDATE ON archive_queue WHEN (NEW.status='failed' OR NEW.last_error IS NOT NULL) != (OLD.status='failed' OR OLD.last_error IS NOT NULL) BEGIN
 INSERT INTO operational_events(kind,entity,failed,occurred_at) VALUES('queue',NEW.id,(NEW.status='failed' OR NEW.last_error IS NOT NULL),unixepoch());
END;
INSERT INTO operational_events(kind,entity,failed,occurred_at) SELECT 'queue',id,1,unixepoch() FROM archive_queue AS NEW WHERE (NEW.status='failed' OR NEW.last_error IS NOT NULL);
CREATE TRIGGER monitor_recurring_archives_insert AFTER INSERT ON recurring_archives WHEN (NEW.status='failed') BEGIN
 INSERT INTO operational_events(kind,entity,failed,occurred_at) VALUES('recurring',NEW.id,1,unixepoch());
END;
CREATE TRIGGER monitor_recurring_archives_update AFTER UPDATE ON recurring_archives WHEN (NEW.status='failed') != (OLD.status='failed') BEGIN
 INSERT INTO operational_events(kind,entity,failed,occurred_at) VALUES('recurring',NEW.id,(NEW.status='failed'),unixepoch());
END;
INSERT INTO operational_events(kind,entity,failed,occurred_at) SELECT 'recurring',id,1,unixepoch() FROM recurring_archives AS NEW WHERE (NEW.status='failed');
