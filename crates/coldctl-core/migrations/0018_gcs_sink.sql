-- Initialization rebuilds schemas with foreign keys disabled on its private
-- connection. Preserve destination IDs and every referring policy/job/queue.
CREATE TABLE destinations_v3 (
 id TEXT PRIMARY KEY NOT NULL,
 name TEXT NOT NULL UNIQUE,
 destination_type TEXT NOT NULL CHECK(destination_type IN ('local','s3','azure','gcs')),
 path TEXT NOT NULL,
 created_at TEXT NOT NULL,
 config_json TEXT
);
INSERT INTO destinations_v3 SELECT id,name,destination_type,path,created_at,config_json FROM destinations;
DROP TABLE destinations;
ALTER TABLE destinations_v3 RENAME TO destinations;
