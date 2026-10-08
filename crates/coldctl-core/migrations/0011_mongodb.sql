CREATE TABLE saved_sources AS SELECT * FROM sources;
CREATE TABLE saved_policies AS SELECT * FROM policies;
DROP TABLE policies;
DROP TABLE sources;
CREATE TABLE sources (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL CHECK (source_type IN ('postgres','mysql','mongodb')),
    connection_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE policies (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    source_id TEXT NOT NULL REFERENCES sources(id) ON DELETE RESTRICT,
    destination_id TEXT NOT NULL REFERENCES destinations(id) ON DELETE RESTRICT,
    config_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
INSERT INTO sources SELECT * FROM saved_sources;
INSERT INTO policies SELECT * FROM saved_policies;
DROP TABLE saved_policies;
DROP TABLE saved_sources;
