CREATE TABLE sources (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    source_type TEXT NOT NULL CHECK (source_type = 'postgres'),
    connection_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
