CREATE TABLE destinations (
    id TEXT PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    destination_type TEXT NOT NULL CHECK (destination_type = 'local'),
    path TEXT NOT NULL,
    created_at TEXT NOT NULL
);
