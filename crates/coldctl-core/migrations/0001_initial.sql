CREATE TABLE installation (
    id TEXT PRIMARY KEY NOT NULL,
    initialized_at TEXT NOT NULL,
    cli_version TEXT NOT NULL
);
-- One installation per database, including concurrent init calls.
CREATE UNIQUE INDEX installation_singleton ON installation ((1));
