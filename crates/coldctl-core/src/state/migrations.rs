use super::database::sql_error;
use crate::error::Error;
use rusqlite::Connection;

pub(super) fn version(conn: &Connection) -> Result<i64, Error> {
    let versions = conn
        .prepare("SELECT version FROM schema_migrations ORDER BY version")
        .map_err(sql_error)?
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    match versions.as_slice() {
        [] => Ok(0),
        [1] => Ok(1),
        [1, 2] => Ok(2),
        [1, 2, 3] => Ok(3),
        [1, 2, 3, 4] => Ok(4),
        [1, 2, 3, 4, 5] => Ok(5),
        [1, 2, 3, 4, 5, 6] => Ok(6),
        [1, 2, 3, 4, 5, 6, 7] => Ok(7),
        [1, 2, 3, 4, 5, 6, 7, 8] => Ok(8),
        _ => Err(Error::InvalidState(
            "unsupported migration history; use a compatible Coldctl version".into(),
        )),
    }
}

pub(super) fn apply(conn: &Connection) -> Result<(), Error> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);")
        .map_err(sql_error)?;
    if version(conn)? == 0 {
        conn.execute_batch(include_str!("../../migrations/0001_initial.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES (1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 1 {
        conn.execute_batch(include_str!("../../migrations/0002_sources.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES (2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 2 {
        conn.execute_batch(include_str!("../../migrations/0003_destinations.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES (3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 3 {
        conn.execute_batch(include_str!("../../migrations/0004_archive.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES (4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 4 {
        conn.execute_batch(include_str!("../../migrations/0005_durability.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES (5, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 5 {
        conn.execute_batch(include_str!("../../migrations/0006_execution.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES (6, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 6 {
        conn.execute_batch(include_str!("../../migrations/0007_observability.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES (7, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 7 {
        conn.execute_batch(include_str!("../../migrations/0008_recovery.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES (8, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    Ok(())
}
