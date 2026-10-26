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
        v if v == (1..=20).collect::<Vec<_>>().as_slice() => Ok(20),
        [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
        ] => Ok(19),
        [] => Ok(0),
        [1] => Ok(1),
        [1, 2] => Ok(2),
        [1, 2, 3] => Ok(3),
        [1, 2, 3, 4] => Ok(4),
        [1, 2, 3, 4, 5] => Ok(5),
        [1, 2, 3, 4, 5, 6] => Ok(6),
        [1, 2, 3, 4, 5, 6, 7] => Ok(7),
        [1, 2, 3, 4, 5, 6, 7, 8] => Ok(8),
        [1, 2, 3, 4, 5, 6, 7, 8, 9] => Ok(9),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] => Ok(10),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11] => Ok(11),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12] => Ok(12),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13] => Ok(13),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14] => Ok(14),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15] => Ok(15),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] => Ok(16),
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17] => Ok(17),
        [
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
        ] => Ok(18),
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
    if version(conn)? == 8 {
        conn.execute_batch(include_str!("../../migrations/0009_mysql.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(9,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 9 {
        conn.execute_batch(include_str!("../../migrations/0010_document_cursors.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(10,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 10 {
        conn.execute_batch(include_str!("../../migrations/0011_mongodb.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(11,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 11 {
        conn.execute_batch(include_str!("../../migrations/0012_scheduler.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(12,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 12 {
        conn.execute_batch(include_str!("../../migrations/0013_deletion.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(13,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 13 {
        conn.execute_batch(include_str!("../../migrations/0014_sinks.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(14,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 14 {
        conn.execute_batch(include_str!("../../migrations/0015_azure_sink.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(15,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 15 {
        conn.execute_batch(include_str!("../../migrations/0016_recurring.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(16,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 16 {
        conn.execute_batch(include_str!("../../migrations/0017_monitoring.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(17,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 17 {
        conn.execute_batch(include_str!("../../migrations/0018_gcs_sink.sql"))
            .map_err(sql_error)?;
        let invalid = conn
            .prepare("PRAGMA foreign_key_check")
            .map_err(sql_error)?
            .exists([])
            .map_err(sql_error)?;
        if invalid {
            return Err(Error::InvalidState(
                "GCS migration found invalid foreign keys".into(),
            ));
        }
        conn.execute(
            "INSERT INTO schema_migrations VALUES(18,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 18 {
        conn.execute_batch(include_str!("../../migrations/0019_recurring_groups.sql"))
            .map_err(sql_error)?;
        conn.execute(
            "INSERT INTO schema_migrations VALUES(19,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    if version(conn)? == 19 {
        let invalid: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM archive_deletions WHERE rows_processed != rows_deleted+rows_absent OR length(manifest_sha256)!=64)", [], |r|r.get(0)).map_err(sql_error)?;
        if invalid {
            return Err(Error::InvalidState(
                "invalid deletion history; refusing migration".into(),
            ));
        }
        conn.execute_batch(include_str!("../../migrations/0020_live_deletion.sql"))
            .map_err(sql_error)?;
        if conn
            .prepare("PRAGMA foreign_key_check")
            .map_err(sql_error)?
            .exists([])
            .map_err(sql_error)?
        {
            return Err(Error::InvalidState(
                "live deletion migration found invalid foreign keys".into(),
            ));
        }
        conn.execute(
            "INSERT INTO schema_migrations VALUES(20,strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            [],
        )
        .map_err(sql_error)?;
    }
    Ok(())
}
