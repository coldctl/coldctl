use super::{InitOutcome, Installation, installation, migrations};
use crate::{error::Error, paths::StatePaths};
use rusqlite::{Connection, OpenFlags, TransactionBehavior};
use std::{path::PathBuf, time::Duration};

pub(super) fn sql_error(source: rusqlite::Error) -> Error {
    Error::Database {
        path: PathBuf::from("state.db"),
        source,
    }
}

pub(super) fn at_path(error: Error, paths: &StatePaths) -> Error {
    match error {
        Error::Database { source, .. } => Error::Database {
            path: paths.database(),
            source,
        },
        other => other,
    }
}

/// Source commands never implicitly create an installation or upgrade its schema.
pub(super) fn open_initialized(paths: &StatePaths, writable: bool) -> Result<Connection, Error> {
    if status(paths)?.is_none() {
        return Err(Error::NotInitialized);
    }
    let flags = if writable {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    } else {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    };
    let conn = Connection::open_with_flags(paths.database(), flags)
        .map_err(|e| at_path(sql_error(e), paths))?;
    conn.pragma_update(None, "foreign_keys", true)
        .map_err(|e| at_path(sql_error(e), paths))?;
    conn.busy_timeout(Duration::from_secs(5))
        .map_err(|e| at_path(sql_error(e), paths))?;
    if migrations::version(&conn).map_err(|e| at_path(e, paths))? < 2 {
        return Err(Error::InvalidState(
            "source schema needs an upgrade; run `coldctl init`".into(),
        ));
    }
    Ok(conn)
}

pub fn initialize(paths: &StatePaths, cli_version: &str) -> Result<InitOutcome, Error> {
    crate::fs_security::create_dir(&paths.data_dir)?;
    crate::fs_security::sqlite(&paths.database())?;
    (|| {
        let mut conn = Connection::open(paths.database()).map_err(sql_error)?;
        conn.busy_timeout(Duration::from_secs(5)).map_err(sql_error)?;
        // Lock before inspecting state. Migrations and identity commit together.
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate).map_err(sql_error)?;
        migrations::apply(&tx)?;
        let existing = installation::read(&tx)?;
        let created = existing.is_none();
        if created {
            tx.execute("INSERT INTO installation (id, initialized_at, cli_version) VALUES (?1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?2)",
                (uuid::Uuid::new_v4().to_string(), cli_version)).map_err(sql_error)?;
        }
        let installation = installation::read(&tx)?
            .ok_or_else(|| Error::InvalidState("missing installation record".into()))?;
        tx.commit().map_err(sql_error)?;
        Ok(InitOutcome { installation, created })
    })().map_err(|error| at_path(error, paths))
}

/// Inspect local state without creating directories, migrating, or contacting the cloud.
pub fn status(paths: &StatePaths) -> Result<Option<Installation>, Error> {
    crate::fs_security::check_sqlite(&paths.database())?;
    if !paths
        .database()
        .try_exists()
        .map_err(|source| Error::Filesystem {
            path: paths.database(),
            source,
        })?
    {
        return Ok(None);
    }
    (|| {
        let conn = Connection::open_with_flags(paths.database(), OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(sql_error)?;
        let has_migrations: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'schema_migrations')", [], |row| row.get(0)).map_err(sql_error)?;
        if !has_migrations || migrations::version(&conn)? == 0 {
            return Ok(None);
        }
        installation::read(&conn)
    })().map_err(|error| at_path(error, paths))
}
