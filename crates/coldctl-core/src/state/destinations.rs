use super::{
    database::{at_path, open_initialized, sql_error},
    migrations,
};
use crate::{
    destination::{Destination, local::resolve_path, validate_name},
    error::Error,
    paths::StatePaths,
};
use rusqlite::{Connection, OptionalExtension, Row};
use std::path::{Path, PathBuf};

fn open(paths: &StatePaths, writable: bool) -> Result<Connection, Error> {
    let conn = open_initialized(paths, writable)?;
    if migrations::version(&conn).map_err(|e| at_path(e, paths))? < 3 {
        return Err(Error::InvalidState(
            "destination schema needs an upgrade; run `coldctl init`".into(),
        ));
    }
    Ok(conn)
}

fn decode(row: &Row<'_>) -> rusqlite::Result<Destination> {
    Ok(Destination {
        id: row.get(0)?,
        name: row.get(1)?,
        destination_type: row.get(2)?,
        path: PathBuf::from(row.get::<_, String>(3)?),
        created_at: row.get(4)?,
    })
}

fn validate(destination: Destination) -> Result<Destination, Error> {
    validate_name(&destination.name)?;
    if destination.destination_type != "local" || !destination.path.is_absolute() {
        return Err(Error::InvalidState(
            "invalid local destination configuration".into(),
        ));
    }
    resolve_path(&destination.path)?;
    Ok(destination)
}

fn get(conn: &Connection, name: &str) -> Result<Destination, Error> {
    let destination = conn
        .query_row(
            "SELECT id, name, destination_type, path, created_at FROM destinations WHERE name = ?1",
            [name],
            decode,
        )
        .optional()
        .map_err(sql_error)?
        .ok_or(Error::DestinationNotFound)?;
    validate(destination)
}

pub fn add_local(paths: &StatePaths, name: &str, path: &Path) -> Result<Destination, Error> {
    validate_name(name)?;
    let root = resolve_path(path)?;
    let root = root.to_str().ok_or(Error::DestinationConfiguration(
        "resolved path must be valid Unicode",
    ))?;
    let conn = open(paths, true)?;
    let changed = conn.execute("INSERT INTO destinations (id, name, destination_type, path, created_at) VALUES (?1, ?2, 'local', ?3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) ON CONFLICT(name) DO NOTHING",
        (uuid::Uuid::new_v4().to_string(), name, root)).map_err(|e| at_path(sql_error(e), paths))?;
    if changed == 0 {
        return Err(Error::DestinationExists);
    }
    get(&conn, name).map_err(|e| at_path(e, paths))
}

pub fn list(paths: &StatePaths) -> Result<Vec<Destination>, Error> {
    let conn = open(paths, false)?;
    (|| {
        let mut statement = conn.prepare("SELECT id, name, destination_type, path, created_at FROM destinations ORDER BY name").map_err(sql_error)?;
        statement.query_map([], decode).map_err(sql_error)?
            .map(|row| validate(row.map_err(sql_error)?)).collect::<Result<Vec<_>, Error>>()
    })().map_err(|e| at_path(e, paths))
}

pub fn show(paths: &StatePaths, name: &str) -> Result<Destination, Error> {
    validate_name(name)?;
    get(&open(paths, false)?, name).map_err(|e| at_path(e, paths))
}

/// Only local configuration is deleted; no destination filesystem operations occur.
pub fn remove(paths: &StatePaths, name: &str) -> Result<(), Error> {
    validate_name(name)?;
    let conn = open(paths, true)?;
    let changed = conn
        .execute("DELETE FROM destinations WHERE name = ?1", [name])
        .map_err(|e| at_path(sql_error(e), paths))?;
    if changed == 0 {
        return Err(Error::DestinationNotFound);
    }
    Ok(())
}
