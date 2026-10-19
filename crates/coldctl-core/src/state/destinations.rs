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
    if migrations::version(&conn).map_err(|e| at_path(e, paths))? < 18 {
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
        config: row
            .get::<_, Option<String>>(5)?
            .map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(|_| rusqlite::Error::InvalidQuery)?,
    })
}

fn validate(destination: Destination) -> Result<Destination, Error> {
    validate_name(&destination.name)?;
    if !destination.path.is_absolute() {
        return Err(Error::InvalidState(
            "invalid local destination configuration".into(),
        ));
    }
    resolve_path(&destination.path)?;
    match (&destination.config, destination.destination_type.as_str()) {
        (None, "local") => (),
        (Some(config), kind) if kind == config.kind() => config.validate()?,
        _ => {
            return Err(Error::DestinationConfiguration(
                "invalid destination type/configuration",
            ));
        }
    }
    Ok(destination)
}

fn get(conn: &Connection, name: &str) -> Result<Destination, Error> {
    let destination = conn
        .query_row(
            "SELECT id, name, destination_type, path, created_at, config_json FROM destinations WHERE name = ?1",
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
        let mut statement = conn.prepare("SELECT id, name, destination_type, path, created_at, config_json FROM destinations ORDER BY name").map_err(sql_error)?;
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

/// Persist only credential references, never resolved secrets.
pub fn add_s3(
    paths: &StatePaths,
    name: &str,
    staging: &Path,
    config: crate::destination::s3::S3Config,
) -> Result<Destination, Error> {
    add_remote(paths, name, staging, config.into())
}

/// Persist provider configuration, with credential references only.
pub fn add_remote(
    paths: &StatePaths,
    name: &str,
    staging: &Path,
    config: crate::destination::DestinationConfig,
) -> Result<Destination, Error> {
    validate_name(name)?;
    config.validate()?;
    let staging = resolve_path(staging)?;
    let conn = open(paths, true)?;
    let changed = conn.execute("INSERT INTO destinations (id,name,destination_type,path,created_at,config_json) VALUES (?1,?2,?5,?3,strftime('%Y-%m-%dT%H:%M:%fZ','now'),?4) ON CONFLICT(name) DO NOTHING", (uuid::Uuid::new_v4().to_string(), name, staging.to_str().ok_or(Error::DestinationConfiguration("invalid staging path"))?, serde_json::to_string(&config).map_err(|_| Error::DestinationConfiguration("invalid configuration"))?, config.kind())).map_err(sql_error)?;
    if changed == 0 {
        return Err(Error::DestinationExists);
    }
    get(&conn, name)
}
