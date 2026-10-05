use super::database::{at_path, open_initialized, sql_error};
use crate::{
    error::Error,
    paths::StatePaths,
    source::{Source, SourceConnection, config::validate_name},
};
use rusqlite::{Connection, OptionalExtension, Row};

fn decode(row: &Row<'_>) -> rusqlite::Result<(String, String, String, String, String)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
    ))
}

fn source(raw: (String, String, String, String, String)) -> Result<Source, Error> {
    let (id, name, source_type, json, created_at) = raw;
    let connection: SourceConnection = serde_json::from_str(&json)
        .map_err(|_| Error::InvalidState("invalid source configuration".into()))?;
    connection.validate()?;
    validate_name(&name)?;
    if source_type != "postgres" {
        return Err(Error::InvalidState("unsupported source type".into()));
    }
    Ok(Source {
        id,
        name,
        source_type,
        connection,
        created_at,
    })
}

fn get(conn: &Connection, name: &str) -> Result<Source, Error> {
    let raw = conn.query_row("SELECT id, name, source_type, connection_json, created_at FROM sources WHERE name = ?1", [name], decode)
        .optional().map_err(sql_error)?.ok_or(Error::SourceNotFound)?;
    source(raw)
}

pub fn add(paths: &StatePaths, name: &str, connection: SourceConnection) -> Result<Source, Error> {
    validate_name(name)?;
    connection.validate()?;
    let conn = open_initialized(paths, true)?;
    let json = serde_json::to_string(&connection)
        .map_err(|_| Error::SourceConfiguration("unable to encode connection configuration"))?;
    let changed = conn.execute("INSERT INTO sources (id, name, source_type, connection_json, created_at) VALUES (?1, ?2, 'postgres', ?3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) ON CONFLICT(name) DO NOTHING",
        (uuid::Uuid::new_v4().to_string(), name, json)).map_err(|e| at_path(sql_error(e), paths))?;
    if changed == 0 {
        return Err(Error::SourceExists);
    }
    get(&conn, name).map_err(|e| at_path(e, paths))
}

pub fn list(paths: &StatePaths) -> Result<Vec<Source>, Error> {
    let conn = open_initialized(paths, false)?;
    (|| {
        let mut statement = conn.prepare("SELECT id, name, source_type, connection_json, created_at FROM sources ORDER BY name").map_err(sql_error)?;
        statement.query_map([], decode).map_err(sql_error)?
            .map(|row| source(row.map_err(sql_error)?)).collect::<Result<Vec<_>, Error>>()
    })().map_err(|e| at_path(e, paths))
}

pub fn show(paths: &StatePaths, name: &str) -> Result<Source, Error> {
    validate_name(name)?;
    get(&open_initialized(paths, false)?, name).map_err(|e| at_path(e, paths))
}

/// Removes configuration only. This function never opens a PostgreSQL connection.
pub fn remove(paths: &StatePaths, name: &str) -> Result<(), Error> {
    validate_name(name)?;
    let conn = open_initialized(paths, true)?;
    let changed = conn
        .execute("DELETE FROM sources WHERE name = ?1", [name])
        .map_err(|e| at_path(sql_error(e), paths))?;
    if changed == 0 {
        return Err(Error::SourceNotFound);
    }
    Ok(())
}
