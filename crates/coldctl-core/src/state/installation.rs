use super::database::sql_error;
use crate::error::Error;
use rusqlite::Connection;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Installation {
    pub id: uuid::Uuid,
    pub initialized_at: String,
    pub cli_version: String,
}

#[derive(Debug)]
pub struct InitOutcome {
    pub installation: Installation,
    pub created: bool,
}

pub(super) fn read(conn: &Connection) -> Result<Option<Installation>, Error> {
    let mut statement = conn
        .prepare("SELECT id, initialized_at, cli_version FROM installation LIMIT 2")
        .map_err(sql_error)?;
    let mut rows = statement.query([]).map_err(sql_error)?;
    let installation = match rows.next().map_err(sql_error)? {
        None => return Ok(None),
        Some(row) => {
            let id: String = row.get(0).map_err(sql_error)?;
            Installation {
                id: uuid::Uuid::parse_str(&id)
                    .map_err(|_| Error::InvalidState("installation ID is not a UUID".into()))?,
                initialized_at: row.get(1).map_err(sql_error)?,
                cli_version: row.get(2).map_err(sql_error)?,
            }
        }
    };
    if rows.next().map_err(sql_error)?.is_some() {
        return Err(Error::InvalidState("multiple installation records".into()));
    }
    Ok(Some(installation))
}
