use super::database::{open_initialized, sql_error};
use crate::{error::Error, paths::StatePaths};
use rusqlite::OptionalExtension;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Progress {
    pub job_id: String,
    pub manifest_sha256: String,
    pub batches: i64,
    pub rows_processed: i64,
    pub rows_deleted: i64,
    /// Includes previously absent rows and deletions whose commit acknowledgement was lost.
    pub rows_absent: i64,
    pub completed: bool,
}
fn open(paths: &StatePaths, write: bool) -> Result<rusqlite::Connection, Error> {
    let conn = open_initialized(paths, write)?;
    if super::migrations::version(&conn)? < 13 {
        return Err(Error::Archive(
            "deletion schema needs an upgrade; run `coldctl init`",
        ));
    }
    Ok(conn)
}
pub fn show(paths: &StatePaths, id: &str) -> Result<Option<Progress>, Error> {
    open(paths, false)?.query_row("SELECT job_id,manifest_sha256,batches,rows_processed,rows_deleted,rows_absent,completed FROM archive_deletions WHERE job_id=?1", [id], |r| Ok(Progress {
        job_id:r.get(0)?,manifest_sha256:r.get(1)?,batches:r.get(2)?,rows_processed:r.get(3)?,rows_deleted:r.get(4)?,rows_absent:r.get(5)?,completed:r.get(6)?,
    })).optional().map_err(sql_error)
}
pub(crate) fn start(paths: &StatePaths, id: &str, hash: &str) -> Result<Progress, Error> {
    open(paths,true)?.execute("INSERT INTO archive_deletions(job_id,manifest_sha256) VALUES(?1,?2) ON CONFLICT(job_id) DO NOTHING", (id,hash)).map_err(sql_error)?;
    let saved = show(paths, id)?.ok_or(Error::Archive("missing deletion checkpoint"))?;
    if saved.manifest_sha256 != hash {
        return Err(Error::Archive("deletion manifest checkpoint mismatch"));
    }
    Ok(saved)
}
pub(crate) fn advance(
    paths: &StatePaths,
    id: &str,
    previous: i64,
    deleted: i64,
    absent: i64,
    completed: bool,
) -> Result<Progress, Error> {
    if deleted < 0 || absent < 0 {
        return Err(Error::Archive("invalid deletion counts"));
    }
    let changed = open(paths,true)?.execute("UPDATE archive_deletions SET batches=batches+1,rows_processed=rows_processed+?3+?4,rows_deleted=rows_deleted+?3,rows_absent=rows_absent+?4,completed=?5,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE job_id=?1 AND batches=?2 AND completed=0", (id,previous,deleted,absent,completed)).map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::Archive("deletion checkpoint changed unexpectedly"));
    }
    show(paths, id)?.ok_or(Error::Archive("missing deletion checkpoint"))
}
pub(crate) fn finish_empty(paths: &StatePaths, id: &str) -> Result<Progress, Error> {
    open(paths,true)?.execute("UPDATE archive_deletions SET completed=1 WHERE job_id=?1 AND batches=0 AND rows_processed=0",[id]).map_err(sql_error)?;
    show(paths, id)?.ok_or(Error::Archive("missing deletion checkpoint"))
}
