use crate::{
    archive::manifest::ManifestObject, destination::StoredObject, error::Error, paths::StatePaths,
    state::archive as store,
};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

fn db_error(_: rusqlite::Error) -> Error {
    Error::Archive("cannot read or persist archive checkpoint")
}

/// A separate SQLite write lock is held for the worker's lifetime. OS locks are
/// released after a crash; the persistent lock file is deliberately never removed.
pub(crate) fn lock(paths: &StatePaths, id: &str) -> Result<Connection, Error> {
    uuid::Uuid::parse_str(id).map_err(|_| Error::Archive("invalid job ID"))?;
    store::job_show(paths, id)?;
    scoped_lock(paths, "job-locks", id)
}

pub(crate) fn policy_lock(paths: &StatePaths, id: &str) -> Result<Connection, Error> {
    uuid::Uuid::parse_str(id).map_err(|_| Error::Archive("invalid policy ID"))?;
    scoped_lock(paths, "policy-locks", id)
}

fn scoped_lock(paths: &StatePaths, scope: &str, id: &str) -> Result<Connection, Error> {
    let directory = paths.data_dir.join(scope);
    crate::fs_security::create_dir(&directory)?;
    let database = directory.join(format!("{id}.db"));
    crate::fs_security::sqlite(&database)?;
    let conn = Connection::open(database).map_err(db_error)?;
    conn.busy_timeout(std::time::Duration::ZERO)
        .map_err(db_error)?;
    conn.execute_batch("BEGIN IMMEDIATE")
        .map_err(|_| Error::Archive("job or policy is busy; another worker or verifier owns it"))?;
    Ok(conn)
}

pub(crate) struct Checkpoint {
    pub source_id: String,
    pub source_identity: String,
    pub upper: Option<i64>,
    pub manifest: Option<StoredObject>,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct Entry {
    pub batch: ManifestObject,
    pub staged_key: String,
}
pub(crate) fn load(paths: &StatePaths, id: &str) -> Result<Checkpoint, Error> {
    let raw: Option<(String, String, Option<i64>, Option<String>)> = store::open(paths, false)?
        .query_row(
            "SELECT source_id,source_identity,upper_key,manifest_json FROM archive_checkpoints WHERE job_id=?1",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(db_error)?;
    let (source_id, source_identity, upper, manifest) = raw.ok_or(Error::Archive(
        "job has no durable checkpoint; legacy or uninitialized jobs cannot be resumed or verified",
    ))?;
    Ok(Checkpoint {
        source_id,
        source_identity,
        upper,
        manifest: manifest
            .map(|s| {
                serde_json::from_str(&s).map_err(|_| Error::Archive("invalid manifest checkpoint"))
            })
            .transpose()?,
    })
}
pub(crate) fn entry(
    paths: &StatePaths,
    id: &str,
    sequence: i64,
) -> Result<Option<(Entry, bool)>, Error> {
    let raw: Option<(String, bool)> = store::open(paths, false)?
        .query_row(
            "SELECT metadata_json,committed FROM archive_objects WHERE job_id=?1 AND sequence=?2",
            (id, sequence),
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(db_error)?;
    raw.map(|(json, committed)| {
        Ok((
            serde_json::from_str(&json).map_err(|_| Error::Archive("invalid batch checkpoint"))?,
            committed,
        ))
    })
    .transpose()
}
pub(crate) fn prepare(
    paths: &StatePaths,
    id: &str,
    sequence: i64,
    entry: &Entry,
) -> Result<(), Error> {
    let json =
        serde_json::to_string(entry).map_err(|_| Error::Archive("cannot encode checkpoint"))?;
    store::open(paths, true)?
        .execute(
            "INSERT INTO archive_objects(job_id,sequence,metadata_json) VALUES(?1,?2,?3)",
            (id, sequence, json),
        )
        .map_err(db_error)?;
    Ok(())
}
pub(crate) fn commit(
    paths: &StatePaths,
    id: &str,
    sequence: i64,
    entry: &Entry,
) -> Result<(), Error> {
    let mut conn = store::open(paths, true)?;
    let tx = conn.transaction().map_err(db_error)?;
    let changed=tx.execute("UPDATE archive_objects SET committed=1 WHERE job_id=?1 AND sequence=?2 AND committed=0",(id,sequence)).map_err(db_error)?;
    if changed == 1 {
        let bytes = i64::try_from(entry.batch.object.bytes)
            .map_err(|_| Error::Archive("archive byte count overflow"))?;
        let changed=tx.execute("UPDATE jobs SET rows_processed=rows_processed+?2,bytes_written=bytes_written+?3,objects_created=objects_created+1,last_key=?4 WHERE id=?1 AND status='running' AND objects_created=?5",(id,entry.batch.rows,bytes,entry.batch.last_key,sequence-1)).map_err(db_error)?;
        if changed != 1 {
            return Err(Error::Archive("job checkpoint is inconsistent"));
        }
    }
    tx.commit().map_err(db_error)
}
pub(crate) fn manifest(paths: &StatePaths, id: &str, object: &StoredObject) -> Result<(), Error> {
    let json = serde_json::to_string(object)
        .map_err(|_| Error::Archive("cannot encode manifest checkpoint"))?;
    store::open(paths, true)?
        .execute(
            "UPDATE archive_checkpoints SET manifest_json=?2 WHERE job_id=?1",
            (id, json),
        )
        .map_err(db_error)?;
    Ok(())
}
pub(crate) fn restart(paths: &StatePaths, id: &str) -> Result<(), Error> {
    store::open(paths,true)?.execute("UPDATE jobs SET status='running',completed_at=NULL,error=NULL,failure_json=NULL,progress_json=NULL,cancel_requested=0,verified_at=NULL WHERE id=?1 AND status!='completed'",[id]).map_err(db_error)?;
    Ok(())
}
pub(crate) fn cancelled(paths: &StatePaths, id: &str) -> Result<bool, Error> {
    let conn = store::open(paths, true)?;
    let changed=conn.execute("UPDATE jobs SET status='cancelled',completed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?1 AND status='running' AND cancel_requested=1",[id]).map_err(db_error)?;
    Ok(changed == 1)
}
pub fn cancel(paths: &StatePaths, id: &str) -> Result<store::Job, Error> {
    store::job_show(paths, id)?;
    store::open(paths, true)?
        .execute(
            "UPDATE jobs SET cancel_requested=1 WHERE id=?1 AND status!='completed'",
            [id],
        )
        .map_err(db_error)?;
    if let Ok(_guard) = lock(paths, id) {
        store::open(paths,true)?.execute("UPDATE jobs SET status='cancelled',completed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?1 AND status!='completed'",[id]).map_err(db_error)?;
    }
    store::job_show(paths, id)
}
pub(crate) fn verified(paths: &StatePaths, id: &str, success: bool) -> Result<(), Error> {
    store::open(paths,true)?.execute("UPDATE jobs SET verified_at=CASE WHEN ?2 THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') ELSE NULL END WHERE id=?1",(id,success)).map_err(db_error)?;
    Ok(())
}

pub(crate) fn pause(paths: &StatePaths, id: &str, reason: &str) -> Result<(), Error> {
    store::open(paths,true)?.execute("UPDATE jobs SET status='paused',error=?2,completed_at=NULL WHERE id=?1 AND status='running'",(id,reason)).map_err(db_error)?;
    Ok(())
}
pub(crate) fn execution(
    paths: &StatePaths,
    id: &str,
    controls: &super::controls::ExecutionControls,
) -> Result<(), Error> {
    controls.validate()?;
    let json = serde_json::to_string(controls)
        .map_err(|_| Error::Archive("cannot encode execution controls"))?;
    store::open(paths, true)?
        .execute("UPDATE jobs SET execution_json=?2 WHERE id=?1", (id, json))
        .map_err(db_error)?;
    Ok(())
}
