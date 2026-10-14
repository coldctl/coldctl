//! Durable queue metadata. Archive checkpoints remain the authority for data progress.
use super::database::{at_path, open_initialized, sql_error};
use crate::{
    archive::{controls::ExecutionControls, planner::SourceStability},
    error::Error,
    paths::StatePaths,
};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Options {
    /// Explicitly authorize verified source deletion after archival.
    #[serde(default)]
    pub delete_after_archive: bool,
    pub source_stability: SourceStability,
    pub allow_repeat: bool,
    /// Explicit authorization to resume automatically against the original stable source.
    pub confirm_original_source: bool,
    pub execution: ExecutionControls,
    pub max_retries: u32,
}
impl Options {
    pub fn validate(&self) -> Result<(), Error> {
        self.execution.validate()?;
        if !self.confirm_original_source
            || self.max_retries > 100
            || self.execution.max_duration_seconds.is_none()
        {
            return Err(Error::Archive(
                "scheduler requires original-source confirmation, a positive execution time slice, and at most 100 retries",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Serialize)]
pub struct Entry {
    pub id: String,
    pub policy_name: String,
    pub job_id: Option<String>,
    pub options: Options,
    pub status: String,
    /// Unix timestamp in UTC.
    pub due_at: i64,
    pub failures: u32,
    pub last_error: Option<String>,
    pub created_at: String,
    pub recurring_id: Option<String>,
    pub cutoff_utc: Option<String>,
}

fn open(paths: &StatePaths, writable: bool) -> Result<Connection, Error> {
    let conn = open_initialized(paths, writable)?;
    if super::migrations::version(&conn)? < 16 {
        return Err(Error::Archive(
            "scheduler schema needs an upgrade; run `coldctl init`",
        ));
    }
    Ok(conn)
}

fn row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Entry> {
    let json: String = row.get(3)?;
    let options: Options =
        serde_json::from_str(&json).map_err(|_| rusqlite::Error::InvalidQuery)?;
    options
        .validate()
        .map_err(|_| rusqlite::Error::InvalidQuery)?;
    Ok(Entry {
        id: row.get(0)?,
        policy_name: row.get(1)?,
        job_id: row.get(2)?,
        options,
        status: row.get(4)?,
        due_at: row.get(5)?,
        failures: row.get(6)?,
        last_error: row.get(7)?,
        created_at: row.get(8)?,
        recurring_id: row.get(9)?,
        cutoff_utc: row.get(10)?,
    })
}
const SELECT: &str = "SELECT id,policy_name,job_id,options_json,status,due_at,failures,last_error,created_at,recurring_id,cutoff_utc FROM archive_queue";

pub fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

/// Enqueue one archive, or adopt an existing incomplete job. No source connection is opened.
pub fn enqueue(
    paths: &StatePaths,
    policy: &str,
    job_id: Option<&str>,
    options: Options,
    delay_seconds: u64,
) -> Result<Entry, Error> {
    options.validate()?;
    if delay_seconds > 31_536_000 {
        return Err(Error::Archive("schedule delay must be at most one year"));
    }
    let policy = super::archive::policy_show(paths, policy)?;
    let reserved: bool = open(paths, false)?.query_row("SELECT EXISTS(SELECT 1 FROM recurring_archives WHERE policy_id=?1 AND status!='stopped')", [&policy.id], |r| r.get(0)).map_err(sql_error)?;
    if reserved {
        return Err(Error::Archive(
            "policy belongs to a recurring schedule; use recurring retry or stop",
        ));
    }
    if let Some(id) = job_id {
        let job = super::archive::job_show(paths, id)?;
        if job.plan.policy.id != policy.id
            || job.status == super::archive::JobStatus::Completed
            || job.imported
        {
            return Err(Error::Archive(
                "only an incomplete original job for this policy can be queued",
            ));
        }
        if job.cancel_requested || job.status == super::archive::JobStatus::Cancelled {
            return Err(Error::Archive(
                "cancelled jobs require explicit manual resume before scheduling",
            ));
        }
    }
    let id = uuid::Uuid::new_v4().to_string();
    let json = serde_json::to_string(&options)
        .map_err(|_| Error::Archive("cannot encode scheduler options"))?;
    open(paths, true)?.execute(
        "INSERT INTO archive_queue(id,policy_id,policy_name,job_id,options_json,status,due_at) VALUES(?1,?2,?3,?4,?5,'pending',?6)",
        (&id, &policy.id, &policy.name, job_id, json, now() + delay_seconds as i64),
    ).map_err(|e| at_path(sql_error(e), paths))?;
    show(paths, &id)
}

pub fn show(paths: &StatePaths, id: &str) -> Result<Entry, Error> {
    open(paths, false)?
        .query_row(&format!("{SELECT} WHERE id=?1"), [id], row)
        .optional()
        .map_err(|e| at_path(sql_error(e), paths))?
        .ok_or(Error::Archive("queue entry not found"))
}
pub fn list(paths: &StatePaths) -> Result<Vec<Entry>, Error> {
    let conn = open(paths, false)?;
    let mut stmt = conn
        .prepare(&format!("{SELECT} ORDER BY due_at,created_at,id"))
        .map_err(sql_error)?;
    stmt.query_map([], row)
        .map_err(sql_error)?
        .map(|r| r.map_err(sql_error))
        .collect()
}

pub fn cancel(paths: &StatePaths, id: &str) -> Result<(), Error> {
    let mut conn = open(paths, true)?;
    let tx = conn.transaction().map_err(sql_error)?;
    let changed = tx.execute("UPDATE archive_queue SET status='cancelled' WHERE id=?1 AND status IN ('pending','running','failed')", [id]).map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::Archive("queue entry not found or already terminal"));
    }
    tx.execute("UPDATE jobs SET cancel_requested=1 WHERE id=(SELECT job_id FROM archive_queue WHERE id=?1) AND status!='completed'", [id]).map_err(sql_error)?;
    tx.commit().map_err(sql_error)
}

pub fn retry(paths: &StatePaths, id: &str, confirm_original_source: bool) -> Result<Entry, Error> {
    let recurring: bool = open(paths, false)?
        .query_row(
            "SELECT recurring_id IS NOT NULL FROM archive_queue WHERE id=?1",
            [id],
            |r| r.get(0),
        )
        .map_err(sql_error)?;
    if recurring {
        return Err(Error::Archive(
            "use scheduler recurring retry for a recurring cycle",
        ));
    }
    if !confirm_original_source {
        return Err(Error::Archive(
            "retry requires confirmation of the original stable source",
        ));
    }
    let changed = open(paths, true)?.execute("UPDATE archive_queue SET status='pending',due_at=?2,failures=0,last_error=NULL WHERE id=?1 AND status='failed'", (id, now())).map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::Archive("only failed queue entries can be retried"));
    }
    show(paths, id)
}

/// Only called while the worker's OS-backed lock is held.
pub(crate) fn recover(paths: &StatePaths) -> Result<(), Error> {
    open(paths, true)?
        .execute(
            "UPDATE archive_queue SET status='pending' WHERE status='running'",
            [],
        )
        .map_err(sql_error)?;
    Ok(())
}
pub(crate) fn claim(paths: &StatePaths, now: i64) -> Result<Option<Entry>, Error> {
    let mut conn = open(paths, true)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    let entry = tx.query_row(&format!("{SELECT} WHERE status='pending' AND due_at<=?1 AND (recurring_id IS NULL OR EXISTS(SELECT 1 FROM recurring_archives r WHERE r.id=recurring_id AND r.status='active')) ORDER BY due_at,created_at,id LIMIT 1"), [now], row).optional().map_err(sql_error)?;
    if let Some(entry) = &entry {
        tx.execute(
            "UPDATE archive_queue SET status='running' WHERE id=?1",
            [&entry.id],
        )
        .map_err(sql_error)?;
    }
    tx.commit().map_err(sql_error)?;
    Ok(entry)
}
pub(crate) fn finish(
    paths: &StatePaths,
    id: &str,
    status: &str,
    due_at: i64,
    failures: u32,
    error: Option<&str>,
) -> Result<(), Error> {
    open(paths, true)?.execute("UPDATE archive_queue SET status=?2,due_at=?3,failures=?4,last_error=?5 WHERE id=?1 AND status='running'",
        (id, status, due_at, failures, error)).map_err(sql_error)?;
    Ok(())
}
