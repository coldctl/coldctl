use super::{
    database::{at_path, open_initialized, sql_error},
    migrations,
};
use crate::{
    archive::planner::ArchivePlan,
    error::Error,
    paths::StatePaths,
    policy::model::{Policy, PolicyConfig},
};
use rusqlite::{Connection, OptionalExtension, Row};
use serde::Serialize;

pub(crate) fn open(paths: &StatePaths, writable: bool) -> Result<Connection, Error> {
    let conn = open_initialized(paths, writable)?;
    if migrations::version(&conn).map_err(|e| at_path(e, paths))? < 8 {
        return Err(Error::Archive(
            "archive schema needs an upgrade; run `coldctl init`",
        ));
    }
    Ok(conn)
}

fn decode_policy(
    row: &Row<'_>,
) -> rusqlite::Result<(String, String, String, String, String, String)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}
fn policy(raw: (String, String, String, String, String, String)) -> Result<Policy, Error> {
    let (id, name, source, destination, json, created_at) = raw;
    let config: PolicyConfig =
        serde_json::from_str(&json).map_err(|_| Error::Archive("invalid stored policy"))?;
    config.validate()?;
    Ok(Policy {
        id,
        name,
        source,
        destination,
        config,
        created_at,
    })
}
const POLICIES: &str = "SELECT p.id,p.name,s.name,d.name,p.config_json,p.created_at FROM policies p JOIN sources s ON s.id=p.source_id JOIN destinations d ON d.id=p.destination_id";

pub fn policy_create(
    paths: &StatePaths,
    name: &str,
    source: &str,
    destination: &str,
    config: PolicyConfig,
) -> Result<Policy, Error> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::Archive(
            "policy name must contain 1–64 ASCII letters, digits, hyphens, or underscores",
        ));
    }
    config.validate()?;
    let conn = open(paths, true)?;
    let source = super::sources::show(paths, source)?;
    let destination = super::destinations::show(paths, destination)?;
    let json =
        serde_json::to_string(&config).map_err(|_| Error::Archive("cannot encode policy"))?;
    let changed = conn.execute("INSERT INTO policies VALUES (?1,?2,?3,?4,?5,strftime('%Y-%m-%dT%H:%M:%fZ','now')) ON CONFLICT(name) DO NOTHING", (uuid::Uuid::new_v4().to_string(),name,source.id,destination.id,json)).map_err(|e| at_path(sql_error(e),paths))?;
    if changed == 0 {
        return Err(Error::Archive("policy name already exists"));
    }
    policy_show(paths, name)
}
pub fn policy_show(paths: &StatePaths, name: &str) -> Result<Policy, Error> {
    let conn = open(paths, false)?;
    policy(
        conn.query_row(
            &format!("{POLICIES} WHERE p.name=?1"),
            [name],
            decode_policy,
        )
        .optional()
        .map_err(|e| at_path(sql_error(e), paths))?
        .ok_or(Error::Archive("policy not found"))?,
    )
}
pub fn policy_list(paths: &StatePaths) -> Result<Vec<Policy>, Error> {
    let conn = open(paths, false)?;
    let mut stmt = conn
        .prepare(&format!("{POLICIES} ORDER BY p.name"))
        .map_err(|e| at_path(sql_error(e), paths))?;
    stmt.query_map([], decode_policy)
        .map_err(|e| at_path(sql_error(e), paths))?
        .map(|row| policy(row.map_err(|e| at_path(sql_error(e), paths))?))
        .collect()
}
pub fn policy_remove(paths: &StatePaths, name: &str) -> Result<(), Error> {
    if open(paths, true)?
        .execute("DELETE FROM policies WHERE name=?1", [name])
        .map_err(|e| at_path(sql_error(e), paths))?
        == 0
    {
        return Err(Error::Archive("policy not found"));
    }
    Ok(())
}

#[derive(Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
    Paused,
}
#[derive(Debug, Serialize)]
pub struct Job {
    pub id: String,
    pub policy_name: String,
    pub plan: ArchivePlan,
    pub status: JobStatus,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub rows_processed: i64,
    pub bytes_written: i64,
    pub objects_created: i64,
    pub last_key: Option<i64>,
    pub error: Option<String>,
    pub cancel_requested: bool,
    pub verified_at: Option<String>,
    pub execution: crate::archive::controls::ExecutionControls,
    pub progress: Option<crate::archive::progress::Progress>,
    pub failure: Option<crate::error::Failure>,
    pub storage_root: std::path::PathBuf,
    pub imported: bool,
}
fn job_row(row: &Row<'_>) -> rusqlite::Result<Job> {
    let invalid = |message: &'static str| {
        rusqlite::Error::FromSqlConversionFailure(
            2,
            rusqlite::types::Type::Text,
            Box::new(Error::Archive(message)),
        )
    };
    let json: String = row.get(2)?;
    let status: String = row.get(3)?;
    let status = match status.as_str() {
        "running" => JobStatus::Running,
        "completed" => JobStatus::Completed,
        "failed" => JobStatus::Failed,
        "cancelled" => JobStatus::Cancelled,
        "paused" => JobStatus::Paused,
        _ => return Err(invalid("invalid job status")),
    };
    let plan: ArchivePlan =
        serde_json::from_str(&json).map_err(|_| invalid("invalid job snapshot"))?;
    let imported_root: Option<String> = row.get(16)?;
    Ok(Job {
        id: row.get(0)?,
        policy_name: row.get(1)?,
        storage_root: imported_root
            .as_ref()
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| plan.destination_path.clone()),
        imported: imported_root.is_some(),
        plan,
        status,
        started_at: row.get(4)?,
        completed_at: row.get(5)?,
        rows_processed: row.get(6)?,
        bytes_written: row.get(7)?,
        objects_created: row.get(8)?,
        last_key: row.get(9)?,
        error: row.get(10)?,
        cancel_requested: row.get(11)?,
        verified_at: row.get(12)?,
        progress: row
            .get::<_, Option<String>>(14)?
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(|_| invalid("invalid progress snapshot"))?,
        failure: row
            .get::<_, Option<String>>(15)?
            .map(|json| serde_json::from_str(&json))
            .transpose()
            .map_err(|_| invalid("invalid failure diagnostic"))?,
        execution: serde_json::from_str(&row.get::<_, String>(13)?)
            .map_err(|_| invalid("invalid execution controls"))?,
    })
}
const JOBS: &str = "SELECT id,policy_name,plan_json,status,started_at,completed_at,rows_processed,bytes_written,objects_created,last_key,error,cancel_requested,verified_at,execution_json,progress_json,failure_json,imported_root FROM jobs";
pub fn job_show(paths: &StatePaths, id: &str) -> Result<Job, Error> {
    let conn = open(paths, false)?;
    conn.query_row(&format!("{JOBS} WHERE id=?1"), [id], job_row)
        .optional()
        .map_err(|e| at_path(sql_error(e), paths))?
        .ok_or(Error::Archive("job not found"))
}
pub fn job_list(paths: &StatePaths) -> Result<Vec<Job>, Error> {
    let conn = open(paths, false)?;
    let mut stmt = conn
        .prepare(&format!("{JOBS} ORDER BY started_at DESC,id"))
        .map_err(|e| at_path(sql_error(e), paths))?;
    stmt.query_map([], job_row)
        .map_err(|e| at_path(sql_error(e), paths))?
        .map(|row| row.map_err(|e| at_path(sql_error(e), paths)))
        .collect()
}
pub(crate) fn job_create(
    paths: &StatePaths,
    plan: &ArchivePlan,
    source_id: &str,
    identity: &str,
    upper: Option<i64>,
    controls: &crate::archive::controls::ExecutionControls,
) -> Result<Job, Error> {
    let id = uuid::Uuid::new_v4().to_string();
    let json =
        serde_json::to_string(plan).map_err(|_| Error::Archive("cannot encode job snapshot"))?;
    let mut conn = open(paths, true)?;
    let tx = conn
        .transaction()
        .map_err(|e| at_path(sql_error(e), paths))?;
    tx.execute("INSERT INTO jobs (id,policy_name,plan_json,status,started_at) VALUES (?1,?2,?3,'running',strftime('%Y-%m-%dT%H:%M:%fZ','now'))",(&id,&plan.policy.name,json)).map_err(|e|at_path(sql_error(e),paths))?;
    tx.execute("INSERT INTO archive_checkpoints(job_id,source_id,source_identity,upper_key) VALUES(?1,?2,?3,?4)",(&id,source_id,identity,upper)).map_err(|e|at_path(sql_error(e),paths))?;
    let execution = serde_json::to_string(controls)
        .map_err(|_| Error::Archive("cannot encode execution controls"))?;
    tx.execute(
        "UPDATE jobs SET execution_json=?2 WHERE id=?1",
        (&id, execution),
    )
    .map_err(|e| at_path(sql_error(e), paths))?;
    tx.commit().map_err(|e| at_path(sql_error(e), paths))?;
    job_show(paths, &id)
}
pub(crate) fn job_finish(paths: &StatePaths, id: &str, error: Option<&str>) -> Result<(), Error> {
    open(paths,true)?.execute("UPDATE jobs SET status=?2,error=?3,completed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?1 AND status='running'",(id,if error.is_some(){"failed"}else{"completed"},error)).map_err(|e|at_path(sql_error(e),paths))?;
    Ok(())
}

pub(crate) fn job_progress(
    paths: &StatePaths,
    id: &str,
    progress: &crate::archive::progress::Progress,
) -> Result<(), Error> {
    let json =
        serde_json::to_string(progress).map_err(|_| Error::Archive("cannot encode progress"))?;
    open(paths, true)?
        .execute("UPDATE jobs SET progress_json=?2 WHERE id=?1", (id, json))
        .map_err(|e| at_path(sql_error(e), paths))?;
    Ok(())
}
pub(crate) fn job_failure(paths: &StatePaths, id: &str, error: &Error) -> Result<(), Error> {
    let failure = error.failure();
    let json =
        serde_json::to_string(&failure).map_err(|_| Error::Archive("cannot encode failure"))?;
    open(paths, true)?
        .execute(
            "UPDATE jobs SET failure_json=?2,error=?3,status=CASE WHEN status='running' THEN 'failed' ELSE status END,completed_at=CASE WHEN status='running' THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') ELSE completed_at END WHERE id=?1",
            (id, json, failure.message),
        )
        .map_err(|e| at_path(sql_error(e), paths))?;
    Ok(())
}

pub(crate) fn job_clear_failure(paths: &StatePaths, id: &str) -> Result<(), Error> {
    open(paths, true)?
        .execute(
            "UPDATE jobs SET failure_json=NULL,error=NULL WHERE id=?1",
            [id],
        )
        .map_err(|e| at_path(sql_error(e), paths))?;
    Ok(())
}
