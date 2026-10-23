//! Durable recurring DAG cycles, owned by the existing single scheduler worker.
use super::{
    archive,
    database::{open_initialized, sql_error},
    scheduler,
};
use crate::{
    archive::{
        checkpoint,
        controls::Shutdown,
        executor, group,
        planner::{ArchivePlan, ParentSelection, SourceStability},
    },
    error::Error,
    paths::StatePaths,
    source::ForeignKey,
};
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
struct Config {
    policies: Vec<String>,
    relationships: Vec<ForeignKey>,
    snapshot: String,
    options: scheduler::Options,
}
#[derive(Serialize)]
pub struct Schedule {
    pub id: String,
    pub status: String,
    pub phase: String,
    pub interval_seconds: i64,
    pub next_due: i64,
    pub cycle: i64,
    pub cycles: i64,
    pub position: i64,
    pub current_queue: Option<String>,
    pub last_error: Option<String>,
    pub policies: Vec<String>,
    pub members: Vec<scheduler::Entry>,
}
fn open(paths: &StatePaths, write: bool) -> Result<Connection, Error> {
    open_initialized(paths, write)
}
fn installed(conn: &Connection) -> Result<bool, Error> {
    Ok(super::migrations::version(conn)? >= 19)
}
fn config(paths: &StatePaths, id: &str) -> Result<Config, Error> {
    let json: String = open(paths, false)?
        .query_row(
            "SELECT config_json FROM recurring_groups WHERE id=?1",
            [id],
            |r| r.get(0),
        )
        .map_err(sql_error)?;
    serde_json::from_str(&json).map_err(|_| Error::Archive("invalid recurring group configuration"))
}
fn snapshot(paths: &StatePaths, names: &[String]) -> Result<String, Error> {
    let mut values = Vec::new();
    for name in names {
        let p = archive::policy_show(paths, name)?;
        let s = super::sources::show(paths, &p.source)?;
        let d = super::destinations::show(paths, &p.destination)?;
        values.push(serde_json::json!({"policy":p,"source":s,"destination":d}));
    }
    serde_json::to_string(&values).map_err(|_| Error::Archive("cannot snapshot recurring group"))
}
fn member_ids(paths: &StatePaths, id: &str, cycle: i64) -> Result<Vec<String>, Error> {
    let conn = open(paths, false)?;
    let mut stmt=conn.prepare("SELECT queue_id FROM recurring_group_members WHERE group_id=?1 AND cycle=?2 ORDER BY ordinal").map_err(sql_error)?;
    stmt.query_map((id, cycle), |r| r.get(0))
        .map_err(sql_error)?
        .map(|r| r.map_err(sql_error))
        .collect()
}
pub fn show(paths: &StatePaths, id: &str) -> Result<Schedule, Error> {
    let c = config(paths, id)?;
    let mut s=open(paths,false)?.query_row("SELECT id,status,phase,interval_seconds,next_due,cycle,cycles,position,current_queue,last_error FROM recurring_groups WHERE id=?1",[id],|r|Ok(Schedule{id:r.get(0)?,status:r.get(1)?,phase:r.get(2)?,interval_seconds:r.get(3)?,next_due:r.get(4)?,cycle:r.get(5)?,cycles:r.get(6)?,position:r.get(7)?,current_queue:r.get(8)?,last_error:r.get(9)?,policies:c.policies,members:vec![]})).map_err(sql_error)?;
    s.members = member_ids(paths, id, s.cycle)?
        .iter()
        .map(|q| scheduler::show(paths, q))
        .collect::<Result<_, _>>()?;
    Ok(s)
}
pub fn list(paths: &StatePaths) -> Result<Vec<Schedule>, Error> {
    let conn = open(paths, false)?;
    if !installed(&conn)? {
        return Ok(vec![]);
    }
    let mut stmt = conn
        .prepare("SELECT id FROM recurring_groups ORDER BY id")
        .map_err(sql_error)?;
    let ids = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(sql_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(sql_error)?;
    ids.iter().map(|id| show(paths, id)).collect()
}
pub(crate) fn reserved(paths: &StatePaths, policy: &str) -> Result<bool, Error> {
    let conn = open(paths, false)?;
    if !installed(&conn)? {
        return Ok(false);
    }
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM recurring_group_reservations WHERE policy_id=?1)",
        [policy],
        |r| r.get(0),
    )
    .map_err(sql_error)
}
pub(crate) fn owner(paths: &StatePaths, queue: &str) -> Result<Option<String>, Error> {
    let conn = open(paths, false)?;
    if !installed(&conn)? {
        return Ok(None);
    }
    conn.query_row(
        "SELECT group_id FROM recurring_group_members WHERE queue_id=?1",
        [queue],
        |r| r.get(0),
    )
    .optional()
    .map_err(sql_error)
}
fn prior_deleted(paths: &StatePaths, policy_id: &str) -> Result<(), Error> {
    for j in archive::job_list(paths)?
        .into_iter()
        .filter(|j| j.plan.policy.id == policy_id)
    {
        if j.imported
            || j.status != archive::JobStatus::Completed
            || !super::deletion::show(paths, &j.id)?.is_some_and(|d| d.completed)
        {
            return Err(Error::Archive(
                "group cycle blocked: all prior policy jobs require completed deletion",
            ));
        }
    }
    Ok(())
}
pub async fn create(
    paths: &StatePaths,
    names: &[String],
    interval: u64,
    options: scheduler::Options,
) -> Result<Schedule, Error> {
    options.validate()?;
    if !options.delete_after_archive
        || options.allow_repeat
        || !(60..=31_536_000).contains(&interval)
        || !matches!(options.source_stability, SourceStability::QuiescentCopy)
    {
        return Err(Error::Archive(
            "recurring groups require quiescent-source deletion authorization and interval 60..31536000",
        ));
    }
    let policies = names
        .iter()
        .map(|n| archive::policy_show(paths, n))
        .collect::<Result<Vec<_>, _>>()?;
    let (order, relationships) = group::discover_order(paths, &policies).await?;
    let _locks = policies
        .iter()
        .map(|p| checkpoint::policy_lock(paths, &p.id))
        .collect::<Result<Vec<_>, _>>()?;
    let names: Vec<_> = order
        .into_iter()
        .rev()
        .map(|i| policies[i].name.clone())
        .collect();
    let conf = Config {
        snapshot: snapshot(paths, &names)?,
        policies: names,
        relationships,
        options,
    };
    let id = uuid::Uuid::new_v4().to_string();
    let mut conn = open(paths, true)?;
    if !installed(&conn)? {
        return Err(Error::Archive(
            "run coldctl init to upgrade recurring group state",
        ));
    }
    for p in &policies {
        prior_deleted(paths, &p.id)?;
    }
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    for p in &policies {
        let conflict:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM recurring_group_reservations WHERE policy_id=?1) OR EXISTS(SELECT 1 FROM recurring_archives WHERE policy_id=?1 AND status!='stopped') OR EXISTS(SELECT 1 FROM archive_queue WHERE policy_id=?1 AND status IN ('pending','running','failed'))",[&p.id],|r|r.get(0)).map_err(sql_error)?;
        if conflict {
            return Err(Error::Archive("group policy is already reserved or queued"));
        }
    }
    tx.execute("INSERT INTO recurring_groups(id,config_json,status,phase,interval_seconds,next_due) VALUES(?1,?2,'active','waiting',?3,?4)",(&id,serde_json::to_string(&conf).map_err(|_|Error::Archive("cannot encode group"))?,interval as i64,scheduler::now())).map_err(sql_error)?;
    for p in policies {
        tx.execute(
            "INSERT INTO recurring_group_reservations VALUES(?1,?2)",
            (&p.id, &id),
        )
        .map_err(sql_error)?;
    }
    tx.commit().map_err(sql_error)?;
    show(paths, &id)
}
fn fail(paths: &StatePaths, id: &str, message: &str) -> Result<(), Error> {
    open(paths,true)?.execute("UPDATE recurring_groups SET status='failed',last_error=?2 WHERE id=?1 AND status='active'",(id,message)).map_err(sql_error)?;
    Ok(())
}

/// Admission and phase transitions are atomic; the worker lock serializes callers.
pub(crate) fn admit(paths: &StatePaths, now: i64) -> Result<(), Error> {
    for s in list(paths)?.into_iter().filter(|s| s.status == "active") {
        let c = config(paths, &s.id)?;
        if snapshot(paths, &c.policies).map_or(true, |value| value != c.snapshot) {
            fail(
                paths,
                &s.id,
                "recurring group configuration changed or unavailable",
            )?;
            continue;
        }
        if let Some(q) = &s.current_queue {
            let entry = scheduler::show(paths, q)?;
            if matches!(entry.status.as_str(), "failed" | "cancelled") {
                fail(
                    paths,
                    &s.id,
                    entry
                        .last_error
                        .as_deref()
                        .unwrap_or("group member failed or cancelled"),
                )?;
                continue;
            }
            if entry.status != "completed" {
                continue;
            }
            if s.phase == "delete"
                && !entry
                    .job_id
                    .as_ref()
                    .map(|id| super::deletion::show(paths, id))
                    .transpose()?
                    .flatten()
                    .is_some_and(|d| d.completed)
            {
                fail(paths, &s.id, "group deletion evidence missing")?;
                continue;
            }
            let mut conn = open(paths, true)?;
            let tx = conn
                .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
                .map_err(sql_error)?;
            let (phase, next) =
                if s.phase == "archive" && (s.position as usize) + 1 < s.members.len() {
                    ("archive", s.position + 1)
                } else if s.phase == "archive" {
                    ("delete", s.position)
                } else {
                    ("delete", s.position - 1)
                };
            if next < 0 {
                tx.execute("UPDATE recurring_groups SET phase='waiting',current_queue=NULL,cycles=cycles+1,next_due=?2 WHERE id=?1 AND status='active'",(&s.id,now+s.interval_seconds)).map_err(sql_error)?;
            } else {
                let nextq = &s.members[next as usize].id;
                tx.execute("UPDATE recurring_groups SET phase=?2,position=?3,current_queue=?4 WHERE id=?1 AND status='active'",(&s.id,phase,next,nextq)).map_err(sql_error)?;
                tx.execute("UPDATE archive_queue SET status='pending',due_at=?2,failures=0,last_error=NULL WHERE id=?1 AND status='completed' AND EXISTS(SELECT 1 FROM recurring_groups WHERE id=?3 AND status='active')",(nextq,now,&s.id)).map_err(sql_error)?;
            }
            tx.commit().map_err(sql_error)?;
            continue;
        }
        if s.next_due > now {
            continue;
        }
        let mut invalid = None;
        for name in &c.policies {
            let p = archive::policy_show(paths, name)?;
            if let Err(e) = prior_deleted(paths, &p.id) {
                invalid = Some(e);
                break;
            }
        }
        if let Some(e) = invalid {
            fail(paths, &s.id, e.failure().message.as_ref())?;
            continue;
        }
        let mut conn = open(paths, true)?;
        let tx = conn
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(sql_error)?;
        let active:bool=tx.query_row("SELECT status='active' AND current_queue IS NULL FROM recurring_groups WHERE id=?1",[&s.id],|r|r.get(0)).map_err(sql_error)?;
        if !active {
            continue;
        }
        let mut options = c.options.clone();
        options.delete_after_archive = false;
        options.allow_repeat = true;
        let json = serde_json::to_string(&options)
            .map_err(|_| Error::Archive("cannot encode group queue"))?;
        let mut first = None;
        for (i, name) in c.policies.iter().enumerate() {
            let p = archive::policy_show(paths, name)?;
            let q = uuid::Uuid::new_v4().to_string();
            tx.execute("INSERT INTO archive_queue(id,policy_id,policy_name,options_json,status,due_at,cutoff_utc) VALUES(?1,?2,?3,?4,'pending',?5,strftime('%Y-%m-%d %H:%M:%f',?5,'unixepoch',printf('-%d days',?6)))",(&q,&p.id,name,&json,now,p.config.older_than_days)).map_err(sql_error)?;
            tx.execute(
                "INSERT INTO recurring_group_members VALUES(?1,?2,?3,?4)",
                (&s.id, s.cycle + 1, i as i64, &q),
            )
            .map_err(sql_error)?;
            if first.is_none() {
                first = Some(q);
            }
        }
        tx.execute("UPDATE recurring_groups SET phase='archive',position=0,current_queue=?2,cycle=cycle+1,last_error=NULL WHERE id=?1",(&s.id,first)).map_err(sql_error)?;
        tx.commit().map_err(sql_error)?;
    }
    Ok(())
}
pub(crate) fn guard_plan(
    paths: &StatePaths,
    queue: Option<&str>,
    plan: &mut ArchivePlan,
    identity: &str,
) -> Result<(), Error> {
    if !reserved(paths, &plan.policy.id)? {
        return Ok(());
    }
    let q = queue.ok_or(Error::Archive(
        "policy belongs to a recurring group; use its worker",
    ))?;
    let id = owner(paths, q)?.ok_or(Error::Archive("queue does not own reserved group policy"))?;
    let s = show(paths, &id)?;
    let c = config(paths, &id)?;
    let owned: bool = open(paths, false)?.query_row(
        "SELECT EXISTS(SELECT 1 FROM recurring_group_reservations r JOIN archive_queue q ON q.policy_id=r.policy_id WHERE r.group_id=?1 AND r.policy_id=?2 AND q.id=?3 AND q.status='running')",
        (&id, &plan.policy.id, q), |r| r.get(0)).map_err(sql_error)?;
    if !owned
        || s.status != "active"
        || s.phase != "archive"
        || s.current_queue.as_deref() != Some(q)
        || snapshot(paths, &c.policies)? != c.snapshot
    {
        return Err(Error::Archive("recurring group changed or stopped"));
    }
    prior_deleted(paths, &plan.policy.id)?;
    for previous in archive::job_list(paths)?
        .into_iter()
        .filter(|j| j.plan.policy.id == plan.policy.id)
    {
        if checkpoint::load(paths, &previous.id)?.source_identity != identity
            || previous.plan.table_oid != plan.table_oid
            || previous.plan.columns != plan.columns
            || previous.plan.primary_key != plan.primary_key
            || previous.plan.safety.as_ref().map(|s| &s.source_identity)
                != plan.safety.as_ref().map(|s| &s.source_identity)
        {
            return Err(Error::Archive(
                "recurring group source identity or schema changed",
            ));
        }
    }
    plan.cutoff_utc = scheduler::show(paths, q)?
        .cutoff_utc
        .ok_or(Error::Archive("group cutoff missing"))?;
    Ok(())
}
pub fn stop(paths: &StatePaths, id: &str) -> Result<Schedule, Error> {
    let mut conn = open(paths, true)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    if tx
        .execute(
            "UPDATE recurring_groups SET status='stopped' WHERE id=?1 AND status!='stopped'",
            [id],
        )
        .map_err(sql_error)?
        != 1
    {
        return Err(Error::Archive("group missing or already stopped"));
    }
    tx.execute("UPDATE archive_queue SET status='cancelled' WHERE id IN (SELECT queue_id FROM recurring_group_members WHERE group_id=?1) AND status IN ('pending','running','failed')",[id]).map_err(sql_error)?;
    tx.execute("UPDATE jobs SET cancel_requested=1 WHERE id IN (SELECT job_id FROM archive_queue WHERE id IN (SELECT queue_id FROM recurring_group_members WHERE group_id=?1)) AND status!='completed'",[id]).map_err(sql_error)?;
    tx.execute(
        "DELETE FROM recurring_group_reservations WHERE group_id=?1",
        [id],
    )
    .map_err(sql_error)?;
    tx.commit().map_err(sql_error)?;
    show(paths, id)
}
pub fn retry(paths: &StatePaths, id: &str) -> Result<Schedule, Error> {
    let s = show(paths, id)?;
    if s.status != "failed" {
        return Err(Error::Archive("only failed groups can be retried"));
    }
    let c = config(paths, id)?;
    if snapshot(paths, &c.policies)? != c.snapshot {
        return Err(Error::Archive(
            "restore original group configuration before retry",
        ));
    }
    if s.members.iter().any(|q| q.status == "cancelled") {
        return Err(Error::Archive(
            "cancelled group cycle is terminal; recover manually",
        ));
    }
    let mut conn = open(paths, true)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    if let Some(q) = &s.current_queue {
        tx.execute("UPDATE archive_queue SET status='pending',due_at=?2,failures=0,last_error=NULL WHERE id=?1 AND status='failed'",(q,scheduler::now())).map_err(sql_error)?;
    }
    tx.execute("UPDATE recurring_groups SET status='active',last_error=NULL WHERE id=?1 AND status='failed'",[id]).map_err(sql_error)?;
    tx.commit().map_err(sql_error)?;
    show(paths, id)
}

pub(crate) async fn execute(
    paths: &StatePaths,
    entry: &scheduler::Entry,
    shutdown: Shutdown,
) -> Result<archive::JobStatus, Error> {
    let id = owner(paths, &entry.id)?.ok_or(Error::Archive("missing group owner"))?;
    let s = show(paths, &id)?;
    let c = config(paths, &id)?;
    if s.status != "active"
        || s.current_queue.as_deref() != Some(&entry.id)
        || snapshot(paths, &c.policies)? != c.snapshot
    {
        return Err(Error::Archive("recurring group changed or stopped"));
    }
    if s.phase == "delete" {
        let ids = s
            .members
            .iter()
            .rev()
            .map(|q| {
                q.job_id
                    .clone()
                    .ok_or(Error::Archive("group archive missing"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let ordered = group::verified_order(paths, &ids).await?;
        let current = entry
            .job_id
            .as_ref()
            .ok_or(Error::Archive("group deletion job missing"))?;
        for prior in ordered.iter().take_while(|j| *j != current) {
            if !super::deletion::show(paths, prior)?.is_some_and(|d| d.completed) {
                return Err(Error::Archive("group child deletion incomplete"));
            }
        }
        if !ordered.contains(current) {
            return Err(Error::Archive("invalid group deletion order"));
        }
        crate::archive::automatic_shadow::prepare_group(paths, &ids, &entry.id, shutdown.clone())
            .await?;
        if shutdown.requested()
            || show(paths, &id)?.status != "active"
            || scheduler::show(paths, &entry.id)?.status != "running"
        {
            return Ok(archive::JobStatus::Paused);
        }
        let p = crate::archive::deletion::run_mode(
            paths,
            current,
            crate::archive::deletion::Options {
                confirm_delete: true,
                confirm_original_source: true,
                max_batches: Some(1),
                shutdown,
            },
            true,
        )
        .await?;
        return Ok(if p.completed {
            archive::JobStatus::Completed
        } else {
            archive::JobStatus::Paused
        });
    }
    if let Some(job) = &entry.job_id {
        let j = archive::job_show(paths, job)?;
        if j.status == archive::JobStatus::Completed {
            return verified_archive_status(paths, j);
        }
        if j.cancel_requested || j.status == archive::JobStatus::Cancelled {
            return Ok(archive::JobStatus::Cancelled);
        }
        return executor::resume_scheduled(
            paths,
            job,
            true,
            entry.options.execution.clone(),
            shutdown,
            None,
            true,
        )
        .await
        .and_then(|j| verified_archive_status(paths, j));
    }
    let policy = archive::policy_show(paths, &entry.policy_name)?;
    let mut selection = Vec::new();
    for fk in c
        .relationships
        .iter()
        .filter(|f| f.child_schema == policy.config.schema && f.child_table == policy.config.table)
    {
        for q in s.members.iter().take(s.position as usize) {
            let parent = archive::job_show(
                paths,
                q.job_id
                    .as_deref()
                    .ok_or(Error::Archive("parent archive missing"))?,
            )?;
            if parent.plan.policy.config.schema == fk.parent_schema
                && parent.plan.policy.config.table == fk.parent_table
            {
                crate::archive::verifier::verify(paths, &parent.id)?;
                selection.push(ParentSelection {
                    job_id: parent.id.clone(),
                    manifest_sha256: checkpoint::load(paths, &parent.id)?
                        .manifest
                        .ok_or(Error::Archive("parent manifest missing"))?
                        .sha256,
                    rows: parent.rows_processed,
                    relationship: fk.clone(),
                });
            }
        }
    }
    executor::run_selected(
        paths,
        &entry.policy_name,
        executor::RunOptions {
            source_stability: SourceStability::QuiescentCopy,
            allow_repeat: true,
            minimum_free_bytes: entry.options.execution.minimum_free_bytes,
            execution: entry.options.execution.clone(),
            shutdown,
            progress: None,
        },
        |_| {},
        Some(&entry.id),
        selection,
    )
    .await
    .and_then(|j| verified_archive_status(paths, j))
}

fn verified_archive_status(
    paths: &StatePaths,
    job: archive::Job,
) -> Result<archive::JobStatus, Error> {
    if job.status == archive::JobStatus::Completed {
        crate::archive::verifier::verify(paths, &job.id)?;
    }
    Ok(job.status)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        archive::controls::ExecutionControls, policy::model::PolicyConfig,
        source::SourceConnection, state,
    };
    fn fixture() -> (tempfile::TempDir, StatePaths, String) {
        let tmp = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(tmp.path())).unwrap();
        state::initialize(&paths, "test").unwrap();
        state::sources::add(
            &paths,
            "mysql",
            SourceConnection::from_mysql_url_env("UNUSED_GROUP_URL".into()).unwrap(),
        )
        .unwrap();
        state::destinations::add_local(&paths, "local", &tmp.path().join("archives")).unwrap();
        let names = vec!["parent".to_string(), "child".to_string()];
        for name in &names {
            archive::policy_create(
                &paths,
                name,
                "mysql",
                "local",
                PolicyConfig {
                    schema: "db".into(),
                    table: name.clone(),
                    time_column: "created_at".into(),
                    older_than_days: 365,
                    batch_size: 2,
                    equals_column: None,
                    equals_value: None,
                },
            )
            .unwrap();
        }
        let c = Config {
            snapshot: snapshot(&paths, &names).unwrap(),
            policies: names.clone(),
            relationships: vec![],
            options: scheduler::Options {
                delete_after_archive: true,
                source_stability: SourceStability::QuiescentCopy,
                allow_repeat: false,
                confirm_original_source: true,
                execution: ExecutionControls {
                    max_duration_seconds: Some(60),
                    ..Default::default()
                },
                max_retries: 2,
            },
        };
        let id = "group".to_string();
        let conn = open(&paths, true).unwrap();
        conn.execute("INSERT INTO recurring_groups(id,config_json,status,phase,interval_seconds,next_due) VALUES(?1,?2,'active','waiting',60,100)",(&id,serde_json::to_string(&c).unwrap())).unwrap();
        for name in names {
            conn.execute(
                "INSERT INTO recurring_group_reservations VALUES(?1,?2)",
                (archive::policy_show(&paths, &name).unwrap().id, &id),
            )
            .unwrap();
        }
        (tmp, paths, id)
    }
    #[test]
    fn only_current_member_is_claimable_and_recovery_preserves_cycle() {
        let (_tmp, p, id) = fixture();
        admit(&p, 99).unwrap();
        assert!(scheduler::list(&p).unwrap().is_empty());
        admit(&p, 100).unwrap();
        let s = show(&p, &id).unwrap();
        assert_eq!(s.members.len(), 2);
        let parent = scheduler::claim(&p, 100).unwrap().unwrap();
        assert_eq!(parent.policy_name, "parent");
        assert!(scheduler::claim(&p, 100).unwrap().is_none());
        scheduler::recover(&p).unwrap();
        assert_eq!(scheduler::claim(&p, 100).unwrap().unwrap().id, parent.id);
        scheduler::finish(&p, &parent.id, "completed", 100, 0, None).unwrap();
        admit(&p, 100).unwrap();
        let child = scheduler::claim(&p, 100).unwrap().unwrap();
        assert_eq!(child.policy_name, "child");
        scheduler::finish(&p, &child.id, "completed", 100, 0, None).unwrap();
        admit(&p, 100).unwrap();
        let s = show(&p, &id).unwrap();
        assert_eq!(s.phase, "delete");
        assert_eq!(s.current_queue.as_deref(), Some(child.id.as_str()));
        assert_eq!(s.cycle, 1);
        assert_eq!(s.cycles, 0);
        scheduler::claim(&p, 100).unwrap().unwrap();
        scheduler::finish(&p, &child.id, "completed", 100, 0, None).unwrap();
        // Queue completion alone cannot advance deletion without a completed journal.
        admit(&p, 100).unwrap();
        assert_eq!(show(&p, &id).unwrap().status, "failed");
        assert!(scheduler::claim(&p, 100).unwrap().is_none());
    }
    #[test]
    fn failure_retry_stop_and_reservations_hold_entire_group() {
        let (_tmp, p, id) = fixture();
        let options = config(&p, &id).unwrap().options;
        assert!(scheduler::enqueue(&p, "parent", None, options.clone(), 0).is_err());
        assert!(state::recurring::create(&p, "parent", 60, options).is_err());
        admit(&p, 100).unwrap();
        let q = scheduler::claim(&p, 100).unwrap().unwrap();
        scheduler::finish(&p, &q.id, "failed", 100, 1, Some("archive unavailable")).unwrap();
        admit(&p, 100).unwrap();
        assert_eq!(show(&p, &id).unwrap().status, "failed");
        assert!(scheduler::claim(&p, i64::MAX).unwrap().is_none());
        assert!(scheduler::retry(&p, &q.id, true).is_err());
        retry(&p, &id).unwrap();
        let recovered = scheduler::claim(&p, i64::MAX).unwrap().unwrap();
        assert_eq!(q.id, recovered.id);
        stop(&p, &id).unwrap();
        scheduler::finish(&p, &q.id, "completed", 100, 0, None).unwrap();
        assert_eq!(scheduler::show(&p, &q.id).unwrap().status, "cancelled");
        assert!(scheduler::claim(&p, i64::MAX).unwrap().is_none());
        admit(&p, i64::MAX).unwrap();
        assert_eq!(scheduler::list(&p).unwrap().len(), 2);
        assert!(!reserved(&p, &archive::policy_show(&p, "parent").unwrap().id).unwrap());
        assert!(retry(&p, &id).is_err());
    }
    #[test]
    fn configuration_drift_holds_before_admission() {
        let (_tmp, p, id) = fixture();
        open(&p, true)
            .unwrap()
            .execute("UPDATE sources SET name='renamed' WHERE name='mysql'", [])
            .unwrap();
        admit(&p, 100).unwrap();
        assert_eq!(show(&p, &id).unwrap().status, "failed");
        assert!(scheduler::list(&p).unwrap().is_empty());
    }
    fn archived(paths: &StatePaths, q: &scheduler::Entry) -> String {
        let mut plan = ArchivePlan {
            parent_selection: vec![],
            connector_pin: None,
            policy: archive::policy_show(paths, &q.policy_name).unwrap(),
            destination_path: paths.data_dir.join("archives"),
            destination_config: None,
            cutoff_utc: "unused".into(),
            primary_key: "id".into(),
            table_oid: 42,
            columns: vec![],
            estimated_rows: None,
            delete: false,
            warnings: vec![],
            safety: None,
        };
        assert!(guard_plan(paths, None, &mut plan, "identity").is_err());
        guard_plan(paths, Some(&q.id), &mut plan, "identity").unwrap();
        assert_ne!(plan.cutoff_utc, "unused");
        let job = archive::job_create_queued(
            paths,
            &plan,
            "mysql",
            "identity",
            None,
            &q.options.execution,
            Some(&q.id),
        )
        .unwrap();
        archive::job_finish(paths, &job.id, None).unwrap();
        scheduler::finish(paths, &q.id, "completed", 100, 0, None).unwrap();
        job.id
    }
    #[test]
    fn next_cycle_waits_for_every_deletion_and_keeps_prior_member_history() {
        let (_tmp, p, id) = fixture();
        admit(&p, 100).unwrap();
        let first = scheduler::claim(&p, 100).unwrap().unwrap();
        let parent = archived(&p, &first);
        admit(&p, 100).unwrap();
        let second = scheduler::claim(&p, 100).unwrap().unwrap();
        let child = archived(&p, &second);
        admit(&p, 100).unwrap();
        for job in [child, parent] {
            let q = scheduler::claim(&p, 100).unwrap().unwrap();
            assert_eq!(q.job_id.as_deref(), Some(job.as_str()));
            state::deletion::start(&p, &job, &"a".repeat(64)).unwrap();
            state::deletion::finish_empty(&p, &job).unwrap();
            scheduler::finish(&p, &q.id, "completed", 100, 0, None).unwrap();
            admit(&p, 100).unwrap();
        }
        let s = show(&p, &id).unwrap();
        assert_eq!(s.cycles, 1);
        assert_eq!(s.next_due, 160);
        assert_eq!(s.phase, "waiting");
        admit(&p, 159).unwrap();
        assert!(scheduler::claim(&p, 159).unwrap().is_none());
        admit(&p, 160).unwrap();
        let s = show(&p, &id).unwrap();
        assert_eq!(s.cycle, 2);
        assert_eq!(s.cycles, 1);
        assert_eq!(scheduler::list(&p).unwrap().len(), 4);
        assert_ne!(s.members[0].id, first.id);
        assert!(s.members.iter().all(|q| q.job_id.is_none()));
    }
}
