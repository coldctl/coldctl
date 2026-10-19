//! Fixed-delay, deletion-gated recurring archives. No key watermark: every cycle
//! rescans surviving eligible rows, including backdated arrivals between cycles.
use super::{
    archive,
    database::{open_initialized, sql_error},
    scheduler,
};
use crate::{
    archive::planner::{ArchivePlan, SourceStability},
    error::Error,
    paths::StatePaths,
};
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;

fn open(paths: &StatePaths, write: bool) -> Result<Connection, Error> {
    let conn = open_initialized(paths, write)?;
    if super::migrations::version(&conn)? < 16 {
        return Err(Error::Archive(
            "recurring schema needs an upgrade; run coldctl init",
        ));
    }
    Ok(conn)
}

#[derive(Debug, Serialize)]
pub struct Schedule {
    pub id: String,
    pub policy_name: String,
    pub interval_seconds: i64,
    pub status: String,
    pub next_due: i64,
    pub current_queue: Option<String>,
    pub cycles: i64,
    pub last_error: Option<String>,
}
const SELECT: &str = "SELECT id,policy_name,interval_seconds,status,next_due,current_queue,cycles,last_error FROM recurring_archives";
fn row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Schedule> {
    Ok(Schedule {
        id: r.get(0)?,
        policy_name: r.get(1)?,
        interval_seconds: r.get(2)?,
        status: r.get(3)?,
        next_due: r.get(4)?,
        current_queue: r.get(5)?,
        cycles: r.get(6)?,
        last_error: r.get(7)?,
    })
}
pub fn list(paths: &StatePaths) -> Result<Vec<Schedule>, Error> {
    let conn = open(paths, false)?;
    let mut stmt = conn
        .prepare(&format!("{SELECT} ORDER BY created_at,id"))
        .map_err(sql_error)?;
    stmt.query_map([], row)
        .map_err(sql_error)?
        .map(|r| r.map_err(sql_error))
        .collect()
}
pub fn show(paths: &StatePaths, id: &str) -> Result<Schedule, Error> {
    open(paths, false)?
        .query_row(&format!("{SELECT} WHERE id=?1"), [id], row)
        .optional()
        .map_err(sql_error)?
        .ok_or(Error::Archive("recurring schedule not found"))
}
fn snapshot(paths: &StatePaths, policy: &str) -> Result<String, Error> {
    let p = archive::policy_show(paths, policy)?;
    let s = super::sources::show(paths, &p.source)?;
    let d = super::destinations::show(paths, &p.destination)?;
    serde_json::to_string(&serde_json::json!({"policy":p,"source":s,"destination":d}))
        .map_err(|_| Error::Archive("cannot encode recurring configuration"))
}
pub fn create(
    paths: &StatePaths,
    policy: &str,
    interval: u64,
    mut options: scheduler::Options,
) -> Result<Schedule, Error> {
    options.validate()?;
    if !(60..=31_536_000).contains(&interval)
        || !options.delete_after_archive
        || options.allow_repeat
    {
        return Err(Error::Archive(
            "recurring requires deletion authorization, interval 60..31536000 seconds, and no allow-repeat override",
        ));
    }
    let p = archive::policy_show(paths, policy)?;
    let _guard = crate::archive::checkpoint::policy_lock(paths, &p.id)?;
    let s = super::sources::show(paths, &p.source)?;
    let d = super::destinations::show(paths, &p.destination)?;
    if !matches!(s.source_type.as_str(), "postgres" | "mysql" | "mongodb")
        || !matches!(d.destination_type.as_str(), "s3" | "azure")
    {
        return Err(Error::Archive(
            "recurring supports PostgreSQL, MySQL or MongoDB to S3 or Azure Blob",
        ));
    }
    if matches!(s.source_type.as_str(), "mysql" | "mongodb")
        && !matches!(options.source_stability, SourceStability::QuiescentCopy)
    {
        return Err(Error::Archive(
            "MySQL and MongoDB recurring require a quiescent source for each complete cycle",
        ));
    }
    ensure_prior_deleted(paths, &p.id)?;
    let frozen = snapshot(paths, policy)?;
    options.allow_repeat = true; // Only the deletion-gated admission path can authorize this.
    let json = serde_json::to_string(&options)
        .map_err(|_| Error::Archive("cannot encode recurring options"))?;
    let id = uuid::Uuid::new_v4().to_string();
    let mut conn = open(paths, true)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    let active: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM archive_queue WHERE policy_id=?1 AND status IN ('pending','running','failed'))", [&p.id], |r|r.get(0)).map_err(sql_error)?;
    if active {
        return Err(Error::Archive(
            "resolve existing queue entries before creating a recurring schedule",
        ));
    }
    tx.execute("INSERT INTO recurring_archives(id,policy_id,policy_name,snapshot_json,options_json,interval_seconds,status,next_due) VALUES(?1,?2,?3,?4,?5,?6,'active',?7)", (&id,&p.id,policy,frozen,json,interval as i64,scheduler::now())).map_err(sql_error)?;
    tx.commit().map_err(sql_error)?;
    show(paths, &id)
}

fn ensure_prior_deleted(paths: &StatePaths, policy_id: &str) -> Result<(), Error> {
    for job in archive::job_list(paths)?
        .into_iter()
        .filter(|j| j.plan.policy.id == policy_id)
    {
        if job.imported
            || job.status != archive::JobStatus::Completed
            || !super::deletion::show(paths, &job.id)?.is_some_and(|d| d.completed)
        {
            return Err(Error::Archive(
                "recurring blocked: every previous policy job must have completed verified deletion",
            ));
        }
    }
    Ok(())
}

/// Called by the single locked worker. Admission and linkage commit together.
pub(crate) fn admit(paths: &StatePaths, now: i64) -> Result<(), Error> {
    for schedule in list(paths)?.into_iter().filter(|s| s.status == "active") {
        if let Some(queue) = &schedule.current_queue {
            let q = scheduler::show(paths, queue)?;
            match q.status.as_str() {
                "completed" => {
                    let complete = q
                        .job_id
                        .as_ref()
                        .map(|id| super::deletion::show(paths, id))
                        .transpose()?
                        .flatten()
                        .is_some_and(|d| d.completed);
                    if !complete {
                        fail(
                            paths,
                            &schedule.id,
                            "completed cycle lacks deletion evidence",
                        )?;
                        continue;
                    }
                    open(paths,true)?.execute("UPDATE recurring_archives SET current_queue=NULL,cycles=cycles+1,next_due=?2 WHERE id=?1 AND status='active' AND current_queue=?3", (&schedule.id,now+schedule.interval_seconds,queue)).map_err(sql_error)?;
                }
                "failed" | "cancelled" => {
                    fail(
                        paths,
                        &schedule.id,
                        "cycle failed or cancelled; inspect its queue and retry the schedule",
                    )?;
                }
                _ => {}
            }
            continue;
        }
        if schedule.next_due > now {
            continue;
        }
        let result = admit_one(paths, &schedule, now);
        if let Err(error) = result {
            fail(paths, &schedule.id, &error.failure().message)?;
        }
    }
    Ok(())
}
fn fail(paths: &StatePaths, id: &str, message: &str) -> Result<(), Error> {
    open(paths,true)?.execute("UPDATE recurring_archives SET status='failed',last_error=?2 WHERE id=?1 AND status='active'",(id,message)).map_err(sql_error)?;
    Ok(())
}
fn admit_one(paths: &StatePaths, s: &Schedule, now: i64) -> Result<(), Error> {
    let p = archive::policy_show(paths, &s.policy_name)?;
    ensure_prior_deleted(paths, &p.id)?;
    let current = snapshot(paths, &s.policy_name)?;
    let mut conn = open(paths, true)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    let (saved,options):(String,String)=tx.query_row("SELECT snapshot_json,options_json FROM recurring_archives WHERE id=?1 AND status='active' AND current_queue IS NULL AND next_due<=?2",(&s.id,now),|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql_error)?;
    if current != saved {
        return Err(Error::Archive(
            "recurring configuration changed; original policy/source/destination must be restored",
        ));
    }
    let cutoff =
        bson::DateTime::from_millis((now - i64::from(p.config.older_than_days) * 86400) * 1000)
            .try_to_rfc3339_string()
            .map_err(|_| Error::Archive("recurring cutoff out of range"))?;
    let id = uuid::Uuid::new_v4().to_string();
    tx.execute("INSERT INTO archive_queue(id,policy_id,policy_name,options_json,status,due_at,recurring_id,cutoff_utc) VALUES(?1,?2,?3,?4,'pending',?5,?6,?7)",(&id,&p.id,&p.name,options,now,&s.id,cutoff)).map_err(sql_error)?;
    tx.execute(
        "UPDATE recurring_archives SET current_queue=?2 WHERE id=?1",
        (&s.id, &id),
    )
    .map_err(sql_error)?;
    tx.commit().map_err(sql_error)
}

/// Freeze the admitted cutoff and reject config/source identity drift before export.
pub(crate) fn guard_plan(
    paths: &StatePaths,
    queue: Option<&str>,
    plan: &mut ArchivePlan,
    identity: &str,
) -> Result<(), Error> {
    let conn = open(paths, false)?;
    let schedule:Option<(String,String)>=conn.query_row("SELECT id,snapshot_json FROM recurring_archives WHERE policy_id=?1 AND status!='stopped'",[&plan.policy.id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(sql_error)?;
    let Some((id, saved)) = schedule else {
        return Ok(());
    };
    let queue = queue.ok_or(Error::Archive(
        "policy belongs to a recurring schedule; use its worker",
    ))?;
    let cutoff:String=conn.query_row("SELECT cutoff_utc FROM archive_queue WHERE id=?1 AND recurring_id=?2 AND status='running'",(queue,&id),|r|r.get(0)).map_err(sql_error)?;
    if snapshot(paths, &plan.policy.name)? != saved {
        return Err(Error::Archive("recurring configuration changed"));
    }
    ensure_prior_deleted(paths, &plan.policy.id)?;
    for previous in archive::job_list(paths)?
        .into_iter()
        .filter(|j| j.plan.policy.id == plan.policy.id)
    {
        if crate::archive::checkpoint::load(paths, &previous.id)?.source_identity != identity
            || previous.plan.primary_key != plan.primary_key
            || previous.plan.safety.as_ref().map(|s| &s.source_identity)
                != plan.safety.as_ref().map(|s| &s.source_identity)
            || previous.plan.table_oid != plan.table_oid
            || previous.plan.columns != plan.columns
        {
            return Err(Error::Archive(
                "recurring source identity or schema changed",
            ));
        }
    }
    // Queue timestamps remain UTC RFC3339. MySQL binds a DATETIME literal in a
    // UTC session, matching its native planner (no RFC3339 timezone suffix).
    plan.cutoff_utc = if super::sources::show(paths, &plan.policy.source)?.source_type == "mysql" {
        cutoff
            .strip_suffix('Z')
            .ok_or(Error::Archive("invalid recurring UTC cutoff"))?
            .replace('T', " ")
    } else {
        cutoff
    };
    Ok(())
}
pub fn stop(paths: &StatePaths, id: &str) -> Result<Schedule, Error> {
    let mut conn = open(paths, true)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    if tx
        .execute(
            "UPDATE recurring_archives SET status='stopped' WHERE id=?1 AND status!='stopped'",
            [id],
        )
        .map_err(sql_error)?
        != 1
    {
        return Err(Error::Archive("schedule not found or already stopped"));
    }
    tx.execute("UPDATE archive_queue SET status='cancelled' WHERE recurring_id=?1 AND status IN ('pending','running','failed')",[id]).map_err(sql_error)?;
    tx.execute("UPDATE jobs SET cancel_requested=1 WHERE id IN (SELECT job_id FROM archive_queue WHERE recurring_id=?1) AND status!='completed'",[id]).map_err(sql_error)?;
    tx.commit().map_err(sql_error)?;
    show(paths, id)
}
pub fn retry(paths: &StatePaths, id: &str) -> Result<Schedule, Error> {
    let s = show(paths, id)?;
    if s.status != "failed" {
        return Err(Error::Archive("only failed schedules can be retried"));
    }
    let mut conn = open(paths, true)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    if let Some(queue) = &s.current_queue {
        let status: String = tx
            .query_row(
                "SELECT status FROM archive_queue WHERE id=?1",
                [queue],
                |r| r.get(0),
            )
            .map_err(sql_error)?;
        if status == "cancelled" {
            return Err(Error::Archive(
                "cancelled cycle is terminal; recover it manually before replacing the schedule",
            ));
        }
        tx.execute("UPDATE archive_queue SET status='pending',due_at=?2,failures=0,last_error=NULL WHERE id=?1 AND status='failed'",(queue,scheduler::now())).map_err(sql_error)?;
    }
    tx.execute("UPDATE recurring_archives SET status='active',last_error=NULL WHERE id=?1 AND status='failed'",[id]).map_err(sql_error)?;
    tx.commit().map_err(sql_error)?;
    show(paths, id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        archive::controls::ExecutionControls, destination::s3::S3Config,
        policy::model::PolicyConfig, source::SourceConnection, state,
    };

    fn fixture() -> (tempfile::TempDir, StatePaths, scheduler::Options) {
        fixture_for(false)
    }

    fn fixture_for(azure: bool) -> (tempfile::TempDir, StatePaths, scheduler::Options) {
        fixture_engine(azure, false)
    }
    fn fixture_engine(
        azure: bool,
        mysql: bool,
    ) -> (tempfile::TempDir, StatePaths, scheduler::Options) {
        let temp = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(temp.path())).unwrap();
        state::initialize(&paths, "test").unwrap();
        state::sources::add(
            &paths,
            "source",
            if mysql {
                SourceConnection::from_mysql_url_env("UNUSED_RECURRING_URL".into()).unwrap()
            } else {
                SourceConnection::from_url_env("UNUSED_RECURRING_URL".into()).unwrap()
            },
        )
        .unwrap();
        if azure {
            state::destinations::add_remote(
                &paths,
                "s3",
                &temp.path().join("staging"),
                crate::destination::azure::AzureConfig {
                    account: "testaccount".into(),
                    container: "test-archives".into(),
                    prefix: "test".into(),
                    endpoint: None,
                    account_key_env: "UNUSED_AZURE_KEY".into(),
                    managed_identity: false,
                    managed_identity_client_id: None,
                    allow_http: false,
                }
                .into(),
            )
            .unwrap();
        } else {
            state::destinations::add_remote(
                &paths,
                "s3",
                &temp.path().join("staging"),
                S3Config {
                    role_auth: false,
                    bucket: "test-archives".into(),
                    region: "us-east-1".into(),
                    prefix: "test".into(),
                    endpoint: None,
                    access_key_env: "UNUSED_ACCESS".into(),
                    secret_key_env: "UNUSED_SECRET".into(),
                    session_token_env: None,
                    allow_http: false,
                }
                .into(),
            )
            .unwrap();
        }
        archive::policy_create(
            &paths,
            "orders",
            "source",
            "s3",
            PolicyConfig {
                schema: "public".into(),
                table: "orders".into(),
                time_column: "created_at".into(),
                older_than_days: 90,
                batch_size: 2,
                equals_column: None,
                equals_value: None,
            },
        )
        .unwrap();
        let options = scheduler::Options {
            delete_after_archive: true,
            source_stability: SourceStability::ImmutableRows,
            allow_repeat: false,
            confirm_original_source: true,
            execution: ExecutionControls {
                max_duration_seconds: Some(60),
                ..Default::default()
            },
            max_retries: 2,
        };
        (temp, paths, options)
    }
    fn complete(paths: &StatePaths, q: &scheduler::Entry, deleted: bool) -> String {
        let mut plan = ArchivePlan {
            connector_pin: None,
            policy: archive::policy_show(paths, "orders").unwrap(),
            destination_path: paths.data_dir.join("staging"),
            destination_config: state::destinations::show(paths, "s3").unwrap().config,
            cutoff_utc: "2000-01-01T00:00:00Z".into(),
            primary_key: "id".into(),
            table_oid: 42,
            columns: vec![],
            estimated_rows: None,
            delete: false,
            warnings: vec![],
            safety: None,
        };
        guard_plan(paths, Some(&q.id), &mut plan, "identity").unwrap();
        assert_ne!(plan.cutoff_utc, "2000-01-01T00:00:00Z");
        let job = archive::job_create_queued(
            paths,
            &plan,
            "source",
            "identity",
            None,
            &q.options.execution,
            Some(&q.id),
        )
        .unwrap();
        archive::job_finish(paths, &job.id, None).unwrap();
        if deleted {
            state::deletion::start(paths, &job.id, &"a".repeat(64)).unwrap();
            state::deletion::finish_empty(paths, &job.id).unwrap();
        }
        scheduler::finish(paths, &q.id, "completed", scheduler::now(), 0, None).unwrap();
        job.id
    }
    #[test]
    fn mysql_recurring_requires_quiescence_and_retains_cutoff_and_deletion_gate() {
        for azure in [false, true] {
            let (_temp, paths, mut options) = fixture_engine(azure, true);
            assert!(create(&paths, "orders", 60, options.clone()).is_err());
            options.source_stability = SourceStability::QuiescentCopy;
            let schedule = create(&paths, "orders", 60, options).unwrap();
            admit(&paths, schedule.next_due).unwrap();
            let queue = scheduler::claim(&paths, schedule.next_due)
                .unwrap()
                .unwrap();
            let cutoff: String = open(&paths, false)
                .unwrap()
                .query_row(
                    "SELECT cutoff_utc FROM archive_queue WHERE id=?1",
                    [&queue.id],
                    |r| r.get(0),
                )
                .unwrap();
            scheduler::recover(&paths).unwrap();
            let recovered = scheduler::claim(&paths, schedule.next_due + 10)
                .unwrap()
                .unwrap();
            assert_eq!(recovered.id, queue.id);
            let job = complete(&paths, &recovered, true);
            assert_eq!(
                archive::job_show(&paths, &job).unwrap().plan.cutoff_utc,
                cutoff.trim_end_matches('Z').replace('T', " ")
            );
            admit(&paths, schedule.next_due + 10).unwrap();
            admit(&paths, schedule.next_due + 70).unwrap();
            let next = scheduler::claim(&paths, schedule.next_due + 70)
                .unwrap()
                .unwrap();
            complete(&paths, &next, false);
            admit(&paths, schedule.next_due + 71).unwrap();
            assert_eq!(show(&paths, &schedule.id).unwrap().status, "failed");
            stop(&paths, &schedule.id).unwrap();
            admit(&paths, schedule.next_due + 1000).unwrap();
            assert_eq!(scheduler::list(&paths).unwrap().len(), 2);
        }
    }
    #[test]
    fn azure_cycles_require_deletion_evidence_and_stop_prevents_admission() {
        let (_temp, paths, options) = fixture_for(true);
        let schedule = create(&paths, "orders", 60, options).unwrap();
        admit(&paths, schedule.next_due).unwrap();
        let queue = scheduler::claim(&paths, schedule.next_due)
            .unwrap()
            .unwrap();
        complete(&paths, &queue, true);
        admit(&paths, schedule.next_due + 1).unwrap();
        assert_eq!(show(&paths, &schedule.id).unwrap().cycles, 1);
        admit(&paths, schedule.next_due + 60).unwrap();
        assert_eq!(scheduler::list(&paths).unwrap().len(), 1);
        admit(&paths, schedule.next_due + 61).unwrap();
        let next = scheduler::claim(&paths, schedule.next_due + 61)
            .unwrap()
            .unwrap();
        complete(&paths, &next, false);
        admit(&paths, schedule.next_due + 62).unwrap();
        assert_eq!(show(&paths, &schedule.id).unwrap().status, "failed");
        stop(&paths, &schedule.id).unwrap();
        admit(&paths, schedule.next_due + 1000).unwrap();
        assert_eq!(scheduler::list(&paths).unwrap().len(), 2);
    }

    #[test]
    fn frozen_windows_restart_delay_and_late_arrival_rescan() {
        let (_t, p, o) = fixture();
        let s = create(&p, "orders", 60, o).unwrap();
        let now = s.next_due;
        admit(&p, now).unwrap();
        admit(&p, now).unwrap();
        assert_eq!(scheduler::list(&p).unwrap().len(), 1);
        let q = scheduler::claim(&p, now).unwrap().unwrap();
        let cutoff: String = open(&p, false)
            .unwrap()
            .query_row(
                "SELECT cutoff_utc FROM archive_queue WHERE id=?1",
                [&q.id],
                |r| r.get(0),
            )
            .unwrap();
        scheduler::recover(&p).unwrap();
        let q2 = scheduler::claim(&p, now + 500).unwrap().unwrap();
        assert_eq!(q.id, q2.id);
        let cutoff2: String = open(&p, false)
            .unwrap()
            .query_row(
                "SELECT cutoff_utc FROM archive_queue WHERE id=?1",
                [&q.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cutoff, cutoff2);
        complete(&p, &q2, true);
        admit(&p, now + 500).unwrap();
        assert_eq!(show(&p, &s.id).unwrap().cycles, 1);
        admit(&p, now + 559).unwrap();
        assert_eq!(scheduler::list(&p).unwrap().len(), 1);
        admit(&p, now + 560).unwrap();
        let next = scheduler::claim(&p, now + 560).unwrap().unwrap();
        assert_ne!(next.id, q.id);
        assert!(next.job_id.is_none());
        // New jobs restart from the beginning, not the previous cycle's maximum key.
        let job = complete(&p, &next, true);
        assert!(archive::job_show(&p, &job).unwrap().last_key.is_none());
    }
    #[test]
    fn missing_deletion_evidence_blocks_following_cycle() {
        let (_t, p, o) = fixture();
        let s = create(&p, "orders", 60, o).unwrap();
        admit(&p, s.next_due).unwrap();
        let q = scheduler::claim(&p, s.next_due).unwrap().unwrap();
        complete(&p, &q, false);
        admit(&p, s.next_due + 100).unwrap();
        assert_eq!(show(&p, &s.id).unwrap().status, "failed");
        assert_eq!(scheduler::list(&p).unwrap().len(), 1);
    }
    #[test]
    fn failure_retry_retains_cycle_and_stop_wins() {
        let (_t, p, o) = fixture();
        let s = create(&p, "orders", 60, o).unwrap();
        admit(&p, s.next_due).unwrap();
        let q = scheduler::claim(&p, s.next_due).unwrap().unwrap();
        scheduler::finish(&p, &q.id, "failed", 0, 3, Some("fixture failure")).unwrap();
        admit(&p, s.next_due + 100).unwrap();
        assert_eq!(show(&p, &s.id).unwrap().status, "failed");
        retry(&p, &s.id).unwrap();
        assert_eq!(
            show(&p, &s.id).unwrap().current_queue.as_deref(),
            Some(q.id.as_str())
        );
        stop(&p, &s.id).unwrap();
        scheduler::finish(&p, &q.id, "pending", 0, 0, None).unwrap();
        admit(&p, s.next_due + 1000).unwrap();
        assert_eq!(scheduler::show(&p, &q.id).unwrap().status, "cancelled");
        assert!(scheduler::claim(&p, s.next_due + 1000).unwrap().is_none());
    }
    #[test]
    fn config_drift_and_unsafe_authorization_fail_closed() {
        let (_t, p, mut o) = fixture();
        o.delete_after_archive = false;
        assert!(create(&p, "orders", 60, o.clone()).is_err());
        o.delete_after_archive = true;
        assert!(create(&p, "orders", 0, o.clone()).is_err());
        let s = create(&p, "orders", 60, o).unwrap();
        open(&p, true)
            .unwrap()
            .execute(
                "UPDATE policies SET config_json=json_set(config_json,'$.older_than_days',1)",
                [],
            )
            .unwrap();
        admit(&p, s.next_due).unwrap();
        assert_eq!(show(&p, &s.id).unwrap().status, "failed");
        assert!(scheduler::list(&p).unwrap().is_empty());
    }

    #[test]
    fn reserved_policy_manual_runs_and_schema_drift_are_blocked() {
        let (_t, p, o) = fixture();
        let s = create(&p, "orders", 60, o.clone()).unwrap();
        assert!(scheduler::enqueue(&p, "orders", None, o, 0).is_err());
        admit(&p, s.next_due).unwrap();
        let q = scheduler::claim(&p, s.next_due).unwrap().unwrap();
        let job = complete(&p, &q, true);
        let mut plan = archive::job_show(&p, &job).unwrap().plan;
        assert!(guard_plan(&p, None, &mut plan, "identity").is_err());
        admit(&p, s.next_due).unwrap();
        admit(&p, s.next_due + 60).unwrap();
        let next = scheduler::claim(&p, s.next_due + 60).unwrap().unwrap();
        plan.table_oid += 1;
        assert!(guard_plan(&p, Some(&next.id), &mut plan, "identity").is_err());
        assert!(scheduler::retry(&p, &next.id, true).is_err());
        stop(&p, &s.id).unwrap();
        // Stop during preflight prevents any job/checkpoint from being published.
        assert!(
            archive::job_create_queued(
                &p,
                &plan,
                "source",
                "identity",
                None,
                &next.options.execution,
                Some(&next.id)
            )
            .is_err()
        );
        assert_eq!(archive::job_list(&p).unwrap().len(), 1);
    }
}
