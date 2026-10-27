//! Explicit metadata allowlist for Cloud; never serialize operational config or errors.
use crate::{paths::StatePaths, state};
use anyhow::Result;
use serde_json::{Value, json};
fn item(id: String, kind: &str, name: String, status: String) -> Value {
    json!({"id":id,"kind":kind,"name":name,"status":status,"jobId":null,"dueAt":null,"attempts":0,"completed":false,"deletedRows":0,"absentRows":0,"version":null,"hasFailure":false,"deleteAuthorized":false})
}
pub fn collect(paths: &StatePaths) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    for q in state::scheduler::list(paths)? {
        let mut v = item(format!("queue:{}", q.id), "queue", q.policy_name, q.status);
        v["jobId"] = json!(q.job_id);
        v["deleteAuthorized"] = json!(q.options.delete_after_archive);
        v["dueAt"] = json!(q.due_at);
        v["attempts"] = json!(q.failures);
        v["hasFailure"] = json!(q.last_error.is_some());
        out.push(v);
    }
    for s in state::recurring::list(paths)? {
        let mut v = item(
            format!("schedule:{}", s.id),
            "schedule",
            s.policy_name,
            s.status,
        );
        v["dueAt"] = json!(s.next_due);
        v["deleteAuthorized"] = json!(true);
        v["attempts"] = json!(s.cycles);
        v["hasFailure"] = json!(s.last_error.is_some());
        out.push(v);
    }
    let c = state::database::open_initialized(paths, false)?;
    let mut stmt = c.prepare("SELECT job_id FROM archive_deletions ORDER BY job_id")?;
    for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
        let id = row?;
        let progress = state::deletion::show(paths, &id)?
            .ok_or_else(|| anyhow::anyhow!("missing deletion checkpoint"))?;
        let completed = progress.completed;
        let mut v = item(
            format!("deletion:{id}"),
            "deletion",
            id.clone(),
            if completed && progress.rows_deferred > 0 {
                "completed_with_deferred"
            } else if completed {
                "completed"
            } else {
                "partial"
            }
            .into(),
        );
        v["jobId"] = json!(id);
        v["deletedRows"] = json!(progress.rows_deleted);
        v["deleteAuthorized"] = json!(true);
        v["absentRows"] = json!(progress.rows_absent);
        v["completed"] = json!(completed);
        if progress.deletion_mode == "live_values_v1" {
            v["deferredRows"] = json!(progress.rows_deferred);
            v["uncertainAbsentRows"] = json!(progress.rows_absent_uncertain);
        }
        out.push(v);
    }
    let path = paths.data_dir.join("outcomes.sqlite");
    if path.exists() {
        crate::fs_security::check_sqlite(&path)?;
        let c = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let mut stmt = c.prepare("SELECT job_id,body FROM outcomes ORDER BY job_id")?;
        for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
            let (id, body) = row?;
            let e: crate::archive::outcomes::Evidence = serde_json::from_str(&body)?;
            let mut v = item(
                format!("outcome:{id}"),
                "outcome",
                id.clone(),
                e.reclaim_state.clone().unwrap_or_else(|| "measured".into()),
            );
            v["jobId"] = json!(id);
            // Explicit projection: never upload source configuration, paths or raw errors.
            let snap = |s: Option<&crate::archive::outcomes::Snapshot>| {
                s.map(|s|json!({"observedAtUnixMs":s.observed_at_unix_ms,"tableBytes":s.table_bytes,"indexBytes":s.index_bytes,"heapFreeBytes":s.heap_free_bytes,"physicalAllocatedBytes":s.physical_allocated_bytes}))
            };
            v["outcomes"] = json!({
                "beforeDeletion":snap(e.before_deletion.as_ref()),"afterDeletion":snap(e.after_deletion.as_ref()),
                "reclaimBefore":snap(e.reclaim.as_ref().map(|r|&r.before)),"reclaimAfter":snap(e.reclaim.as_ref().and_then(|r|r.after.as_ref())),
                "reclaimMethod":e.reclaim.as_ref().map(|r|&r.method),
                "requiredFreeBytes":e.reclaim.as_ref().and_then(|r|r.required_free_bytes),
                "minimumFreeBytes":e.reclaim.as_ref().and_then(|r|r.minimum_free_bytes),
                "billing":e.billing.map(|b|json!({"currency":b.currency,"beforeMinorUnits":b.before_minor_units,"afterMinorUnits":b.after_minor_units,"beforeStartUnixMs":b.before_start_unix_ms,"beforeEndUnixMs":b.before_end_unix_ms,"afterStartUnixMs":b.after_start_unix_ms,"afterEndUnixMs":b.after_end_unix_ms}))
            });
            out.push(v);
        }
    }
    for c in crate::connectors::inventory(paths)? {
        let pin = c.package.pin();
        let mut v = item(
            format!("connector:{}:{}:{}", pin.id, pin.version, pin.sha256),
            "connector",
            pin.id.clone(),
            if c.revoked {
                "revoked"
            } else if !c.healthy {
                "unhealthy"
            } else if c.active {
                "active"
            } else {
                "installed"
            }
            .into(),
        );
        v["version"] = json!(pin.version);
        v["hasFailure"] = json!(c.revoked || !c.healthy);
        out.push(v);
    }
    let path = paths.data_dir.join("monitoring.sqlite");
    if path.exists() {
        crate::fs_security::check_sqlite(&path)?;
        let c = rusqlite::Connection::open_with_flags(
            path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        c.busy_timeout(std::time::Duration::from_secs(2))?;
        let mut stmt =
            c.prepare("SELECT kind,entity FROM incidents WHERE active=1 ORDER BY kind,entity")?;
        for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))? {
            let (kind, entity) = row?;
            let mut v = item(
                format!("failure:{kind}:{entity}"),
                "failure",
                kind,
                "active".into(),
            );
            v["hasFailure"] = json!(true);
            out.push(v);
        }
    }
    Ok(out)
}

/// Read only allowlisted columns, avoiding full saved plans and error payloads.
pub fn jobs(paths: &StatePaths) -> Result<Vec<Value>> {
    let c = state::database::open_initialized(paths, false)?;
    let mut stmt=c.prepare("SELECT json_object('id',id,'policyName',policy_name,'source',json_extract(plan_json,'$.policy.source'),'destination',json_extract(plan_json,'$.policy.destination'),'schema',json_extract(plan_json,'$.policy.config.schema'),'table',json_extract(plan_json,'$.policy.config.table'),'status',status,'rowsProcessed',rows_processed,'bytesWritten',bytes_written,'objectsCreated',objects_created,'startedAt',started_at,'completedAt',completed_at,'verifiedAt',verified_at,'stage',json_extract(progress_json,'$.stage'),'observedAtUnixMs',json_extract(progress_json,'$.observed_at_unix_ms'),'imported',json(CASE WHEN imported_root IS NULL THEN 'false' ELSE 'true' END)) FROM jobs ORDER BY started_at DESC,id")?;
    let mut out = Vec::new();
    let mut bytes = 0;
    for row in stmt.query_map([], |r| r.get::<_, String>(0))? {
        let json = row?;
        bytes += json.len();
        anyhow::ensure!(
            out.len() < 102400 && bytes <= 40 * 1024 * 1024,
            "job metadata exceeds report budget"
        );
        out.push(serde_json::from_str(&json)?);
    }
    Ok(out)
}
