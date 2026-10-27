//! Observations are not savings attribution. Missing evidence stays unknown.
use super::checkpoint;
use crate::{
    error::Error,
    paths::StatePaths,
    source::connector::{DeleteSpec, Request, launch_in_state},
    state::{archive as jobs, sources},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub observed_at_unix_ms: u64,
    pub table_bytes: u64,
    pub index_bytes: u64,
    /// Exact heap free space from pgstattuple, when available; not index/TOAST space.
    pub heap_free_bytes: Option<u64>,
    pub filenode: u32,
    pub physical_allocated_bytes: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimOptions {
    pub window_start_unix_ms: u64,
    pub window_end_unix_ms: u64,
    pub wait_timeout_seconds: u32,
    pub max_duration_seconds: u32,
}
impl ReclaimOptions {
    pub fn remaining_ms(&self, now: u64) -> Result<u64, Error> {
        if self.window_start_unix_ms > now
            || self.window_end_unix_ms <= now
            || self
                .window_end_unix_ms
                .saturating_sub(self.window_start_unix_ms)
                > 86_400_000
            || !(1..=290).contains(&self.max_duration_seconds)
            || u64::from(self.max_duration_seconds) <= u64::from(self.wait_timeout_seconds) + 1
            || !(1..=60).contains(&self.wait_timeout_seconds)
            || self.window_end_unix_ms - now < u64::from(self.wait_timeout_seconds) * 1000 + 1000
        {
            return Err(Error::Archive(
                "reclaim requires an active window of at most 24 hours and a 1-60 second lock wait within it",
            ));
        }
        Ok((self.window_end_unix_ms - now).min(u64::from(self.max_duration_seconds) * 1000))
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReclaimResult {
    /// Sanitized local-only reason; never projected into Cloud.
    pub failure: Option<String>,
    pub method: String,
    pub status: String,
    pub before: Snapshot,
    pub after: Option<Snapshot>,
    pub required_free_bytes: Option<u64>,
    pub minimum_free_bytes: Option<u64>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub before_deletion: Option<Snapshot>,
    pub after_deletion: Option<Snapshot>,
    pub reclaim: Option<ReclaimResult>,
    pub reclaim_state: Option<String>,
    pub reclaim_window: Option<ReclaimOptions>,
    pub billing: Option<Billing>,
}
/// Operator-entered invoice observations, never a projection or causal savings claim.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Billing {
    pub currency: String,
    pub before_minor_units: u64,
    pub after_minor_units: u64,
    pub before_start_unix_ms: u64,
    pub before_end_unix_ms: u64,
    pub after_start_unix_ms: u64,
    pub after_end_unix_ms: u64,
}
impl Billing {
    pub fn validate(&self, now: u64) -> Result<(), Error> {
        if self.currency.len() != 3
            || !self.currency.bytes().all(|b| b.is_ascii_uppercase())
            || self.before_minor_units > 9_007_199_254_740_991
            || self.after_minor_units > 9_007_199_254_740_991
            || self.before_start_unix_ms >= self.before_end_unix_ms
            || self.before_end_unix_ms > self.after_start_unix_ms
            || self.after_start_unix_ms >= self.after_end_unix_ms
            || self.after_end_unix_ms > now
        {
            return Err(Error::Archive(
                "billing requires a three-letter currency, nonnegative amounts and ordered, nonoverlapping completed periods",
            ));
        }
        Ok(())
    }
}
fn bad() -> Error {
    Error::Archive("cannot read or persist outcome evidence")
}
fn db(paths: &StatePaths) -> Result<Connection, Error> {
    if crate::state::status(paths)?.is_none() {
        return Err(Error::NotInitialized);
    }
    let path = paths.data_dir.join("outcomes.sqlite");
    crate::fs_security::sqlite(&path)?;
    let c = Connection::open(path).map_err(|_| bad())?;
    c.busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|_| bad())?;
    c.execute_batch(
        "CREATE TABLE IF NOT EXISTS outcomes(job_id TEXT PRIMARY KEY,body TEXT NOT NULL)",
    )
    .map_err(|_| bad())?;
    Ok(c)
}
pub fn evidence(paths: &StatePaths, id: &str) -> Result<Evidence, Error> {
    jobs::job_show(paths, id)?;
    if !paths.data_dir.join("outcomes.sqlite").exists() {
        return Ok(Evidence::default());
    }
    let body: Option<String> = db(paths)?
        .query_row("SELECT body FROM outcomes WHERE job_id=?1", [id], |r| {
            r.get(0)
        })
        .optional()
        .map_err(|_| bad())?;
    body.map(|s| serde_json::from_str(&s).map_err(|_| bad()))
        .transpose()
        .map(Option::unwrap_or_default)
}
fn save(paths: &StatePaths, id: &str, e: &Evidence) -> Result<(), Error> {
    db(paths)?.execute("INSERT INTO outcomes(job_id,body) VALUES(?1,?2) ON CONFLICT(job_id) DO UPDATE SET body=excluded.body",params![id,serde_json::to_string(e).map_err(|_|bad())?]).map_err(|_|bad())?;
    Ok(())
}
pub fn report(paths: &StatePaths, id: &str) -> Result<serde_json::Value, Error> {
    let job = jobs::job_show(paths, id)?;
    let deletion = crate::state::deletion::show(paths, id)?;
    let e = evidence(paths, id)?;
    let physical = e.reclaim.as_ref().and_then(|r| {
        Some(
            r.before
                .physical_allocated_bytes?
                .saturating_sub(r.after.as_ref()?.physical_allocated_bytes?),
        )
    });

    Ok(
        serde_json::json!({"job_id":id,"archive_bytes_written":job.bytes_written,"deletion":deletion,"evidence":e,"physical_bytes_returned_observed_net":physical,"projected_savings":null,"attribution":"Point-in-time observations; concurrent writes and other maintenance can change sizes. Billing is operator-observed, not attributed to coldctl."}),
    )
}
/// Caller owns the job lock. A late baseline is never invented on resume.
pub(crate) async fn capture(
    paths: &StatePaths,
    id: &str,
    client: &mut crate::source::connector::Client,
    spec: &DeleteSpec,
    before: bool,
    first: bool,
) -> Result<(), Error> {
    if !client.hello.features.contains("postgres-outcomes-v1") {
        return Ok(());
    }
    let mut e = evidence(paths, id)?;
    if before && (!first || e.before_deletion.is_some()) {
        return Ok(());
    }
    let s: Snapshot = client
        .call(Request::MeasurePostgres(spec.clone()), 30_000, false)
        .await?;
    if before {
        e.before_deletion = Some(s);
    } else {
        e.after_deletion = Some(s);
    }
    save(paths, id, &e)
}
pub async fn reclaim(
    paths: &StatePaths,
    id: &str,
    options: ReclaimOptions,
) -> Result<ReclaimResult, Error> {
    options.remaining_ms(crate::protection::now_ms())?;
    let _job = checkpoint::lock(paths, id)?;
    let job = jobs::job_show(paths, id)?;
    let _policy = checkpoint::policy_lock(paths, &job.plan.policy.id)?;
    if job.imported
        || job.status != jobs::JobStatus::Completed
        || job.plan.safety.is_none()
        || !job.plan.parent_selection.is_empty()
        || !crate::state::deletion::show(paths, id)?.is_some_and(|p| p.completed)
    {
        return Err(Error::Archive(
            "reclaim requires a completed original single-table archive and completed verified deletion",
        ));
    }
    let journal = checkpoint::load(paths, id)?;
    let source = sources::show(paths, &job.plan.policy.source)?;
    if source.id != journal.source_id || source.connection.engine() != "postgres" {
        return Err(Error::Archive("original PostgreSQL source required"));
    }
    crate::protection::admit(paths, &job.plan.policy.source)?;
    let mut client = launch_in_state(&source.connection, paths, None).await?;
    if !client.hello.features.contains("postgres-outcomes-v1") {
        return Err(Error::Archive(
            "connector does not support PostgreSQL outcomes",
        ));
    }
    let mut e = evidence(paths, id)?;
    if e.reclaim_state
        .as_deref()
        .is_some_and(|s| s == "running" || s.ends_with("inspect_required"))
    {
        return Err(Error::Archive(
            "previous reclaim was interrupted; inspect PostgreSQL and use archive reclaim-acknowledge before retrying",
        ));
    }
    let deadline = options.remaining_ms(crate::protection::now_ms())?;
    e.reclaim_state = Some("running".into());
    e.reclaim_window = Some(options.clone());
    e.reclaim = None;
    save(paths, id, &e)?;
    let result = client
        .call::<_, ReclaimResult>(
            Request::ReclaimPostgres {
                spec: DeleteSpec {
                    plan: job.plan,
                    source_identity: journal.source_identity,
                    require_empty_children: false,
                },
                options,
            },
            (deadline + 5000) as u32,
            true,
        )
        .await;
    match result {
        Ok(r) => {
            e.reclaim_state = Some(r.status.clone());
            e.reclaim = Some(r.clone());
            save(paths, id, &e)?;
            Ok(r)
        }
        // Transport failure may follow source mutation. Retain running/unfinalized, requiring inspection.
        Err(err) => Err(err.into()),
    }
}
pub fn acknowledge(paths: &StatePaths, id: &str) -> Result<(), Error> {
    let _lock = checkpoint::lock(paths, id)?;
    let mut e = evidence(paths, id)?;
    if !e
        .reclaim_state
        .as_deref()
        .is_some_and(|s| s == "running" || s.ends_with("inspect_required"))
    {
        return Err(Error::Archive("no interrupted reclaim to acknowledge"));
    }
    e.reclaim_state = Some("interrupted_acknowledged".into());
    save(paths, id, &e)
}
pub fn billing(paths: &StatePaths, id: &str, billing: Billing) -> Result<(), Error> {
    billing.validate(crate::protection::now_ms())?;
    let _lock = checkpoint::lock(paths, id)?;
    let mut e = evidence(paths, id)?;
    e.billing = Some(billing);
    save(paths, id, &e)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn windows_fail_closed() {
        let mut o = ReclaimOptions {
            window_start_unix_ms: 1000,
            window_end_unix_ms: 10000,
            wait_timeout_seconds: 2,
            max_duration_seconds: 240,
        };
        assert!(o.remaining_ms(999).is_err());
        assert!(o.remaining_ms(1000).is_ok());
        assert!(o.remaining_ms(8000).is_err());
        assert!(o.remaining_ms(10000).is_err());
        o.max_duration_seconds = 291;
        assert!(o.remaining_ms(1000).is_err());
        o.max_duration_seconds = 240;
        o.wait_timeout_seconds = u32::MAX;
        assert!(o.remaining_ms(1000).is_err());
        o.wait_timeout_seconds = 2;
        o.window_end_unix_ms = 86_402_000;
        assert!(o.remaining_ms(1000).is_err());
    }
    #[test]
    fn billing_requires_comparable_evidence_periods() {
        let mut b = Billing {
            currency: "USD".into(),
            before_minor_units: 100,
            after_minor_units: 120,
            before_start_unix_ms: 1,
            before_end_unix_ms: 10,
            after_start_unix_ms: 10,
            after_end_unix_ms: 20,
        };
        assert!(b.validate(20).is_ok());
        b.after_start_unix_ms = 9;
        assert!(b.validate(20).is_err());
    }
}
