//! Source-scoped, fail-closed admission using operator-supplied monitoring samples.
use crate::{error::Error, paths::StatePaths, state::sources};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
pub(crate) const PAUSED: &str =
    "production protection paused work; inspect protection status and fresh telemetry";
const SHADOW_REQUIRED: &str =
    "deletion requires a recent complete passing gateway shadow report for this manifest";
fn bad() -> Error {
    Error::Archive("invalid production protection configuration or state")
}
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct Policy {
    pub window_start_utc_minute: Option<u16>,
    pub window_end_utc_minute: Option<u16>,
    pub max_replica_lag_ms: Option<u64>,
    pub max_cpu_percent: Option<f64>,
    pub max_lock_waiters: Option<u64>,
    pub sample_max_age_seconds: u64,
    pub min_batch_delay_ms: u64,
    pub max_batch_delay_ms: u64,
    pub require_gateway_shadow: bool,
    pub shadow_max_age_seconds: u64,
    pub automatic_shadow:
        std::collections::BTreeMap<String, crate::archive::automatic_shadow::Config>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sample {
    pub source_id: String,
    pub observed_at_unix_ms: u64,
    pub replica_lag_ms: Option<u64>,
    pub cpu_percent: Option<f64>,
    pub lock_waiters: Option<u64>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Decision {
    pub allowed: bool,
    pub reason: String,
    pub delay_ms: u64,
}
impl Policy {
    pub fn validate(&self) -> Result<(), Error> {
        if !self.automatic_shadow.is_empty() && !self.require_gateway_shadow {
            return Err(bad());
        }
        for (name, config) in &self.automatic_shadow {
            if name.is_empty() || name.len() > 128 {
                return Err(bad());
            }
            config.validate()?;
        }
        if (self.require_gateway_shadow && !(1..=86400).contains(&self.shadow_max_age_seconds))
            || self.window_start_utc_minute.is_some() != self.window_end_utc_minute.is_some()
            || self.window_start_utc_minute.is_some_and(|n| n >= 1440)
            || self.window_end_utc_minute.is_some_and(|n| n >= 1440)
            || (self.window_start_utc_minute.is_some()
                && self.window_start_utc_minute == self.window_end_utc_minute)
            || self.max_replica_lag_ms == Some(0)
            || self.max_lock_waiters == Some(0)
            || self
                .max_cpu_percent
                .is_some_and(|n| !n.is_finite() || n <= 0.0 || n > 100.0)
            || self.min_batch_delay_ms > self.max_batch_delay_ms
            || self.max_batch_delay_ms > 60_000
            || (self.needs_sample() && !(1..=300).contains(&self.sample_max_age_seconds))
        {
            return Err(bad());
        }
        Ok(())
    }
    fn needs_sample(&self) -> bool {
        self.max_replica_lag_ms.is_some()
            || self.max_cpu_percent.is_some()
            || self.max_lock_waiters.is_some()
    }
    pub fn evaluate(&self, sample: Option<&Sample>, now_ms: u64) -> Decision {
        let deny = |reason: &str| Decision {
            allowed: false,
            reason: reason.into(),
            delay_ms: 0,
        };
        if self.validate().is_err() {
            return deny("invalid policy");
        }
        if let (Some(start), Some(end)) = (self.window_start_utc_minute, self.window_end_utc_minute)
        {
            let minute = ((now_ms / 60_000) % 1440) as u16;
            let inside = if start < end {
                minute >= start && minute < end
            } else {
                minute >= start || minute < end
            };
            if !inside {
                return deny("outside UTC operating window");
            }
        }
        let mut pressure = 0.0_f64;
        if self.needs_sample() {
            let Some(s) = sample else {
                return deny("monitoring sample unavailable");
            };
            if s.observed_at_unix_ms > now_ms
                || now_ms - s.observed_at_unix_ms > self.sample_max_age_seconds * 1000
            {
                return deny("monitoring sample stale or from future");
            }
            for (value, limit) in [
                (
                    s.replica_lag_ms.map(|n| n as f64),
                    self.max_replica_lag_ms.map(|n| n as f64),
                ),
                (s.cpu_percent, self.max_cpu_percent),
                (
                    s.lock_waiters.map(|n| n as f64),
                    self.max_lock_waiters.map(|n| n as f64),
                ),
            ] {
                if let Some(limit) = limit {
                    let Some(value) = value else {
                        return deny("required metric unavailable");
                    };
                    if !value.is_finite() || value < 0.0 || value >= limit {
                        return deny("production metric reached limit");
                    }
                    pressure = pressure.max(value / limit);
                }
            }
        }
        Decision {
            allowed: true,
            reason: "admitted".into(),
            delay_ms: self.min_batch_delay_ms
                + ((self.max_batch_delay_ms - self.min_batch_delay_ms) as f64 * pressure * pressure)
                    as u64,
        }
    }
}
fn db(paths: &StatePaths) -> Result<Connection, Error> {
    if crate::state::status(paths)?.is_none() {
        return Err(Error::NotInitialized);
    }
    let path = paths.data_dir.join("protection.sqlite");
    crate::fs_security::sqlite(&path)?;
    let db = Connection::open(path).map_err(|_| bad())?;
    db.busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|_| bad())?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS protection(source_id TEXT PRIMARY KEY, policy TEXT NOT NULL, sample TEXT, decision TEXT)").map_err(|_|bad())?;
    Ok(db)
}
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub fn set(paths: &StatePaths, source: &str, policy: Policy) -> Result<(), Error> {
    policy.validate()?;
    let id = sources::show(paths, source)?.id;
    db(paths)?.execute("INSERT INTO protection(source_id,policy) VALUES (?1,?2) ON CONFLICT(source_id) DO UPDATE SET policy=excluded.policy,sample=NULL,decision=NULL",params![id,serde_json::to_string(&policy).map_err(|_|bad())?]).map_err(|_|bad())?;
    Ok(())
}
pub fn sample(paths: &StatePaths, source: &str, sample: Sample) -> Result<(), Error> {
    let id = sources::show(paths, source)?.id;
    if sample.source_id != id
        || sample.observed_at_unix_ms > now_ms()
        || sample
            .cpu_percent
            .is_some_and(|n| !n.is_finite() || !(0.0..=100.0).contains(&n))
    {
        return Err(bad());
    }
    let changed = db(paths)?.execute("UPDATE protection SET sample=?2 WHERE source_id=?1 AND (sample IS NULL OR CAST(json_extract(sample,'$.observed_at_unix_ms') AS INTEGER) < ?3)",params![id,serde_json::to_string(&sample).map_err(|_|bad())?,sample.observed_at_unix_ms as i64]).map_err(|_|bad())?;
    if changed != 1 {
        return Err(Error::Archive(
            "protection policy missing or sample is not newer",
        ));
    }
    Ok(())
}
pub fn status(paths: &StatePaths, source: &str) -> Result<serde_json::Value, Error> {
    let id = sources::show(paths, source)?.id;
    let row: Option<(String, Option<String>, Option<String>)> = db(paths)?
        .query_row(
            "SELECT policy,sample,decision FROM protection WHERE source_id=?1",
            [&id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(|_| bad())?;
    Ok(match row {
        Some((p, s, d)) => {
            serde_json::json!({"source_id":id,"policy":serde_json::from_str::<serde_json::Value>(&p).map_err(|_|bad())?,"sample":s.and_then(|s|serde_json::from_str::<serde_json::Value>(&s).ok()),"last_decision":d.and_then(|s|serde_json::from_str::<serde_json::Value>(&s).ok())})
        }
        None => serde_json::json!({"source_id":id,"enabled":false}),
    })
}
pub fn clear(paths: &StatePaths, source: &str) -> Result<(), Error> {
    let id = sources::show(paths, source)?.id;
    db(paths)?
        .execute("DELETE FROM protection WHERE source_id=?1", [id])
        .map_err(|_| bad())?;
    Ok(())
}
pub fn check(paths: &StatePaths, source: &str) -> Result<Decision, Error> {
    let id = sources::show(paths, source)?.id;
    let db = db(paths)?;
    let row: Option<(String, Option<String>)> = db
        .query_row(
            "SELECT policy,sample FROM protection WHERE source_id=?1",
            [&id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(|_| bad())?;
    let Some((p, s)) = row else {
        return Ok(Decision {
            allowed: true,
            reason: "no policy configured".into(),
            delay_ms: 0,
        });
    };
    let policy: Policy = serde_json::from_str(&p).map_err(|_| bad())?;
    let sample: Option<Sample> = s
        .map(|s| serde_json::from_str(&s))
        .transpose()
        .map_err(|_| bad())?;
    if sample.as_ref().is_some_and(|s| s.source_id != id) {
        return Err(bad());
    }
    let result = policy.evaluate(sample.as_ref(), now_ms());
    db.execute(
        "UPDATE protection SET decision=?2 WHERE source_id=?1",
        params![id, serde_json::to_string(&result).map_err(|_| bad())?],
    )
    .map_err(|_| bad())?;
    Ok(result)
}
pub fn admit(paths: &StatePaths, source: &str) -> Result<(), Error> {
    if !check(paths, source)?.allowed {
        return Err(Error::Archive(PAUSED));
    }
    Ok(())
}
pub fn is_pause(error: &Error) -> bool {
    matches!(error,Error::Archive(reason) if *reason==PAUSED || *reason==SHADOW_REQUIRED)
}
pub async fn boundary(
    paths: &StatePaths,
    source: &str,
    shutdown: &crate::archive::controls::Shutdown,
) -> Result<(), Error> {
    let decision = check(paths, source)?;
    if !decision.allowed {
        return Err(Error::Archive(PAUSED));
    }
    let end = std::time::Instant::now() + std::time::Duration::from_millis(decision.delay_ms);
    while std::time::Instant::now() < end {
        if shutdown.requested() {
            return Err(Error::Archive(PAUSED));
        }
        tokio::time::sleep(
            std::time::Duration::from_millis(100)
                .min(end.saturating_duration_since(std::time::Instant::now())),
        )
        .await;
    }
    admit(paths, source)
}
/// Optional deletion gate: a recent full gateway comparison tied to this manifest.
pub fn authorize_delete(
    paths: &StatePaths,
    job: &crate::state::archive::Job,
    manifest_sha256: &str,
) -> Result<(), Error> {
    let state = status(paths, &job.plan.policy.source)?;
    let Some(value) = state.get("policy") else {
        return Ok(());
    };
    let policy: Policy = serde_json::from_value(value.clone()).map_err(|_| bad())?;
    policy.validate()?;
    if !policy.require_gateway_shadow {
        return Ok(());
    }
    if crate::archive::automatic_shadow::authorize(paths, job, manifest_sha256, &policy)? {
        return Ok(());
    }
    let reports = crate::archive::shadow::reports(paths, &job.id)?;
    let valid = reports.first().is_some_and(|r| {
        report_allows_delete(
            &policy,
            r,
            job.rows_processed as u64,
            manifest_sha256,
            now_ms(),
        )
    });
    if !valid {
        return Err(Error::Archive(SHADOW_REQUIRED));
    }
    Ok(())
}

pub(crate) fn report_allows_delete(
    policy: &Policy,
    report: &crate::archive::shadow::Report,
    rows: u64,
    manifest: &str,
    now: u64,
) -> bool {
    report.status == "passed"
        && report
            .finished_at_unix_ms
            .is_some_and(|end| end >= report.started_at_unix_ms && end <= now)
        && report.gateway_checked
        && report.rows == rows
        && rows > 0
        && report.gateway_lookups == rows
        && report.source_mismatches == 0
        && report.gateway_mismatches == 0
        && report.manifest_sha256.as_deref() == Some(manifest)
        && now
            .checked_sub(report.started_at_unix_ms)
            .is_some_and(|age| age <= policy.shadow_max_age_seconds * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn deletion_evidence_requires_complete_current_matching_gateway_pass() {
        let policy = Policy {
            require_gateway_shadow: true,
            shadow_max_age_seconds: 30,
            ..Default::default()
        };
        let mut report = crate::archive::shadow::Report {
            id: "run".into(),
            job_id: "job".into(),
            started_at_unix_ms: 100_000,
            finished_at_unix_ms: Some(110_000),
            status: "passed".into(),
            batches: 1,
            rows: 3,
            source_mismatches: 0,
            gateway_mismatches: 0,
            gateway_checked: true,
            gateway_lookups: 3,
            gateway_requests: 1,
            manifest_sha256: Some("manifest".into()),
            failure: None,
            automatic_binding: None,
            retryable: false,
        };
        assert!(report_allows_delete(
            &policy, &report, 3, "manifest", 120_000
        ));
        assert!(!report_allows_delete(&policy, &report, 3, "other", 120_000));
        assert!(!report_allows_delete(
            &policy, &report, 3, "manifest", 130_001
        ));
        assert!(!report_allows_delete(
            &policy, &report, 3, "manifest", 99_999
        ));
        assert!(!report_allows_delete(
            &policy, &report, 4, "manifest", 120_000
        ));
        report.gateway_lookups = 2;
        assert!(!report_allows_delete(
            &policy, &report, 3, "manifest", 120_000
        ));
        report.gateway_lookups = 3;
        report.finished_at_unix_ms = None;
        assert!(!report_allows_delete(
            &policy, &report, 3, "manifest", 120_000
        ));
        report.finished_at_unix_ms = Some(110_000);
        report.status = "incomplete".into();
        assert!(!report_allows_delete(
            &policy, &report, 3, "manifest", 120_000
        ));
    }
    #[test]
    fn samples_are_source_bound_monotonic_and_cleared_on_policy_change() {
        let temp = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
        crate::state::initialize(&paths, "test").unwrap();
        let source = sources::add(
            &paths,
            "test",
            crate::source::SourceConnection::from_url_env("UNUSED_FIXTURE".into()).unwrap(),
        )
        .unwrap();
        let policy = Policy {
            max_cpu_percent: Some(80.0),
            sample_max_age_seconds: 30,
            ..Default::default()
        };
        set(&paths, "test", policy.clone()).unwrap();
        assert!(!check(&paths, "test").unwrap().allowed);
        let mut value = Sample {
            source_id: "other".into(),
            observed_at_unix_ms: now_ms() - 1,
            replica_lag_ms: None,
            cpu_percent: Some(20.0),
            lock_waiters: None,
        };
        assert!(sample(&paths, "test", value.clone()).is_err());
        value.source_id = source.id;
        sample(&paths, "test", value.clone()).unwrap();
        assert!(check(&paths, "test").unwrap().allowed);
        assert!(sample(&paths, "test", value.clone()).is_err());
        value.observed_at_unix_ms -= 1;
        assert!(sample(&paths, "test", value).is_err());
        set(&paths, "test", policy).unwrap();
        assert!(!check(&paths, "test").unwrap().allowed);
        clear(&paths, "test").unwrap();
        assert!(check(&paths, "test").unwrap().allowed);
    }
    #[test]
    fn windows_wrap_and_exclude_end() {
        let p = Policy {
            window_start_utc_minute: Some(1380),
            window_end_utc_minute: Some(60),
            ..Default::default()
        };
        assert!(p.evaluate(None, 0).allowed);
        assert!(!p.evaluate(None, 60 * 60_000).allowed);
        assert!(p.evaluate(None, 1380 * 60_000).allowed);
    }
    #[test]
    fn missing_stale_unknown_and_limit_fail_closed() {
        let p = Policy {
            max_replica_lag_ms: Some(2000),
            sample_max_age_seconds: 30,
            max_batch_delay_ms: 1000,
            ..Default::default()
        };
        assert!(!p.evaluate(None, 100_000).allowed);
        let mut s = Sample {
            source_id: "id".into(),
            observed_at_unix_ms: 100_000,
            replica_lag_ms: Some(1000),
            cpu_percent: None,
            lock_waiters: None,
        };
        assert_eq!(p.evaluate(Some(&s), 100_000).delay_ms, 250);
        assert!(!p.evaluate(Some(&s), 130_001).allowed);
        assert!(!p.evaluate(Some(&s), 99_999).allowed);
        s.replica_lag_ms = Some(2000);
        assert!(!p.evaluate(Some(&s), 100_000).allowed);
        s.replica_lag_ms = None;
        assert!(!p.evaluate(Some(&s), 100_000).allowed);
    }
}
