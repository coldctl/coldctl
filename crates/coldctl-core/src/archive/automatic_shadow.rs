//! Automatic pre-deletion validation. Receipts authorize only one immutable job.
use super::{checkpoint, controls::Shutdown, shadow};
use crate::{
    error::Error,
    paths::StatePaths,
    protection,
    state::{archive::Job, deletion},
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub gateway_url: String,
    pub dataset_id: String,
    pub token_env: String,
    /// Separate administrator credential used only for catalog refresh.
    pub refresh_token_env: String,
}
impl Config {
    pub fn validate(&self) -> Result<(), Error> {
        shadow::validate_gateway(&self.gateway_url, &self.dataset_id)?;
        for name in [&self.token_env, &self.refresh_token_env] {
            if name.is_empty()
                || name.len() > 128
                || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                || name.as_bytes()[0].is_ascii_digit()
            {
                return Err(Error::Archive(
                    "automatic shadow requires credential environment-variable names",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn binding(&self) -> Result<String, Error> {
        self.validate()?;
        Ok(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).map_err(|_| invalid())?)
        ))
    }
    pub(crate) async fn refresh(&self) -> Result<(), Error> {
        let mut url = shadow::validate_gateway(&self.gateway_url, &self.dataset_id)?;
        url.set_path("/api/admin/catalog/refresh");
        let token = std::env::var(&self.refresh_token_env).map_err(|_| {
            Error::Archive("gateway refresh credential environment variable unavailable")
        })?;
        if token.is_empty() {
            return Err(Error::Archive(
                "gateway refresh credential environment variable empty",
            ));
        }
        let response = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .map_err(|_| invalid())?
            .post(url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|_| transient())?;
        if response.status().is_server_error() || response.status().as_u16() == 429 {
            return Err(transient());
        }
        if !response.status().is_success() {
            return Err(Error::Archive(
                "gateway catalog refresh rejected; inspect permissions and configuration",
            ));
        }
        Ok(())
    }
}
fn invalid() -> Error {
    Error::Archive("automatic shadow evidence or configuration changed; refusing deletion")
}
fn transient() -> Error {
    Error::Archive("automatic shadow operation timed out or unavailable")
}
fn policy(paths: &StatePaths, job: &Job) -> Result<Option<protection::Policy>, Error> {
    let state = protection::status(paths, &job.plan.policy.source)?;
    state
        .get("policy")
        .map(|v| {
            let policy: protection::Policy =
                serde_json::from_value(v.clone()).map_err(|_| invalid())?;
            policy.validate()?;
            Ok(policy)
        })
        .transpose()
}
fn valid(
    policy: &protection::Policy,
    report: &shadow::Report,
    job: &Job,
    manifest: &str,
    binding: &str,
    now: u64,
) -> bool {
    // Empty verified jobs have nothing to delete; they must not stall recurring schedules.
    let empty = job.objects_created == 0
        && job.rows_processed == 0
        && report.rows == 0
        && report.gateway_lookups == 0
        && report.status == "passed"
        && report.gateway_checked
        && report.source_mismatches == 0
        && report.gateway_mismatches == 0
        && report.manifest_sha256.as_deref() == Some(manifest)
        && report
            .finished_at_unix_ms
            .is_some_and(|end| end >= report.started_at_unix_ms && end <= now)
        && now
            .checked_sub(report.started_at_unix_ms)
            .is_some_and(|age| age <= policy.shadow_max_age_seconds * 1000);
    report.job_id == job.id
        && report.batches == job.objects_created as u64
        && report.failure.is_none()
        && report.automatic_binding.as_deref() == Some(binding)
        && (empty
            || protection::report_allows_delete(
                policy,
                report,
                job.rows_processed as u64,
                manifest,
                now,
            ))
}
fn db(paths: &StatePaths) -> Result<rusqlite::Connection, Error> {
    let db = shadow::db(paths)?;
    db.execute_batch("CREATE TABLE IF NOT EXISTS automatic_shadow_receipts(job_id TEXT PRIMARY KEY, manifest TEXT NOT NULL, binding TEXT NOT NULL, report TEXT NOT NULL, authorized_at INTEGER NOT NULL)").map_err(|_| invalid())?;
    Ok(db)
}
fn receipt(paths: &StatePaths, job: &Job, manifest: &str, binding: &str) -> Result<bool, Error> {
    let row: Option<(String, String, String, i64)> = db(paths)?.query_row(
        "SELECT manifest,binding,report,authorized_at FROM automatic_shadow_receipts WHERE job_id=?1", [&job.id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).optional().map_err(|_| invalid())?;
    let Some((saved_manifest, saved_binding, body, at)) = row else {
        return Ok(false);
    };
    let at = u64::try_from(at).map_err(|_| invalid())?;
    let report: shadow::Report = serde_json::from_str(&body).map_err(|_| invalid())?;
    // Recheck the original evidence at authorization time, not against today's age.
    let original_policy = protection::Policy {
        shadow_max_age_seconds: 86400,
        ..Default::default()
    };
    if saved_manifest != manifest
        || saved_binding != binding
        || report.job_id != job.id
        || at > protection::now_ms()
        || !valid(&original_policy, &report, job, manifest, binding, at)
    {
        return Err(invalid());
    }
    Ok(true)
}

/// Called under the deletion job/policy locks. A receipt is useful only with a deletion journal.
pub(crate) fn authorize(
    paths: &StatePaths,
    job: &Job,
    manifest: &str,
    policy: &protection::Policy,
) -> Result<bool, Error> {
    let Some(config) = policy.automatic_shadow.get(&job.plan.policy.name) else {
        return Ok(false);
    };
    let binding = config.binding()?;
    if let Some(progress) = deletion::show(paths, &job.id)? {
        if progress.manifest_sha256 != manifest || !receipt(paths, job, manifest, &binding)? {
            return Err(invalid());
        }
        return Ok(true);
    }
    let reports = shadow::reports(paths, &job.id)?;
    let now = protection::now_ms();
    let report = reports
        .first()
        .filter(|r| valid(policy, r, job, manifest, &binding, now))
        .ok_or(Error::Archive(
            "automatic shadow requires a complete current passing report before deletion",
        ))?;
    db(paths)?.execute("INSERT INTO automatic_shadow_receipts(job_id,manifest,binding,report,authorized_at) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(job_id) DO UPDATE SET manifest=excluded.manifest,binding=excluded.binding,report=excluded.report,authorized_at=excluded.authorized_at",
        params![job.id,manifest,binding,serde_json::to_string(report).map_err(|_| invalid())?,now as i64]).map_err(|_| invalid())?;
    Ok(true)
}

/// Run before deletion, retaining existing explicit schedule deletion authorization.
pub(crate) async fn prepare(
    paths: &StatePaths,
    id: &str,
    queue: &str,
    shutdown: Shutdown,
) -> Result<(), Error> {
    let job = crate::state::archive::job_show(paths, id)?;
    let Some(policy) = policy(paths, &job)? else {
        return Ok(());
    };
    let Some(config) = policy.automatic_shadow.get(&job.plan.policy.name) else {
        return Ok(());
    };
    protection::admit(paths, &job.plan.policy.source)?;
    let cp = checkpoint::load(paths, id)?;
    let manifest = cp.manifest.ok_or_else(invalid)?;
    let binding = config.binding()?;
    if deletion::show(paths, id)?.is_some() {
        if !receipt(paths, &job, &manifest.sha256, &binding)? {
            return Err(invalid());
        }
        return Ok(());
    }
    if shadow::reports(paths, id)?.first().is_some_and(|r| {
        valid(
            &policy,
            r,
            &job,
            &manifest.sha256,
            &binding,
            protection::now_ms(),
        )
    }) {
        return Ok(());
    }
    let report = shadow::run_inner(
        paths,
        id,
        shadow::Options {
            confirm_original_source: true,
            gateway_url: Some(config.gateway_url.clone()),
            dataset_id: Some(config.dataset_id.clone()),
            token_env: Some(config.token_env.clone()),
            shutdown,
            ..Default::default()
        },
        Some(config),
        Some(queue),
    )
    .await?;
    match report.status.as_str() {
        "passed" => Ok(()),
        "incomplete" => Err(Error::Archive(protection::PAUSED)),
        "mismatch" => Err(Error::Archive(
            "automatic shadow found differing source or gateway records; deletion blocked; inspect shadow reports",
        )),
        _ if report.retryable => Err(transient()),
        _ => Err(Error::Archive(
            "automatic shadow failed; deletion blocked; inspect shadow reports and gateway configuration",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        archive::planner::ArchivePlan,
        policy::model::PolicyConfig,
        source::SourceConnection,
        state::{self, archive, destinations, sources},
    };

    fn config() -> Config {
        Config {
            gateway_url: "https://archive.example.com".into(),
            dataset_id: "dataset".into(),
            token_env: "READ_TOKEN".into(),
            refresh_token_env: "REFRESH_TOKEN".into(),
        }
    }
    fn fixture() -> (
        tempfile::TempDir,
        StatePaths,
        Job,
        protection::Policy,
        shadow::Report,
    ) {
        let temp = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(temp.path())).unwrap();
        state::initialize(&paths, "test").unwrap();
        let source = sources::add(
            &paths,
            "source",
            SourceConnection::from_url_env("UNUSED_TEST".into()).unwrap(),
        )
        .unwrap();
        destinations::add_local(&paths, "disk", &temp.path().join("archive")).unwrap();
        archive::policy_create(
            &paths,
            "orders",
            "source",
            "disk",
            PolicyConfig {
                schema: "public".into(),
                table: "orders".into(),
                time_column: "created_at".into(),
                older_than_days: 365,
                batch_size: 2,
                equals_column: None,
                equals_value: None,
            },
        )
        .unwrap();
        let plan = ArchivePlan {
            connector_pin: None,
            policy: archive::policy_show(&paths, "orders").unwrap(),
            destination_config: None,
            destination_path: temp.path().join("archive"),
            cutoff_utc: "2025-01-01T00:00:00Z".into(),
            primary_key: "id".into(),
            table_oid: 42,
            columns: vec![],
            estimated_rows: None,
            delete: false,
            warnings: vec![],
            safety: None,
        };
        let mut job = archive::job_create(
            &paths,
            &plan,
            &source.id,
            "identity",
            None,
            &Default::default(),
        )
        .unwrap();
        job.rows_processed = 3;
        job.objects_created = 2;
        let policy = protection::Policy {
            require_gateway_shadow: true,
            shadow_max_age_seconds: 30,
            automatic_shadow: [("orders".into(), config())].into(),
            ..Default::default()
        };
        let now = protection::now_ms();
        let report = shadow::Report {
            id: "report".into(),
            job_id: job.id.clone(),
            started_at_unix_ms: now - 10,
            finished_at_unix_ms: Some(now),
            status: "passed".into(),
            batches: 2,
            rows: 3,
            source_mismatches: 0,
            gateway_mismatches: 0,
            gateway_checked: true,
            gateway_lookups: 3,
            manifest_sha256: Some("manifest".into()),
            failure: None,
            automatic_binding: Some(config().binding().unwrap()),
            retryable: false,
        };
        (temp, paths, job, policy, report)
    }
    fn save(paths: &StatePaths, report: &shadow::Report) {
        shadow::db(paths)
            .unwrap()
            .execute(
                "INSERT OR REPLACE INTO shadow_reports(id,job_id,body) VALUES(?1,?2,?3)",
                params![
                    report.id,
                    report.job_id,
                    serde_json::to_string(report).unwrap()
                ],
            )
            .unwrap();
    }
    #[test]
    fn receipt_is_bound_and_durable_but_cannot_replace_initial_validation() {
        let (_temp, paths, job, policy, mut report) = fixture();
        assert!(authorize(&paths, &job, "manifest", &policy).is_err());
        save(&paths, &report);
        assert!(authorize(&paths, &job, "manifest", &policy).unwrap());
        // A receipt without a deletion journal does not bypass a newer failed report.
        report.status = "mismatch".into();
        save(&paths, &report);
        assert!(authorize(&paths, &job, "manifest", &policy).is_err());
        report.status = "passed".into();
        save(&paths, &report);
        authorize(&paths, &job, "manifest", &policy).unwrap();
        deletion::start(&paths, &job.id, "manifest").unwrap();
        // Reports can age out or leave the latest-100 view; the immutable receipt remains.
        shadow::db(&paths)
            .unwrap()
            .execute("DELETE FROM shadow_reports", [])
            .unwrap();
        assert!(authorize(&paths, &job, "manifest", &policy).unwrap());
        assert!(authorize(&paths, &job, "another-manifest", &policy).is_err());
        let mut changed = policy.clone();
        changed
            .automatic_shadow
            .get_mut("orders")
            .unwrap()
            .dataset_id = "another-dataset".into();
        assert!(authorize(&paths, &job, "manifest", &changed).is_err());
        db(&paths)
            .unwrap()
            .execute("DELETE FROM automatic_shadow_receipts", [])
            .unwrap();
        assert!(authorize(&paths, &job, "manifest", &policy).is_err());
    }
    #[test]
    fn partial_expired_unbound_and_empty_evidence() {
        let (_temp, _paths, mut job, policy, mut report) = fixture();
        let binding = config().binding().unwrap();
        let now = protection::now_ms();
        assert!(valid(&policy, &report, &job, "manifest", &binding, now));
        assert!(!valid(
            &policy,
            &report,
            &job,
            "manifest",
            &binding,
            now + 31_000
        ));
        report.automatic_binding = None;
        assert!(!valid(&policy, &report, &job, "manifest", &binding, now));
        report.automatic_binding = Some(binding.clone());
        report.status = "incomplete".into();
        assert!(!valid(&policy, &report, &job, "manifest", &binding, now));
        report.status = "passed".into();
        report.rows = 0;
        report.gateway_lookups = 0;
        report.batches = 0;
        assert!(!valid(&policy, &report, &job, "manifest", &binding, now));
        job.rows_processed = 0;
        job.objects_created = 0;
        assert!(valid(&policy, &report, &job, "manifest", &binding, now));
    }
    #[test]
    fn config_requires_explicit_gate_and_safe_credential_references() {
        let mut policy = protection::Policy {
            automatic_shadow: [("orders".into(), config())].into(),
            ..Default::default()
        };
        assert!(policy.validate().is_err());
        policy.require_gateway_shadow = true;
        policy.shadow_max_age_seconds = 30;
        policy.validate().unwrap();
        policy.automatic_shadow.get_mut("orders").unwrap().token_env = "plaintext token".into();
        assert!(policy.validate().is_err());
    }
}
