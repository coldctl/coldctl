//! Read-only comparison of the saved archive range with its source and optional gateway.
use super::{checkpoint, controls::Shutdown, verifier};
use crate::{
    error::Error,
    paths::StatePaths,
    source::{ArchiveSource, ConnectorArchive},
    state::{archive as jobs, sources},
};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
#[derive(Default)]
pub struct Options {
    pub confirm_original_source: bool,
    pub max_batches: Option<u64>,
    pub gateway_batch_size: Option<usize>,
    pub max_duration_seconds: Option<u64>,
    pub gateway_url: Option<String>,
    pub dataset_id: Option<String>,
    pub token_env: Option<String>,
    pub shutdown: Shutdown,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Report {
    pub id: String,
    pub job_id: String,
    pub started_at_unix_ms: u64,
    pub finished_at_unix_ms: Option<u64>,
    pub status: String,
    pub batches: u64,
    pub rows: u64,
    pub source_mismatches: u64,
    pub gateway_mismatches: u64,
    pub gateway_checked: bool,
    pub gateway_lookups: u64,
    #[serde(default)]
    pub gateway_requests: u64,
    pub manifest_sha256: Option<String>,
    pub failure: Option<String>,
    #[serde(default)]
    pub automatic_binding: Option<String>,
    #[serde(default)]
    pub retryable: bool,
}
fn bad() -> Error {
    Error::Archive(
        "shadow validation failed; inspect the durable report; values and credentials are omitted",
    )
}
pub(crate) fn db(paths: &StatePaths) -> Result<Connection, Error> {
    if crate::state::status(paths)?.is_none() {
        return Err(Error::NotInitialized);
    }
    let path = paths.data_dir.join("shadow.sqlite");
    crate::fs_security::sqlite(&path)?;
    let c = Connection::open(path).map_err(|_| bad())?;
    c.busy_timeout(std::time::Duration::from_secs(2))
        .map_err(|_| bad())?;
    c.execute_batch("CREATE TABLE IF NOT EXISTS shadow_reports(id TEXT PRIMARY KEY,job_id TEXT NOT NULL,body TEXT NOT NULL)").map_err(|_|bad())?;
    Ok(c)
}
fn save(paths: &StatePaths, r: &Report) -> Result<(), Error> {
    db(paths)?.execute("INSERT INTO shadow_reports(id,job_id,body) VALUES (?1,?2,?3) ON CONFLICT(id) DO UPDATE SET body=excluded.body",params![r.id,r.job_id,serde_json::to_string(r).map_err(|_|bad())?]).map_err(|_|bad())?;
    Ok(())
}
pub fn reports(paths: &StatePaths, id: &str) -> Result<Vec<Report>, Error> {
    jobs::job_show(paths, id)?;
    let c = db(paths)?;
    let mut stmt = c
        .prepare("SELECT body FROM shadow_reports WHERE job_id=?1 ORDER BY rowid DESC LIMIT 100")
        .map_err(|_| bad())?;
    let rows = stmt
        .query_map([id], |r| r.get::<_, String>(0))
        .map_err(|_| bad())?;
    rows.map(|r| serde_json::from_str(&r.map_err(|_| bad())?).map_err(|_| bad()))
        .collect()
}
fn native(value: &Option<String>, kind: &str) -> Result<Value, Error> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    Ok(match kind {
        "int2" | "int4" | "int8" => json!(value.parse::<i64>().map_err(|_| bad())?),
        "bool" => match value.as_str() {
            "t" | "true" => json!(true),
            "f" | "false" => json!(false),
            _ => return Err(bad()),
        },
        "float4" | "float8" => {
            let n = value.parse::<f64>().map_err(|_| bad())?;
            if !n.is_finite() {
                return Err(Error::Archive(
                    "shadow validation of non-finite floating values is unsupported",
                ));
            }
            json!(n)
        }
        _ => json!(value),
    })
}
fn equal(a: &Value, b: &Value, kind: &str) -> bool {
    if kind == "float4" {
        if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
            return (a as f32).to_bits() == (b as f32).to_bits();
        }
    }
    if kind == "float8" {
        if let (Some(a), Some(b)) = (a.as_f64(), b.as_f64()) {
            return a.to_bits() == b.to_bits();
        }
    }
    a == b
}
pub(crate) fn validate_gateway(origin: &str, dataset: &str) -> Result<url::Url, Error> {
    let url = url::Url::parse(origin).map_err(|_| bad())?;
    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(Error::Archive(
            "gateway must be an HTTPS origin (HTTP only on loopback), without credentials or query",
        ));
    }
    if dataset.is_empty()
        || dataset.len() > 128
        || !dataset
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(bad());
    }
    Ok(url)
}
struct Gateway {
    client: reqwest::Client,
    url: url::Url,
    token: String,
}
// Reserve room for the response envelope and record identifiers under the 1 MiB
// response cap. Six bytes bounds JSON escaping of each source UTF-8 byte.
fn gateway_batch_len(
    rows: &[Vec<Option<String>>],
    columns: &[super::batch::ArchiveColumn],
    limit: usize,
) -> usize {
    let names = columns
        .iter()
        .map(|c| c.name.len().saturating_mul(6).saturating_add(8))
        .sum::<usize>();
    let mut bytes = 0usize;
    let mut count = 0;
    for row in rows.iter().take(limit) {
        let size = row.iter().fold(names.saturating_add(512), |size, value| {
            size.saturating_add(
                value
                    .as_ref()
                    .map_or(4, |v| v.len().saturating_mul(6).saturating_add(4)),
            )
        });
        if count > 0 && bytes.saturating_add(size) > 768 * 1024 {
            break;
        }
        bytes = bytes.saturating_add(size);
        count += 1;
    }
    count
}
impl Gateway {
    fn new(options: &Options) -> Result<Option<Self>, Error> {
        if options.gateway_url.is_none()
            && options.dataset_id.is_none()
            && options.token_env.is_none()
        {
            return Ok(None);
        }
        let mut url = validate_gateway(
            options.gateway_url.as_deref().ok_or_else(bad)?,
            options.dataset_id.as_deref().ok_or_else(bad)?,
        )?;
        let dataset = options.dataset_id.as_deref().ok_or_else(bad)?;
        url.set_path(&format!("/api/datasets/{dataset}/search"));
        let token = std::env::var(options.token_env.as_deref().ok_or_else(bad)?)
            .map_err(|_| Error::Archive("gateway token environment variable unavailable"))?;
        if token.is_empty() {
            return Err(bad());
        }
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .map_err(|_| bad())?;
        Ok(Some(Self { client, url, token }))
    }
    async fn lookup(
        &self,
        plan: &super::planner::ArchivePlan,
        rows: &[Vec<Option<String>>],
    ) -> Result<Value, Error> {
        let key = plan
            .columns
            .iter()
            .position(|c| c.name == plan.primary_key)
            .ok_or_else(bad)?;
        let columns: Vec<_> = plan.columns.iter().map(|c| &c.name).collect();
        let values = rows
            .iter()
            .map(|row| native(&row[key], &plan.columns[key].postgres_type))
            .collect::<Result<Vec<_>, _>>()?;
        let query = json!({"columns":columns,"limit":rows.len() + 1,"filters":[{"column":plan.primary_key,"op":"IN","values":values}]});
        let mut response = self
            .client
            .post(self.url.clone())
            .bearer_auth(&self.token)
            .json(&query)
            .send()
            .await
            .map_err(|_| Error::Archive("gateway shadow operation timed out or unavailable"))?;
        if response.status().is_server_error() || response.status().as_u16() == 429 {
            return Err(Error::Archive(
                "gateway shadow operation timed out or unavailable",
            ));
        }
        if !response.status().is_success() {
            return Err(Error::Archive(
                "gateway lookup failed; status/body omitted; check gateway and authentication",
            ));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| bad())? {
            if bytes.len() + chunk.len() > 1_048_576 {
                return Err(Error::Archive("gateway shadow response exceeds 1 MiB"));
            }
            bytes.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&bytes).map_err(|_| bad())
    }
}
// Match by typed primary key, never by response order. Duplicates, extras,
// missing keys and truncated responses all fail closed.
fn batch_mismatches(
    primary_key: &str,
    columns: &[super::batch::ArchiveColumn],
    rows: &[Vec<Option<String>>],
    response: &Value,
) -> Result<u64, Error> {
    let records = response
        .get("records")
        .and_then(Value::as_array)
        .ok_or_else(bad)?;
    if response.get("truncated") != Some(&Value::Bool(false)) || records.len() != rows.len() {
        return Ok(rows.len() as u64);
    }
    let key = columns
        .iter()
        .position(|c| c.name == primary_key)
        .ok_or_else(bad)?;
    let mut by_key = std::collections::HashMap::new();
    for record in records {
        let data = record
            .get("data")
            .and_then(Value::as_object)
            .ok_or_else(bad)?;
        let value = data.get(primary_key).ok_or_else(bad)?;
        if by_key.insert(value.to_string(), data).is_some() {
            return Ok(rows.len() as u64);
        }
    }
    let mut mismatches = 0;
    for row in rows {
        if row.len() != columns.len() {
            return Err(bad());
        }
        let expected = native(&row[key], &columns[key].postgres_type)?.to_string();
        let matches = by_key.remove(&expected).is_some_and(|data| {
            data.len() == columns.len()
                && columns.iter().zip(row).all(|(c, value)| {
                    data.get(&c.name).is_some_and(|actual| {
                        native(value, &c.postgres_type)
                            .is_ok_and(|expected| equal(actual, &expected, &c.postgres_type))
                    })
                })
        });
        if !matches {
            mismatches += 1;
        }
    }
    Ok(mismatches)
}
pub async fn run(paths: &StatePaths, id: &str, options: Options) -> Result<Report, Error> {
    run_inner(paths, id, options, None, None).await
}
pub(crate) async fn run_inner(
    paths: &StatePaths,
    id: &str,
    options: Options,
    automatic: Option<&super::automatic_shadow::Config>,
    queue: Option<&str>,
) -> Result<Report, Error> {
    if !options.confirm_original_source
        || options.max_batches == Some(0)
        || options.max_duration_seconds == Some(0)
        || !(1..=100).contains(&options.gateway_batch_size.unwrap_or(64))
    {
        return Err(Error::Archive(
            "shadow requires original source confirmation, positive limits, and gateway batch size 1-100",
        ));
    }
    let _job = checkpoint::lock(paths, id)?;
    let job = jobs::job_show(paths, id)?;
    if job.imported || job.status != jobs::JobStatus::Completed || job.plan.safety.is_none() {
        return Err(Error::Archive(
            "shadow requires a completed original archive with retained stable source data",
        ));
    }
    let _policy = checkpoint::policy_lock(paths, &job.plan.policy.id)?;
    if crate::state::deletion::show(paths, id)?.is_some() {
        return Err(Error::Archive(
            "shadow requires an archive with no deletion history",
        ));
    }
    let mut report = Report {
        id: uuid::Uuid::new_v4().to_string(),
        job_id: id.into(),
        started_at_unix_ms: crate::protection::now_ms(),
        finished_at_unix_ms: None,
        status: "running".into(),
        batches: 0,
        rows: 0,
        source_mismatches: 0,
        gateway_mismatches: 0,
        gateway_checked: options.gateway_url.is_some(),
        gateway_lookups: 0,
        gateway_requests: 0,
        manifest_sha256: None,
        failure: None,
        automatic_binding: automatic.map(|c| c.binding()).transpose()?,
        retryable: false,
    };
    save(paths, &report)?;
    let work = compare(paths, &job, &options, &mut report, automatic, queue);
    let outcome = if let Some(seconds) = options.max_duration_seconds {
        match tokio::time::timeout(std::time::Duration::from_secs(seconds), work).await {
            Ok(result) => result,
            Err(_) => {
                report.failure =
                    Some("shadow duration budget reached; coverage is incomplete".into());
                Ok(false)
            }
        }
    } else {
        work.await
    };
    match outcome {
        Ok(complete) => {
            report.status = if !complete {
                "incomplete"
            } else if report.source_mismatches + report.gateway_mismatches > 0 {
                "mismatch"
            } else {
                "passed"
            }
            .into()
        }
        Err(error) => {
            report.status = if crate::protection::is_pause(&error) {
                "incomplete"
            } else {
                "error"
            }
            .into();
            report.retryable = error.failure().retryable;
            report.failure = Some(error.failure().message);
        }
    }
    report.finished_at_unix_ms = Some(crate::protection::now_ms());
    save(paths, &report)?;
    Ok(report)
}
async fn compare(
    paths: &StatePaths,
    job: &jobs::Job,
    options: &Options,
    report: &mut Report,
    automatic: Option<&super::automatic_shadow::Config>,
    queue: Option<&str>,
) -> Result<bool, Error> {
    crate::protection::admit(paths, &job.plan.policy.source)?;
    verifier::verify_observed(paths, &job.id, None)?;
    let cp = checkpoint::load(paths, &job.id)?;
    report.manifest_sha256 = cp.manifest.as_ref().map(|m| m.sha256.clone());
    save(paths, report)?;
    if let Some(config) = automatic {
        if stopped(paths, options, queue)? {
            return Ok(false);
        }
        config.refresh().await?;
    }
    crate::protection::admit(paths, &job.plan.policy.source)?;
    let gateway = Gateway::new(options)?;
    let source = sources::show(paths, &job.plan.policy.source)?;
    if source.id != cp.source_id {
        return Err(bad());
    }
    let mut reader = ConnectorArchive::connect_in_state(
        source.connection,
        paths,
        job.plan.connector_pin.as_ref(),
    )
    .await?;
    if reader.identity() != cp.source_identity {
        return Err(bad());
    }
    reader.configure_timeouts(&job.execution).await?;
    reader.validate_resume(&job.plan).await?;
    reader.prepare_parents(paths, &job.plan).await?;
    let mut last = None;
    for sequence in 1..=job.objects_created {
        if stopped(paths, options, queue)?
            || options.max_batches.is_some_and(|n| report.batches >= n)
        {
            return Ok(false);
        }
        crate::protection::boundary(paths, &job.plan.policy.source, &options.shutdown).await?;
        let (entry, committed) = checkpoint::entry(paths, &job.id, sequence)?.ok_or_else(bad)?;
        if !committed {
            return Err(bad());
        }
        let archived = crate::source::postgres_restore::read_batch(paths, job, sequence)?;
        // The connector fixes the upper bound for the scan; row credit may vary.
        let mut plan = job.plan.clone();
        plan.policy.config.batch_size = archived.len() as i32;
        crate::protection::admit(paths, &job.plan.policy.source)?;
        let actual = reader
            .read_batch(&plan, last, cp.upper.ok_or_else(bad)?)
            .await?;
        let mut source_matches = actual
            .as_ref()
            .is_some_and(|b| b.last_key == entry.batch.last_key && b.rows.len() == archived.len());
        if let Some(actual) = actual.as_ref() {
            for (a, b) in actual.rows.iter().zip(&archived) {
                for ((a, b), c) in a.iter().zip(b).zip(&plan.columns) {
                    if !equal(
                        &native(a, &c.postgres_type)?,
                        &native(b, &c.postgres_type)?,
                        &c.postgres_type,
                    ) {
                        source_matches = false;
                    }
                }
            }
        }
        if !source_matches {
            report.source_mismatches += 1;
        }
        if let Some(gateway) = &gateway {
            let mut remaining = archived.as_slice();
            while !remaining.is_empty() {
                let count = gateway_batch_len(
                    remaining,
                    &plan.columns,
                    options.gateway_batch_size.unwrap_or(64),
                );
                let (rows, rest) = remaining.split_at(count);
                if stopped(paths, options, queue)? {
                    return Ok(false);
                }
                crate::protection::boundary(paths, &job.plan.policy.source, &options.shutdown)
                    .await?;
                report.gateway_requests += 1;
                let response = gateway.lookup(&plan, rows).await?;
                report.gateway_lookups += rows.len() as u64;
                report.gateway_mismatches +=
                    batch_mismatches(&plan.primary_key, &plan.columns, rows, &response)?;
                save(paths, report)?;
                remaining = rest;
            }
        }

        report.rows += archived.len() as u64;
        report.batches += 1;
        last = Some(entry.batch.last_key);
        save(paths, report)?;
    }
    if let Some(upper) = cp.upper {
        crate::protection::boundary(paths, &job.plan.policy.source, &options.shutdown).await?;
        if reader.read_batch(&job.plan, last, upper).await?.is_some() {
            report.source_mismatches += 1;
        }
    }
    Ok(true)
}
fn stopped(paths: &StatePaths, options: &Options, queue: Option<&str>) -> Result<bool, Error> {
    Ok(options.shutdown.requested()
        || match queue {
            Some(id) => crate::state::scheduler::show(paths, id)?.status == "cancelled",
            None => false,
        })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wide_rows_reduce_batch_size_without_exceeding_key_limit() {
        let columns = vec![super::super::batch::ArchiveColumn {
            name: "payload".into(),
            postgres_type: "text".into(),
            nullable: false,
        }];
        let narrow = vec![vec![Some("value".into())]; 100];
        assert_eq!(gateway_batch_len(&narrow, &columns, 64), 64);
        let wide = vec![vec![Some("\\".repeat(65536))]; 64];
        assert_eq!(gateway_batch_len(&wide, &columns, 64), 1);
        assert_eq!(gateway_batch_len(&narrow[..3], &columns, 64), 3);
    }
    #[test]
    fn batched_gateway_checks_match_keys_and_fail_closed() {
        let columns = vec![
            super::super::batch::ArchiveColumn {
                name: "id".into(),
                postgres_type: "int8".into(),
                nullable: false,
            },
            super::super::batch::ArchiveColumn {
                name: "amount".into(),
                postgres_type: "numeric".into(),
                nullable: true,
            },
        ];
        let rows = vec![
            vec![Some("1".into()), Some("1.2300".into())],
            vec![Some("2".into()), None],
        ];
        let good = json!({"truncated":false,"records":[{"data":{"id":2,"amount":null}},{"data":{"id":1,"amount":"1.2300"}}]});
        assert_eq!(batch_mismatches("id", &columns, &rows, &good).unwrap(), 0);
        for bad in [
            json!({"truncated":true,"records":good["records"]}),
            json!({"truncated":false,"records":[good["records"][0].clone()]}),
            json!({"truncated":false,"records":[good["records"][0].clone(),good["records"][0].clone()]}),
            json!({"truncated":false,"records":[{"data":{"id":3,"amount":null}},{"data":{"id":1,"amount":"1.2300"}}]}),
            json!({"truncated":false,"records":[{"data":{"id":2,"amount":null}},{"data":{"id":1,"amount":1.23}}]}),
            json!({"truncated":false,"records":[{"data":{"id":2}},{"data":{"id":1,"amount":"1.2300"}}]}),
        ] {
            assert!(batch_mismatches("id", &columns, &rows, &bad).unwrap() > 0);
        }
        assert!(batch_mismatches("id", &columns, &rows, &json!({})).is_err());
    }
    #[test]
    fn comparisons_preserve_precision_and_native_text() {
        assert!(equal(
            &native(&Some("t".into()), "bool").unwrap(),
            &json!(true),
            "bool"
        ));
        assert!(!equal(&json!("1.0000001"), &json!("1.0000002"), "numeric"));
        assert!(!equal(
            &json!("2026-01-01 01:00:00.000001"),
            &json!("2026-01-01 01:00:00.000002"),
            "timestamp"
        ));
        assert_ne!(
            native(&None, "text").unwrap(),
            native(&Some("".into()), "text").unwrap()
        );
        assert_eq!(
            native(&Some("9223372036854775807".into()), "int8").unwrap(),
            json!(i64::MAX)
        );
        assert!(native(&Some("NaN".into()), "float8").is_err());
    }
    #[test]
    fn gateway_rejects_unsafe_origins_before_reading_credentials() {
        for url in [
            "http://example.com",
            "https://user:secret@example.com",
            "https://example.com/path",
            "https://example.com/?token=secret",
        ] {
            assert!(
                Gateway::new(&Options {
                    gateway_url: Some(url.into()),
                    dataset_id: Some("dataset".into()),
                    token_env: Some("UNUSED".into()),
                    ..Default::default()
                })
                .is_err()
            );
        }
    }
}
