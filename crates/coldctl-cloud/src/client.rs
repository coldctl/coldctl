use coldctl_core::{paths::StatePaths, state};
use serde::Serialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Snapshot {
    pub schema_version: u8,
    pub agent_id: String,
    pub agent_version: String,
    pub captured_at_unix_ms: u64,
    pub sources: Vec<Source>,
    pub destinations: Vec<Destination>,
    pub policies: Vec<Policy>,
    pub jobs: Vec<Job>,
}
#[derive(Serialize)]
pub struct Source {
    pub id: String,
    pub name: String,
    pub kind: String,
}
#[derive(Serialize)]
pub struct Destination {
    pub id: String,
    pub name: String,
    pub kind: String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Policy {
    pub id: String,
    pub name: String,
    pub source: String,
    pub destination: String,
    pub schema: String,
    pub table: String,
    pub time_column: String,
    pub older_than_days: i32,
    pub batch_size: i32,
    pub has_equality_filter: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
    pub id: String,
    pub policy_name: String,
    pub source: String,
    pub destination: String,
    pub schema: String,
    pub table: String,
    pub status: state::archive::JobStatus,
    pub rows_processed: i64,
    pub bytes_written: i64,
    pub objects_created: i64,
    pub started_at: String,
    pub completed_at: Option<String>,
    pub verified_at: Option<String>,
    pub stage: Option<coldctl_core::archive::progress::Stage>,
    pub observed_at_unix_ms: Option<u64>,
    pub imported: bool,
}
/// Explicit allowlist: never serialize connection configuration, plans, predicates,
/// row values, file paths, keys, raw errors or environment references.
pub fn collect(paths: &StatePaths, version: &str) -> Result<Snapshot, Box<dyn std::error::Error>> {
    let captured_at_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
    let installation = state::status(paths)?.ok_or("Run coldctl init first")?;
    let sources = state::sources::list(paths)?
        .into_iter()
        .map(|s| Source {
            id: s.id,
            name: s.name,
            kind: s.source_type,
        })
        .collect();
    let destinations = state::destinations::list(paths)?
        .into_iter()
        .map(|d| Destination {
            id: d.id,
            name: d.name,
            kind: d.destination_type,
        })
        .collect();
    let policies = state::archive::policy_list(paths)?
        .into_iter()
        .map(|p| Policy {
            id: p.id,
            name: p.name,
            source: p.source,
            destination: p.destination,
            schema: p.config.schema,
            table: p.config.table,
            time_column: p.config.time_column,
            older_than_days: p.config.older_than_days,
            batch_size: p.config.batch_size,
            has_equality_filter: p.config.equals_column.is_some(),
        })
        .collect();
    let jobs = state::archive::job_list(paths)?
        .into_iter()
        .map(|j| Job {
            id: j.id,
            policy_name: j.policy_name,
            source: j.plan.policy.source,
            destination: j.plan.policy.destination,
            schema: j.plan.policy.config.schema,
            table: j.plan.policy.config.table,
            status: j.status,
            rows_processed: j.rows_processed,
            bytes_written: j.bytes_written,
            objects_created: j.objects_created,
            started_at: j.started_at,
            completed_at: j.completed_at,
            verified_at: j.verified_at,
            stage: j.progress.as_ref().map(|p| p.stage),
            observed_at_unix_ms: j.progress.map(|p| p.observed_at_unix_ms),
            imported: j.imported,
        })
        .collect();
    Ok(Snapshot {
        schema_version: 1,
        agent_id: installation.id.to_string(),
        agent_version: version.into(),
        captured_at_unix_ms,
        sources,
        destinations,
        policies,
        jobs,
    })
}

pub fn encode(snapshot: &Snapshot) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec(snapshot)?;
    if bytes.len() > 48 * 1024
        || String::from_utf8_lossy(&bytes).encode_utf16().count() > 30000
        || snapshot.sources.len()
            + snapshot.destinations.len()
            + snapshot.policies.len()
            + snapshot.jobs.len()
            > 200
    {
        return Err("Metadata exceeds the first sync contract (48 KiB / 30,000 UTF-16 units / 200 records); nothing was uploaded".into());
    }
    Ok(bytes)
}
fn endpoint(
    base: &str,
    allow_local_http: bool,
) -> Result<reqwest::Url, Box<dyn std::error::Error>> {
    let mut url = reqwest::Url::parse(base).map_err(|_| "Invalid Cloud URL")?;
    let loopback = matches!(url.host_str(), Some("127.0.0.1") | Some("[::1]"));
    if !(url.scheme() == "https" || (allow_local_http && loopback && url.scheme() == "http"))
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err("Use an HTTPS Cloud origin without credentials, path, query or fragment; explicit --allow-local-http permits numeric loopback only".into());
    }
    url.set_path("/api/agents/sync");
    Ok(url)
}
pub async fn sync(
    base: &str,
    token: &str,
    allow_local_http: bool,
    body: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = endpoint(base, allow_local_http)?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| "Unable to initialize Cloud HTTP client")?;
    let response = client.post(url).bearer_auth(token).header("Content-Type", "application/json").body(body).send().await.map_err(|_| "Cloud sync failed: check network, TLS and Cloud origin; local archive state is unchanged")?;
    match response.status().as_u16() {
        200 => Ok(()),
        401 | 403 => Err("Cloud rejected the token: use an unexpired lifecycle token for your workspace".into()),
        409 => Err("Cloud has a newer report or another sync is active; collect and sync again".into()),
        400 | 413 => Err("Cloud rejected the metadata contract or size; check agent/console versions and limits".into()),
        _ => Err("Cloud sync failed; retry after checking Cloud availability (local archive state is unchanged)".into()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn destination_boundary() {
        assert!(endpoint("https://console.example.com", false).is_ok());
        assert!(endpoint("http://127.0.0.1:5180", true).is_ok());
        for u in [
            "http://console.example.com",
            "https://user:secret@console.example.com",
            "https://console.example.com/?token=x",
            "https://console.example.com/api",
            "http://localhost:5180",
        ] {
            assert!(endpoint(u, true).is_err());
        }
        assert!(endpoint("http://127.0.0.1:5180", false).is_err());
    }
    #[test]
    fn metadata_omits_connection_and_filter_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(dir.path())).unwrap();
        state::initialize(&paths, "test").unwrap();
        state::sources::add(
            &paths,
            "db",
            coldctl_core::source::SourceConnection::UrlEnv {
                variable: "SECRET_URL_ENV".into(),
            },
        )
        .unwrap();
        state::destinations::add_local(&paths, "archive", &dir.path().join("PRIVATE_PATH"))
            .unwrap();
        state::archive::policy_create(
            &paths,
            "policy",
            "db",
            "archive",
            coldctl_core::policy::model::PolicyConfig {
                schema: "public".into(),
                table: "events".into(),
                time_column: "created_at".into(),
                older_than_days: 365,
                batch_size: 20,
                equals_column: Some("status".into()),
                equals_value: Some("PRIVATE_LITERAL".into()),
            },
        )
        .unwrap();
        let report = String::from_utf8(encode(&collect(&paths, "test").unwrap()).unwrap()).unwrap();
        for secret in [
            "SECRET_URL_ENV",
            "PRIVATE_PATH",
            "PRIVATE_LITERAL",
            "password",
            "connection",
            "equalsValue",
        ] {
            assert!(!report.contains(secret));
        }
        assert!(report.contains("hasEqualityFilter"));
    }
}
