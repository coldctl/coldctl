//! Local incident history and at-least-once webhook outbox. No source data or errors.
use anyhow::{Result, ensure};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub fn validate_path(path: &Path) -> Result<()> {
    for component in std::path::absolute(path)?.ancestors() {
        match std::fs::symlink_metadata(component) {
            Ok(meta) => {
                #[cfg(windows)]
                let linked = {
                    use std::os::windows::fs::MetadataExt;
                    meta.file_attributes() & 0x400 != 0
                };
                #[cfg(not(windows))]
                let linked = meta.file_type().is_symlink();
                ensure!(
                    !linked && (meta.is_file() || meta.is_dir()),
                    "monitoring storage must not contain links or special files"
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AlertConfig {
    pub webhook_url_env: Option<String>,
    pub webhook_token_env: Option<String>,
    pub allow_loopback_http: bool,
}
impl AlertConfig {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.webhook_url_env.is_some() || self.webhook_token_env.is_none(),
            "webhook token requires a webhook URL reference"
        );
        for name in self
            .webhook_url_env
            .iter()
            .chain(self.webhook_token_env.iter())
        {
            ensure!(
                !name.is_empty()
                    && name.len() <= 128
                    && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'),
                "invalid webhook environment reference"
            );
        }
        Ok(())
    }
    fn resolve(&self) -> Result<Option<(url::Url, Option<String>)>> {
        self.validate()?;
        let Some(name) = &self.webhook_url_env else {
            return Ok(None);
        };
        let value = std::env::var(name)
            .map_err(|_| anyhow::anyhow!("webhook URL environment variable unavailable"))?;
        let url = url::Url::parse(&value).map_err(|_| anyhow::anyhow!("invalid webhook URL"))?;
        let loopback = matches!(url.host_str(), Some("127.0.0.1" | "[::1]"));
        ensure!(
            url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none()
                && (url.scheme() == "https"
                    || (self.allow_loopback_http && loopback && url.scheme() == "http")),
            "webhook requires HTTPS (or explicitly allowed numeric loopback HTTP)"
        );
        let token = self
            .webhook_token_env
            .as_ref()
            .map(|n| {
                std::env::var(n)
                    .ok()
                    .filter(|v| {
                        !v.is_empty() && v.len() <= 4096 && v.bytes().all(|b| b.is_ascii_graphic())
                    })
                    .ok_or_else(|| {
                        anyhow::anyhow!("webhook token environment variable unavailable")
                    })
            })
            .transpose()?;
        Ok(Some((url, token)))
    }
    pub fn check(&self) -> Result<()> {
        self.resolve().map(|_| ())
    }
}

#[derive(Clone)]
pub struct Monitor {
    path: PathBuf,
    component: String,
    service_id: String,
    pub alerts: AlertConfig,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Event {
    pub id: String,
    pub component: String,
    pub service_id: String,
    pub kind: String,
    pub entity_id: String,
    pub state: String,
    pub occurred_at: i64,
}
impl Monitor {
    pub fn open(path: &Path, component: &str, alerts: AlertConfig) -> Result<Self> {
        alerts.validate()?;
        ensure!(
            matches!(component, "agent" | "gateway"),
            "invalid monitoring component"
        );
        if path.exists() {
            ensure!(
                !path.symlink_metadata()?.file_type().is_symlink(),
                "monitoring database cannot be a symbolic link"
            );
        }
        validate_path(path)?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(path) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let mut store = Self {
            path: path.into(),
            component: component.into(),
            service_id: String::new(),
            alerts,
        };
        let c = store.connect()?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        c.execute_batch("PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS incidents(kind TEXT NOT NULL, entity TEXT NOT NULL, active INTEGER NOT NULL, updated INTEGER NOT NULL, PRIMARY KEY(kind,entity));
            CREATE TABLE IF NOT EXISTS events(id TEXT PRIMARY KEY, kind TEXT NOT NULL, entity TEXT NOT NULL, state TEXT NOT NULL, created INTEGER NOT NULL, delivery TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, due INTEGER NOT NULL, last_status INTEGER);
            CREATE INDEX IF NOT EXISTS events_due ON events(delivery,due);
            CREATE INDEX IF NOT EXISTS events_retention ON events(delivery,created);
            CREATE TABLE IF NOT EXISTS counters(name TEXT PRIMARY KEY, value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS metadata(name TEXT PRIMARY KEY,value TEXT NOT NULL);")?;
        c.execute(
            "INSERT OR IGNORE INTO metadata VALUES('service_id',?1)",
            [uuid::Uuid::new_v4().to_string()],
        )?;
        store.service_id = c.query_row(
            "SELECT value FROM metadata WHERE name='service_id'",
            [],
            |r| r.get(0),
        )?;
        Ok(store)
    }
    fn connect(&self) -> Result<Connection> {
        validate_path(&self.path)?;
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut name = self.path.as_os_str().to_owned();
            name.push(suffix);
            validate_path(Path::new(&name))?;
        }
        let c = Connection::open(&self.path)?;
        c.busy_timeout(Duration::from_secs(2))?;
        Ok(c)
    }
    pub fn observe(&self, kind: &str, entity: &str, failed: bool) -> Result<()> {
        ensure!(
            matches!(
                kind,
                "job"
                    | "queue"
                    | "recurring"
                    | "state_read"
                    | "cloud_sync"
                    | "refresh"
                    | "search"
                    | "record"
                    | "download"
                    | "export"
                    | "ready"
                    | "api"
            ),
            "invalid incident kind"
        );
        ensure!(
            entity == "service" || uuid::Uuid::parse_str(entity).is_ok(),
            "incident entity must be an internal UUID"
        );
        let mut c = self.connect()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let previous: Option<bool> = tx
            .query_row(
                "SELECT active FROM incidents WHERE kind=?1 AND entity=?2",
                (kind, entity),
                |r| r.get(0),
            )
            .optional()?;
        if previous == Some(failed) || (previous.is_none() && !failed) {
            return Ok(());
        }
        let at = now();
        tx.execute("INSERT INTO incidents VALUES(?1,?2,?3,?4) ON CONFLICT(kind,entity) DO UPDATE SET active=excluded.active,updated=excluded.updated", (kind, entity, failed, at))?;
        tx.execute("INSERT INTO events(id,kind,entity,state,created,delivery,due) VALUES(?1,?2,?3,?4,?5,?6,?5)", (uuid::Uuid::new_v4().to_string(), kind, entity, if failed { "firing" } else { "resolved" }, at, if self.alerts.webhook_url_env.is_some() { "pending" } else { "disabled" }))?;
        tx.execute("INSERT INTO counters VALUES('transitions',1) ON CONFLICT(name) DO UPDATE SET value=value+1", [])?;
        tx.commit()?;
        Ok(())
    }
    pub fn cursor(&self) -> Result<i64> {
        Ok(self
            .connect()?
            .query_row(
                "SELECT value FROM counters WHERE name='source_cursor'",
                [],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0))
    }
    pub fn ingest(
        &self,
        sequence: i64,
        kind: &str,
        entity: &str,
        failed: bool,
        at: i64,
    ) -> Result<()> {
        ensure!(
            matches!(kind, "job" | "queue" | "recurring") && uuid::Uuid::parse_str(entity).is_ok(),
            "invalid source event"
        );
        let mut c = self.connect()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cursor: i64 = tx
            .query_row(
                "SELECT value FROM counters WHERE name='source_cursor'",
                [],
                |r| r.get(0),
            )
            .optional()?
            .unwrap_or(0);
        if sequence <= cursor {
            return Ok(());
        }
        tx.execute("INSERT INTO incidents VALUES(?1,?2,?3,?4) ON CONFLICT(kind,entity) DO UPDATE SET active=excluded.active,updated=excluded.updated", (kind,entity,failed,at))?;
        tx.execute("INSERT INTO events(id,kind,entity,state,created,delivery,due) VALUES(?1,?2,?3,?4,?5,?6,?5)", (uuid::Uuid::new_v4().to_string(),kind,entity,if failed {"firing"} else {"resolved"},at,if self.alerts.webhook_url_env.is_some() {"pending"} else {"disabled"}))?;
        tx.execute("INSERT INTO counters VALUES('source_cursor',?1) ON CONFLICT(name) DO UPDATE SET value=excluded.value",[sequence])?;
        tx.execute("INSERT INTO counters VALUES('transitions',1) ON CONFLICT(name) DO UPDATE SET value=value+1",[])?;
        tx.commit()?;
        Ok(())
    }
    /// Snapshot synchronization preserves resolved history instead of deleting failures.
    pub fn reconcile(&self, kind: &str, failed: &[String]) -> Result<()> {
        let c = self.connect()?;
        let existing = c
            .prepare("SELECT entity FROM incidents WHERE kind=?1 AND active=1")?
            .query_map([kind], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        for id in failed {
            self.observe(kind, id, true)?;
        }
        for id in existing {
            if !failed.contains(&id) {
                self.observe(kind, &id, false)?;
            }
        }
        Ok(())
    }
    pub fn count(&self, name: &str) -> Result<()> {
        ensure!(
            matches!(
                name,
                "http_success" | "http_client_error" | "http_server_error" | "journal_gaps"
            ),
            "invalid counter"
        );
        self.connect()?.execute(
            "INSERT INTO counters VALUES(?1,1) ON CONFLICT(name) DO UPDATE SET value=value+1",
            [name],
        )?;
        Ok(())
    }
    pub fn history(&self) -> Result<serde_json::Value> {
        let c = self.connect()?;
        let rows = c.prepare("SELECT id,kind,entity,state,created,delivery,attempts,due,last_status FROM events ORDER BY rowid DESC LIMIT 100")?.query_map([], |r| Ok(serde_json::json!({"id":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"entity_id":r.get::<_,String>(2)?,"state":r.get::<_,String>(3)?,"occurred_at":r.get::<_,i64>(4)?,"delivery":r.get::<_,String>(5)?,"attempts":r.get::<_,i64>(6)?,"next_attempt_at":r.get::<_,i64>(7)?,"last_http_status":r.get::<_,Option<i64>>(8)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(
            serde_json::json!({"component":self.component,"service_id":self.service_id,"events":rows,"limit":100}),
        )
    }
    pub fn metrics(&self) -> Result<String> {
        let c = self.connect()?;
        let active: i64 =
            c.query_row("SELECT count(*) FROM incidents WHERE active=1", [], |r| {
                r.get(0)
            })?;
        let pending: i64 = c.query_row(
            "SELECT count(*) FROM events WHERE delivery='pending'",
            [],
            |r| r.get(0),
        )?;
        let oldest: i64 = c.query_row(
            "SELECT COALESCE(min(created),?1) FROM events WHERE delivery='pending'",
            [now()],
            |r| r.get(0),
        )?;
        let mut output = format!(
            "# TYPE coldctl_monitor_active_failures gauge\ncoldctl_monitor_active_failures {active}\n# TYPE coldctl_monitor_alerts_pending gauge\ncoldctl_monitor_alerts_pending {pending}\n# TYPE coldctl_monitor_oldest_pending_seconds gauge\ncoldctl_monitor_oldest_pending_seconds {}\n",
            (now() - oldest).max(0)
        );
        for row in c
            .prepare("SELECT name,value FROM counters ORDER BY name")?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
        {
            let (name, value) = row?;
            // Counter names are internal, never derived from URLs or user input.
            output.push_str(&format!("# TYPE coldctl_monitor_{name}_total counter\ncoldctl_monitor_{name}_total {value}\n"));
        }
        Ok(output)
    }
    pub fn prune(&self) -> Result<()> {
        let c = self.connect()?;
        let cutoff = now() - 30 * 86400;
        c.execute(
            "DELETE FROM events WHERE delivery!='pending' AND created<?1",
            [cutoff],
        )?;
        c.execute(
            "DELETE FROM incidents WHERE active=0 AND updated<?1 AND NOT EXISTS(SELECT 1 FROM events WHERE events.kind=incidents.kind AND events.entity=incidents.entity AND delivery='pending')",
            [cutoff],
        )?;
        Ok(())
    }
    fn claim(&self) -> Result<Option<Event>> {
        if self.alerts.webhook_url_env.is_none() {
            return Ok(None);
        }
        let mut c = self.connect()?;
        let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let event = tx.query_row("SELECT e.id,e.kind,e.entity,e.state,e.created FROM events e WHERE e.delivery='pending' AND e.due<=?1 AND NOT EXISTS(SELECT 1 FROM events prior WHERE prior.delivery='pending' AND prior.kind=e.kind AND prior.entity=e.entity AND prior.rowid<e.rowid) ORDER BY e.created,e.rowid LIMIT 1", [now()], |r| Ok(Event { id:r.get(0)?, component:self.component.clone(), service_id:self.service_id.clone(), kind:r.get(1)?, entity_id:r.get(2)?, state:r.get(3)?, occurred_at:r.get(4)? })).optional()?;
        if let Some(e) = &event {
            tx.execute(
                "UPDATE events SET due=?1,attempts=attempts+1 WHERE id=?2",
                (now() + 60, &e.id),
            )?;
        }
        tx.commit()?;
        Ok(event)
    }
    pub async fn deliver_one(&self) -> Result<()> {
        self.deliver_to(self.alerts.resolve().ok().flatten()).await
    }
    async fn deliver_to(&self, target: Option<(url::Url, Option<String>)>) -> Result<()> {
        let Some(event) = self.claim()? else {
            return Ok(());
        };
        // Never propagate transport errors, response bodies or secret URLs to logs/state.
        let status = async {
            let (url, token) = target?;
            let client = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .connect_timeout(Duration::from_secs(3))
                .build()
                .ok()?;
            let mut request = client
                .post(url)
                .header("Idempotency-Key", &event.id)
                .json(&event);
            if let Some(token) = token {
                request = request.bearer_auth(token);
            }
            request.send().await.ok().map(|r| r.status().as_u16())
        }
        .await;
        let c = self.connect()?;
        if status.is_some_and(|s| (200..300).contains(&s)) {
            c.execute(
                "UPDATE events SET delivery='sent',last_status=?1 WHERE id=?2",
                (status, &event.id),
            )?;
        } else {
            let attempts: i64 = c.query_row(
                "SELECT attempts FROM events WHERE id=?1",
                [&event.id],
                |r| r.get(0),
            )?;
            let delay = (30 * (1_i64 << attempts.saturating_sub(1).min(7))).min(3600);
            c.execute(
                "UPDATE events SET due=?1,last_status=?2 WHERE id=?3",
                (now() + delay, status, &event.id),
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transitions_survive_restart_deduplicate_and_resolve() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("monitor.sqlite");
        let m = Monitor::open(&path, "agent", AlertConfig::default()).unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        m.reconcile("job", std::slice::from_ref(&id)).unwrap();
        m.reconcile("job", std::slice::from_ref(&id)).unwrap();
        let m = Monitor::open(&path, "agent", AlertConfig::default()).unwrap();
        assert_eq!(m.history().unwrap()["events"].as_array().unwrap().len(), 1);
        m.reconcile("job", &[]).unwrap();
        assert_eq!(m.history().unwrap()["events"][0]["state"], "resolved");
        assert!(
            m.metrics()
                .unwrap()
                .contains("coldctl_monitor_active_failures 0")
        );
        assert!(m.observe("job", "secret database name", true).is_err());
    }
    #[test]
    fn outbox_claim_is_leased_and_disabled_hooks_do_not_replay_history() {
        let dir = tempfile::tempdir().unwrap();
        let m = Monitor::open(
            &dir.path().join("monitor.sqlite"),
            "gateway",
            AlertConfig {
                webhook_url_env: Some("UNSET_MONITOR_TEST_URL".into()),
                ..Default::default()
            },
        )
        .unwrap();
        m.observe("refresh", "service", true).unwrap();
        let first = m.claim().unwrap().unwrap();
        assert!(m.claim().unwrap().is_none());
        m.connect()
            .unwrap()
            .execute("UPDATE events SET due=0", [])
            .unwrap();
        assert_eq!(m.claim().unwrap().unwrap().id, first.id);
    }
}

#[cfg(test)]
mod delivery_tests {
    use super::*;
    #[tokio::test]
    async fn webhook_retries_keeps_id_and_does_not_follow_redirects() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url =
            url::Url::parse(&format!("http://{}/hook", listener.local_addr().unwrap())).unwrap();
        let receiver = std::thread::spawn(move || {
            let mut ids = Vec::new();
            for status in [503, 302, 204] {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(15)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buf = [0; 4096];
                loop {
                    let n = socket.read(&mut buf).unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&buf[..n]);
                    let raw = String::from_utf8_lossy(&request);
                    if let Some((headers, body)) = raw.split_once("\r\n\r\n") {
                        let length = headers
                            .lines()
                            .find_map(|l| {
                                l.to_lowercase()
                                    .strip_prefix("content-length: ")
                                    .and_then(|n| n.parse::<usize>().ok())
                            })
                            .unwrap();
                        if body.len() >= length {
                            assert!(
                                headers
                                    .to_lowercase()
                                    .contains("authorization: bearer fixture-secret")
                            );
                            let payload: serde_json::Value = serde_json::from_str(body).unwrap();
                            assert_eq!(payload["state"], "firing");
                            assert!(!body.contains("fixture-secret"));
                            ids.push(payload["id"].clone());
                            break;
                        }
                    }
                }
                write!(socket,"HTTP/1.1 {status} Fixture\r\nContent-Length: 0\r\nConnection: close\r\nLocation: http://127.0.0.1:1/never\r\n\r\n").unwrap();
            }
            assert!(ids.iter().all(|id| id == &ids[0]));
        });
        let temp = tempfile::tempdir().unwrap();
        let m = Monitor::open(
            &temp.path().join("monitor.sqlite"),
            "agent",
            AlertConfig {
                webhook_url_env: Some("FIXTURE_URL".into()),
                ..Default::default()
            },
        )
        .unwrap();
        m.observe("refresh", "service", true).unwrap();
        m.observe("refresh", "service", false).unwrap();
        for expected in [503, 302, 204] {
            m.connect()
                .unwrap()
                .execute("UPDATE events SET due=0 WHERE state='firing'", [])
                .unwrap();
            m.deliver_to(Some((url.clone(), Some("fixture-secret".into()))))
                .await
                .unwrap();
            let history = m.history().unwrap();
            let firing = history["events"]
                .as_array()
                .unwrap()
                .iter()
                .find(|e| e["state"] == "firing")
                .unwrap();
            assert_eq!(firing["last_http_status"], expected);
            if expected != 204 {
                assert!(firing["next_attempt_at"].as_i64().unwrap() > now());
                assert!(m.claim().unwrap().is_none());
            }
        }
        receiver.join().unwrap();
        assert_eq!(m.claim().unwrap().unwrap().state, "resolved");
        assert!(!m.history().unwrap().to_string().contains("fixture-secret"));
    }
    #[test]
    fn source_ingestion_is_atomic_and_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("monitor.sqlite");
        let id = uuid::Uuid::new_v4().to_string();
        let m = Monitor::open(&path, "agent", Default::default()).unwrap();
        m.ingest(1, "job", &id, true, now()).unwrap();
        m.ingest(2, "job", &id, false, now()).unwrap();
        let m = Monitor::open(&path, "agent", Default::default()).unwrap();
        m.ingest(1, "job", &id, true, now()).unwrap();
        assert_eq!(m.cursor().unwrap(), 2);
        assert_eq!(m.history().unwrap()["events"].as_array().unwrap().len(), 2);
        assert!(
            m.metrics()
                .unwrap()
                .contains("coldctl_monitor_active_failures 0")
        );
    }
}
