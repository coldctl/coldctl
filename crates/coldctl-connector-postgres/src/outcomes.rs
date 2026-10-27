//! Self-hosted PostgreSQL maintenance. Never cancel another application's backend.
use crate::{
    postgres::{PostgresSource, Session, postgres_error},
    postgres_archive::{inspect, quote_identifier as quote, validate_signature},
};
use coldctl_core::{
    archive::outcomes::{ReclaimOptions, ReclaimResult, Snapshot},
    error::Error,
    source::{
        config::{ResolvedConnection, TlsMode},
        connector::DeleteSpec,
    },
};
use std::{
    io::Write,
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
fn pg(e: tokio_postgres::Error) -> Error {
    postgres_error("measure or reclaim PostgreSQL space", e)
}
pub(crate) async fn lock(client: &tokio_postgres::Client, oid: u32) -> Result<(), Error> {
    // Session lock, held across batches / external pg_repack and released on disconnect.
    let held: bool = client
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_lock(1129270340,$1)",
            &[&(oid as i32)],
        )
        .await
        .map_err(pg)?
        .get(0);
    if !held {
        return Err(Error::Archive(
            "table deletion or reclamation is already active",
        ));
    }
    Ok(())
}
async fn session(connection: ResolvedConnection, spec: &DeleteSpec) -> Result<Session, Error> {
    let s = PostgresSource::new(connection).connect_restore().await?;
    if s.identity != spec.source_identity {
        return Err(Error::Archive("original source identity changed"));
    }
    validate(&s, spec).await?;
    Ok(s)
}
async fn validate(s: &Session, spec: &DeleteSpec) -> Result<(), Error> {
    let p = &spec.plan;
    let safety = p
        .safety
        .as_ref()
        .ok_or(Error::Archive("missing original safety contract"))?;
    let r = s
        .client
        .query_one(
            "SELECT oid,datname::text FROM pg_catalog.pg_database WHERE datname=current_database()",
            &[],
        )
        .await
        .map_err(pg)?;
    if r.get::<_, u32>(0) != safety.source_identity.database_oid
        || r.get::<_, String>(1) != safety.source_identity.database_name
    {
        return Err(Error::Archive("source database changed"));
    }
    let (oid, key, columns) = inspect(&s.client, &p.policy).await?;
    if oid != p.table_oid || key != p.primary_key || columns != p.columns {
        return Err(Error::Archive("source table changed"));
    }
    validate_signature(&s.client, p).await
}
async fn snapshot(s: &Session, oid: u32) -> Result<Snapshot, Error> {
    let row=s.client.query_one("SELECT pg_catalog.pg_table_size($1::oid),pg_catalog.pg_indexes_size($1::oid),pg_catalog.pg_relation_filenode($1::oid)",&[&oid]).await.map_err(pg)?;
    let mut result = Snapshot {
        observed_at_unix_ms: now(),
        table_bytes: row.get::<_, i64>(0) as u64,
        index_bytes: row.get::<_, i64>(1) as u64,
        heap_free_bytes: None,
        physical_allocated_bytes: None,
        filenode: row.get(2),
    };
    // Optional exact heap measurement. A missing extension/privilege/timeout is unknown,
    // never a dead-tuple estimate masquerading as free disk.
    let ns=s.client.query_opt("SELECT n.nspname::text FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace WHERE e.extname='pgstattuple'",&[]).await.map_err(pg)?;
    if let Some(ns) = ns {
        let sql = format!(
            "SELECT free_space FROM {}.pgstattuple($1::oid::regclass)",
            quote(&ns.get::<_, String>(0))
        );
        if let Ok(r) = s.client.query_one(&sql, &[&oid]).await {
            result.heap_free_bytes = Some(r.get::<_, i64>(0) as u64);
        }
    }
    Ok(result)
}
pub async fn measure(connection: ResolvedConnection, spec: DeleteSpec) -> Result<Snapshot, Error> {
    let s = session(connection, &spec).await?;
    snapshot(&s, spec.plan.table_oid).await
}
fn required_space(table: u64, index: u64) -> Result<u64, Error> {
    table
        .checked_add(index)
        .and_then(|v| v.checked_mul(2))
        .ok_or(Error::Archive("relation sizes overflow disk gate"))
}
fn space_gate(free: u64, required: u64) -> Result<(), Error> {
    if free < required {
        Err(Error::Archive(
            "insufficient server disk space for pg_repack",
        ))
    } else {
        Ok(())
    }
}
/// Prove local paths reference the server's filesystems before using local statvfs.
/// Requires co-location/shared paths and pg_read_server_files (normally superuser).
/// Each table/index/TOAST/default tablespace must independently have 2x total bytes.
async fn free_space(s: &Session, oid: u32, required: u64) -> Result<(u64, Option<u64>), Error> {
    let rows=s.client.query("WITH rels AS (SELECT $1::oid AS oid UNION SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid=$1 UNION SELECT reltoastrelid FROM pg_catalog.pg_class WHERE oid=$1 AND reltoastrelid<>0 UNION SELECT indexrelid FROM pg_catalog.pg_index WHERE indrelid=(SELECT reltoastrelid FROM pg_catalog.pg_class WHERE oid=$1)) SELECT current_setting('data_directory') AS root, pg_catalog.pg_relation_filepath(oid) AS path,true AS relation FROM rels UNION SELECT current_setting('data_directory'), CASE WHEN t.spcname='pg_default' THEN 'base/'||d.oid::text ELSE pg_catalog.pg_tablespace_location(t.oid) END,false FROM pg_catalog.pg_database d JOIN pg_catalog.pg_tablespace t ON t.oid=d.dattablespace WHERE d.datname=current_database()",&[&oid]).await.map_err(pg)?;
    let mut minimum = u64::MAX;
    let mut allocated = Some(0u64);
    for r in rows {
        let path = std::path::PathBuf::from(r.get::<_, String>(0)).join(r.get::<_, String>(1));
        let dir = if path.is_dir() {
            path.clone()
        } else {
            path.parent()
                .ok_or(Error::Archive("missing server filesystem"))?
                .to_path_buf()
        };
        let mut proof = tempfile::Builder::new()
            .prefix(".coldctl-space-proof-")
            .tempfile_in(&dir)
            .map_err(|_| {
                Error::Archive(
                    "disk gate requires access to the PostgreSQL server's actual filesystems",
                )
            })?;
        let nonce = uuid::Uuid::new_v4().to_string();
        proof
            .write_all(nonce.as_bytes())
            .map_err(|_| Error::Archive("cannot write filesystem proof"))?;
        proof
            .flush()
            .map_err(|_| Error::Archive("cannot flush filesystem proof"))?;
        let proof_path = proof
            .path()
            .to_str()
            .ok_or(Error::Archive("invalid filesystem proof path"))?;
        let read: String = s
            .client
            .query_one("SELECT pg_catalog.pg_read_file($1,0,36)", &[&proof_path])
            .await
            .map_err(pg)?
            .get(0);
        if read != nonce {
            return Err(Error::Archive(
                "local filesystem does not match PostgreSQL server",
            ));
        }
        let free = fs2::available_space(&dir)
            .map_err(|_| Error::Archive("cannot measure server free disk"))?;
        space_gate(free, required)?;
        minimum = minimum.min(free);
        if r.get::<_, bool>(2) {
            allocated =
                allocated.and_then(|n| physical_bytes(&path).and_then(|b| n.checked_add(b)));
        }
    }
    if minimum == u64::MAX {
        return Err(Error::Archive("no server filesystem measured"));
    }
    Ok((minimum, allocated))
}
// Actual allocated filesystem blocks, not apparent file length. Include all forks
// and segments of this relation only. Proof above binds the path to the server.
#[cfg(unix)]
fn physical_bytes(path: &std::path::Path) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    let name = path.file_name()?.to_str()?;
    let mut total = 0u64;
    for e in std::fs::read_dir(path.parent()?).ok()? {
        let e = e.ok()?;
        let filename = e.file_name();
        let filename = filename.to_str()?;
        let Some(suffix) = filename.strip_prefix(name) else {
            continue;
        };
        let suffix = suffix
            .strip_prefix("_fsm")
            .or_else(|| suffix.strip_prefix("_vm"))
            .or_else(|| suffix.strip_prefix("_init"))
            .unwrap_or(suffix);
        if !suffix.is_empty()
            && !suffix
                .strip_prefix('.')
                .is_some_and(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
        {
            continue;
        }
        let m = e.metadata().ok()?;
        if !m.is_file() {
            return None;
        }
        total = total.checked_add(m.blocks().checked_mul(512)?)?;
    }
    Some(total)
}
#[cfg(not(unix))]
fn physical_bytes(_path: &std::path::Path) -> Option<u64> {
    None
}
fn command(connection: &ResolvedConnection, table: &str, wait: u32) -> tokio::process::Command {
    let mut c = tokio::process::Command::new("pg_repack");
    c.env_clear();
    for name in ["PATH", "SystemRoot", "WINDIR"] {
        if let Some(v) = std::env::var_os(name) {
            c.env(name, v);
        }
    }
    c.args([
        "--no-password",
        "--no-superuser-check",
        "--no-order",
        "--no-analyze",
        "--jobs=1",
        "--no-kill-backend",
    ])
    .arg(format!("--wait-timeout={wait}"))
    .arg(format!("--table={table}"))
    .env("PGHOST", &connection.host)
    .env("PGPORT", connection.port.to_string())
    .env("PGDATABASE", &connection.database)
    .env("PGUSER", &connection.user)
    .env("PGPASSWORD", connection.password.as_deref().unwrap_or(""))
    .env("PGCONNECT_TIMEOUT", "10")
    .env("PGAPPNAME", "coldctl-reclaim")
    .env(
        "PGSSLMODE",
        if connection.tls == TlsMode::Disable {
            "disable"
        } else {
            "verify-full"
        },
    )
    .stdin(Stdio::null())
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .kill_on_drop(true);
    c
}
/// Explicitly kill and reap our own helper before releasing the maintenance lock.
async fn run_bounded(
    mut command: tokio::process::Command,
    deadline: tokio::time::Instant,
) -> Result<Option<std::process::ExitStatus>, Error> {
    let mut child = command
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| Error::Archive("cannot start reclaim helper"))?;
    match tokio::time::timeout_at(deadline, child.wait()).await {
        Ok(status) => {
            Ok(Some(status.map_err(|_| {
                Error::Archive("cannot wait for reclaim helper")
            })?))
        }
        Err(_) => {
            child
                .kill()
                .await
                .map_err(|_| Error::Archive("cannot stop reclaim helper"))?;
            child
                .wait()
                .await
                .map_err(|_| Error::Archive("cannot reap reclaim helper"))?;
            Ok(None)
        }
    }
}
pub async fn reclaim(
    connection: ResolvedConnection,
    spec: DeleteSpec,
    options: ReclaimOptions,
) -> Result<ReclaimResult, Error> {
    let remaining = options.remaining_ms(now())?;
    // Includes preflight, disk checks and cleanup, not just the subprocess runtime.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(remaining);
    let s = session(connection.clone(), &spec).await?;
    lock(&s.client, spec.plan.table_oid).await?;
    let owner:bool=s.client.query_one("SELECT pg_catalog.pg_has_role(c.relowner,'USAGE') OR (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname=current_user) FROM pg_catalog.pg_class c WHERE c.oid=$1",&[&spec.plan.table_oid]).await.map_err(pg)?.get(0);
    if !owner {
        return Err(Error::Archive(
            "reclaim requires table owner or superuser; VACUUM must not silently skip the table",
        ));
    }

    let before = snapshot(&s, spec.plan.table_oid).await?;
    let mut r = ReclaimResult {
        failure: None,
        method: "none".into(),
        status: "refused".into(),
        before,
        after: None,
        required_free_bytes: None,
        minimum_free_bytes: None,
    };
    let result=tokio::time::timeout_at(deadline,async {
        let ext=s.client.query_opt("SELECT 1 FROM pg_catalog.pg_extension WHERE extname='pg_repack'",&[]).await.map_err(pg)?.is_some();
        let table=format!("{}.{}",quote(&spec.plan.policy.config.schema),quote(&spec.plan.policy.config.table));
        let mut command=command(&connection,&table,options.wait_timeout_seconds);
        // Keep certificate lifetime through subprocess exit; never downgrade TLS.
        let mut ca=None;
        if connection.tls!=TlsMode::Disable {
            if let Some(pem)=&connection.ca_pem {
                let mut file=tempfile::NamedTempFile::new().map_err(|_|Error::Archive("cannot stage reclaim CA"))?;
                file.write_all(pem.as_bytes()).map_err(|_|Error::Archive("cannot stage reclaim CA"))?;
                command.env("PGSSLROOTCERT",file.path()); ca=Some(file);
            } else { command.env("PGSSLROOTCERT","system"); }
        }
        // Probe binary availability without invoking a mutation. Never use a shell.
        let binary=if ext {
            match tokio::process::Command::new("pg_repack").arg("--version").stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null()).kill_on_drop(true).status().await {
                Ok(s)=>{if !s.success(){return Err(Error::Archive("pg_repack version probe failed"));} true},
                Err(e) if e.kind()==std::io::ErrorKind::NotFound=>false,
                Err(_)=>return Err(Error::Archive("cannot execute pg_repack")),
            }
        } else {false};
        if binary {
            r.method="pg_repack".into();
            let required=required_space(r.before.table_bytes,r.before.index_bytes)?;
            r.required_free_bytes=Some(required);
            let (free,allocated)=free_space(&s,spec.plan.table_oid,required).await?;
            r.minimum_free_bytes=Some(free); r.before.physical_allocated_bytes=allocated;
            options.remaining_ms(now())?;
            validate(&s,&spec).await?;
            // Bound our helper's server queries too, and have PostgreSQL notice a
            // dead client during long work. These affect only coldctl connections.
            let remaining=deadline.saturating_duration_since(tokio::time::Instant::now()).as_millis().min(i32::MAX as u128);
            command.env("PGOPTIONS",format!("-c statement_timeout={remaining} -c idle_in_transaction_session_timeout={remaining} -c client_connection_check_interval=1000"));
            let Some(status)=run_bounded(command,deadline-Duration::from_millis(500)).await? else {
                r.status="window_expired_inspect_required".into(); r.failure=Some("maintenance deadline expired; our helper was stopped".into()); return Ok(());
            };
            drop(ca);
            r.status=if status.success(){"gave_up"}else{"failed_inspect_required"}.into();
            r.after=Some(snapshot(&s,spec.plan.table_oid).await?);
            if let Ok((_,allocated))=free_space(&s,spec.plan.table_oid,0).await {r.after.as_mut().unwrap().physical_allocated_bytes=allocated;}
            if status.success() && r.after.as_ref().is_some_and(|a|a.filenode!=r.before.filenode) {r.status="completed".into();}
        } else {
            r.method="vacuum".into();
            options.remaining_ms(now())?;
            let timeout=deadline.saturating_duration_since(tokio::time::Instant::now()).as_millis().min(i32::MAX as u128).to_string();
            s.client.query_one("SELECT set_config('statement_timeout',$1,false),set_config('lock_timeout',$2,false)",&[&timeout,&format!("{}s",options.wait_timeout_seconds)]).await.map_err(pg)?;
            // No table-tail truncation/exclusive lock; reusable space only.
            s.client.batch_execute(&format!("VACUUM (TRUNCATE FALSE) {table}")).await.map_err(pg)?;
            r.status="completed_reusable_only".into();
            r.after=Some(snapshot(&s,spec.plan.table_oid).await?);

        }
        Ok::<_,Error>(())
    }).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            r.failure=Some(match error {Error::Archive(reason)=>reason.to_owned(),_=>"PostgreSQL maintenance failed; inspect permissions, source state and logs locally".into()});
            r.status = if r.method == "pg_repack" && r.minimum_free_bytes.is_some() {
                "failed_inspect_required"
            } else {
                "refused"
            }
            .into();
        }
        Err(_) => {
            r.status = "window_expired_inspect_required".into();
            r.failure = Some("maintenance deadline expired; inspect source before retrying".into());
        }
    }
    Ok(r)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "subprocess helper only"]
    fn deadline_helper() {
        if std::env::var_os("COLDCTL_RECLAIM_HELPER").is_none() {
            return;
        }
        std::thread::sleep(Duration::from_secs(30));
    }
    #[tokio::test]
    async fn deadline_kills_and_reaps_only_our_helper() {
        let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "outcomes::tests::deadline_helper", "--ignored"])
            .env("COLDCTL_RECLAIM_HELPER", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let start = tokio::time::Instant::now();
        assert!(
            run_bounded(command, start + Duration::from_millis(200))
                .await
                .unwrap()
                .is_none()
        );
        assert!(start.elapsed() < Duration::from_secs(5));
    }
    #[test]
    fn insufficient_space_and_overflow_refused() {
        assert_eq!(required_space(100, 50).unwrap(), 300);
        assert!(space_gate(299, 300).is_err());
        assert!(space_gate(300, 300).is_ok());
        assert!(required_space(u64::MAX, 1).is_err());
    }
    #[test]
    fn repack_never_kills_competing_backends_or_exposes_password() {
        let c = ResolvedConnection {
            host: "localhost".into(),
            port: 5432,
            database: "test".into(),
            user: "test".into(),
            tls: TlsMode::Disable,
            password: Some("secret-value".into()),
            ca_pem: None,
        };
        let cmd = command(&c, "\"public\".\"events\"", 2);
        let args: Vec<_> = cmd
            .as_std()
            .get_args()
            .map(|x| x.to_string_lossy().into_owned())
            .collect();
        assert!(args.contains(&"--wait-timeout=2".into()));
        assert!(args.contains(&"--no-kill-backend".into()));
        assert!(!args.join(" ").contains("secret-value"));
    }
}
