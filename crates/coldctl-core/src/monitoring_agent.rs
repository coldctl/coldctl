use crate::{monitoring::Monitor, paths::StatePaths};
use anyhow::{Result, ensure};
use rusqlite::{Connection, OpenFlags};
use std::time::Duration;

pub fn sample(paths: &StatePaths, monitor: &Monitor) -> Result<String> {
    crate::fs_security::check_sqlite(&paths.database())?;
    let mut c = Connection::open_with_flags(paths.database(), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    c.busy_timeout(Duration::from_secs(2))?;
    let cursor = monitor.cursor()?;
    let tx = c.transaction()?;
    let schema: i64 = tx.query_row("SELECT max(version) FROM schema_migrations", [], |r| {
        r.get(0)
    })?;
    ensure!(
        (17..=20).contains(&schema),
        "monitoring supports schemas 17 through 20; use a matching agent and run coldctl init after stopping workers"
    );
    let journal_end: i64 = tx.query_row(
        "SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name='operational_events'),0)",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        cursor <= journal_end,
        "source journal regressed; restore matching monitoring state or start a new monitoring sidecar after reviewing pending alerts"
    );
    let first: i64 = tx.query_row(
        "SELECT COALESCE(min(seq),0) FROM operational_events",
        [],
        |r| r.get(0),
    )?;
    let gap = first > cursor + 1;
    let events = tx.prepare("SELECT seq,kind,entity,failed,occurred_at FROM operational_events WHERE seq>?1 ORDER BY seq LIMIT 1000")?.query_map([cursor], |r| Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,bool>(3)?,r.get::<_,i64>(4)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
    let mut snapshots = Vec::new();
    let mut text = format!(
        "# TYPE coldctl_agent_journal_gap gauge\ncoldctl_agent_journal_gap {}\n",
        i32::from(gap)
    );
    for (table, kind) in [
        ("jobs", "job"),
        ("archive_queue", "queue"),
        ("recurring_archives", "recurring"),
    ] {
        let rows = tx
            .prepare(&format!(
                "SELECT status,count(*) FROM {table} GROUP BY status"
            ))?
            .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        text.push_str(&format!("# TYPE coldctl_agent_{kind}_count gauge\n"));
        for status in [
            "running",
            "completed",
            "failed",
            "cancelled",
            "paused",
            "pending",
            "active",
            "stopped",
        ] {
            let count = rows
                .iter()
                .find(|(s, _)| s == status)
                .map_or(0, |(_, n)| *n);
            text.push_str(&format!(
                "coldctl_agent_{kind}_count{{status=\"{status}\"}} {count}\n"
            ));
        }
        if events.len() < 1000 {
            let condition = match kind {
                "job" => "status='failed' OR failure_json IS NOT NULL",
                "queue" => "status='failed' OR last_error IS NOT NULL",
                _ => "status='failed'",
            };
            let ids = tx
                .prepare(&format!("SELECT id FROM {table} WHERE {condition}"))?
                .query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            snapshots.push((kind, ids));
        }
    }
    for (metric, query) in [
        (
            "archived_rows",
            "SELECT COALESCE(sum(rows_processed),0) FROM jobs",
        ),
        (
            "archived_bytes",
            "SELECT COALESCE(sum(bytes_written),0) FROM jobs",
        ),
        (
            "deleted_rows",
            "SELECT COALESCE(sum(rows_deleted),0) FROM archive_deletions",
        ),
    ] {
        let value: i64 = tx.query_row(query, [], |r| r.get(0))?;
        text.push_str(&format!(
            "# TYPE coldctl_agent_{metric} gauge\ncoldctl_agent_{metric} {value}\n"
        ));
    }
    let due: i64 = tx.query_row(
        "SELECT COALESCE(min(due_at),?1) FROM archive_queue WHERE status IN ('pending','running')",
        [crate::monitoring::now()],
        |r| r.get(0),
    )?;
    text.push_str(&format!(
        "# TYPE coldctl_agent_oldest_due_seconds gauge\ncoldctl_agent_oldest_due_seconds {}\n",
        (crate::monitoring::now() - due).max(0)
    ));
    tx.commit()?;
    if paths.data_dir.join("cloud-sync.sqlite").exists() {
        let sync = crate::cloud_sync::Store::open(paths)?.status()?;
        monitor.observe("cloud_sync", "service", sync.last_error.is_some())?;
        text.push_str(&format!("# TYPE coldctl_cloud_sync_last_success_timestamp_seconds gauge\ncoldctl_cloud_sync_last_success_timestamp_seconds {}\n# TYPE coldctl_cloud_sync_consecutive_failures gauge\ncoldctl_cloud_sync_consecutive_failures {}\n", sync.last_success.unwrap_or(0), sync.consecutive_failures));
    }
    // Release the source read transaction before any monitoring DB writes/network work.
    if gap {
        monitor.count("journal_gaps")?;
    }
    for (seq, kind, id, failed, at) in events {
        monitor.ingest(seq, &kind, &id, failed, at)?;
    }
    for (kind, ids) in snapshots {
        monitor.reconcile(kind, &ids)?;
    }
    Ok(text)
}

pub fn lock(file: &std::fs::File) -> Result<()> {
    fs2::FileExt::try_lock_exclusive(file)
        .map_err(|_| anyhow::anyhow!("a monitor is already running for this state directory"))
}
