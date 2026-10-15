//! Local bookkeeping only: never persist tokens or metadata payloads.
use crate::{
    monitoring::{now, validate_path},
    paths::StatePaths,
};
use anyhow::Result;
use rusqlite::Connection;
use serde::Serialize;
use std::{fs::File, time::Duration};

#[derive(Serialize)]
pub struct Status {
    pub cloud_origin: String,
    pub last_attempt: Option<i64>,
    pub last_success: Option<i64>,
    pub consecutive_failures: i64,
    pub last_error: Option<String>,
    pub next_attempt: Option<i64>,
}
pub struct Store(Connection);
impl Store {
    pub fn open(paths: &StatePaths) -> Result<Self> {
        let path = paths.data_dir.join("cloud-sync.sqlite");
        crate::fs_security::sqlite(&path)?;
        let c = Connection::open(path)?;
        c.busy_timeout(Duration::from_secs(2))?;
        c.execute_batch("CREATE TABLE IF NOT EXISTS sync_status(id INTEGER PRIMARY KEY CHECK(id=1), origin TEXT NOT NULL, attempted INTEGER, succeeded INTEGER, failures INTEGER NOT NULL DEFAULT 0, error TEXT, due INTEGER);")?;
        Ok(Self(c))
    }
    pub fn configure(&self, origin: &str) -> Result<()> {
        self.0.execute("INSERT INTO sync_status(id,origin) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET origin=excluded.origin,attempted=NULL,succeeded=NULL,failures=0,error=NULL,due=NULL WHERE origin != excluded.origin",[origin])?;
        Ok(())
    }
    pub fn status(&self) -> Result<Status> {
        Ok(self.0.query_row(
            "SELECT origin,attempted,succeeded,failures,error,due FROM sync_status WHERE id=1",
            [],
            |r| {
                Ok(Status {
                    cloud_origin: r.get(0)?,
                    last_attempt: r.get(1)?,
                    last_success: r.get(2)?,
                    consecutive_failures: r.get(3)?,
                    last_error: r.get(4)?,
                    next_attempt: r.get(5)?,
                })
            },
        )?)
    }
    pub fn begin(&self) -> Result<()> {
        self.0.execute(
            "UPDATE sync_status SET attempted=?1,due=?1+30 WHERE id=1",
            [now()],
        )?;
        Ok(())
    }
    pub fn finish(&self, error: Option<&str>, interval: u64, watch: bool) -> Result<u64> {
        let failures = if error.is_some() {
            self.status()?.consecutive_failures.saturating_add(1)
        } else {
            0
        };
        let delay = if failures == 0 {
            interval
        } else {
            (30_u64 * (1 << (failures - 1).min(7))).min(3600)
        };
        self.0.execute("UPDATE sync_status SET succeeded=CASE WHEN ?1 IS NULL THEN ?2 ELSE succeeded END, failures=?3,error=?1,due=?4 WHERE id=1",rusqlite::params![error,now(),failures,watch.then_some(now()+delay as i64)])?;
        Ok(delay)
    }
    pub fn stopped(&self) -> Result<()> {
        self.0
            .execute("UPDATE sync_status SET due=NULL WHERE id=1", [])?;
        Ok(())
    }
}
pub fn lock(paths: &StatePaths) -> Result<File> {
    let path = paths.data_dir.join("cloud-sync.lock");
    validate_path(&path)?;
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    fs2::FileExt::try_lock_exclusive(&file)
        .map_err(|_| anyhow::anyhow!("Cloud sync is already running for this state directory"))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_state_survives_restart_and_success_recovers() {
        let dir = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(dir.path())).unwrap();
        let s = Store::open(&paths).unwrap();
        s.configure("https://example.com/").unwrap();
        let guard = lock(&paths).unwrap();
        assert!(lock(&paths).is_err());
        s.begin().unwrap();
        assert_eq!(s.finish(Some("network"), 60, true).unwrap(), 30);
        drop(s);
        let s = Store::open(&paths).unwrap();
        s.configure("https://example.com/").unwrap();
        assert_eq!(s.status().unwrap().consecutive_failures, 1);
        assert_eq!(s.finish(Some("network"), 60, true).unwrap(), 60);
        assert_eq!(s.finish(None, 300, true).unwrap(), 300);
        assert!(s.status().unwrap().last_success.is_some());
        assert!(s.status().unwrap().last_error.is_none());
        s.stopped().unwrap();
        assert!(s.status().unwrap().next_attempt.is_none());
        drop(guard);
        assert!(lock(&paths).is_ok());
    }
}
