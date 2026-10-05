use super::database::{open_initialized, sql_error};
use crate::{error::Error, paths::StatePaths};
use rusqlite::{
    Connection,
    backup::{Backup, StepResult},
};
use serde::Serialize;
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

#[derive(Serialize)]
pub struct BackupReport {
    pub path: PathBuf,
    pub bytes: u64,
    pub sha256: String,
    pub archives_included: bool,
}

/// SQLite's online backup API includes committed WAL contents. The result is a
/// consistent database snapshot, not an atomic snapshot of external archive files.
pub fn create(paths: &StatePaths, output: &Path) -> Result<BackupReport, Error> {
    crate::fs_security::check(output)?;
    let source = open_initialized(paths, false)?;
    let parent = output
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = parent
        .canonicalize()
        .map_err(|_| Error::Archive("backup parent directory must exist"))?;
    let state_dir = paths
        .data_dir
        .canonicalize()
        .map_err(|_| Error::Archive("cannot resolve state directory"))?;
    if parent.starts_with(state_dir) {
        return Err(Error::Archive(
            "backup must be outside the state directory to protect SQLite files and sidecars",
        ));
    }
    let name = output
        .file_name()
        .ok_or(Error::Archive("backup requires a file path"))?;
    let output = parent.join(name);
    if output.symlink_metadata().is_ok() {
        return Err(Error::Archive(
            "backup output already exists; choose a new file",
        ));
    }
    let temp = tempfile::NamedTempFile::new_in(parent)
        .map_err(|_| Error::Archive("cannot stage state backup"))?;
    {
        let mut target = Connection::open(temp.path()).map_err(sql_error)?;
        let started = Instant::now();
        {
            let backup = Backup::new(&source, &mut target).map_err(sql_error)?;
            loop {
                if started.elapsed() > Duration::from_secs(30) {
                    return Err(Error::Archive(
                        "state backup timed out; stop writers and retry",
                    ));
                }
                match backup.step(128).map_err(sql_error)? {
                    StepResult::Done => break,
                    StepResult::More => {}
                    StepResult::Busy | StepResult::Locked => {
                        std::thread::sleep(Duration::from_millis(10))
                    }
                    _ => return Err(Error::Archive("unexpected state backup result")),
                }
            }
        }
        let integrity: String = target
            .query_row("PRAGMA integrity_check", [], |r| r.get(0))
            .map_err(sql_error)?;
        if integrity != "ok"
            || target
                .prepare("PRAGMA foreign_key_check")
                .map_err(sql_error)?
                .query([])
                .map_err(sql_error)?
                .next()
                .map_err(sql_error)?
                .is_some()
        {
            return Err(Error::Archive("state backup verification failed"));
        }
        target.close().map_err(|(_, e)| sql_error(e))?;
    }
    temp.as_file()
        .sync_all()
        .map_err(|_| Error::Archive("cannot sync state backup"))?;
    let (bytes, sha256) = crate::destination::local::fingerprint(temp.path())?;
    temp.persist_noclobber(&output)
        .map_err(|_| Error::Archive("cannot publish state backup without overwrite"))?;
    Ok(BackupReport {
        path: output,
        bytes,
        sha256,
        archives_included: false,
    })
}
