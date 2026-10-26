use super::database::{open_initialized, sql_error};
use crate::{error::Error, paths::StatePaths};
use rusqlite::OptionalExtension;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Progress {
    pub job_id: String,
    pub manifest_sha256: String,
    pub batches: i64,
    pub rows_processed: i64,
    pub rows_deleted: i64,
    /// Includes previously absent rows and deletions whose commit acknowledgement was lost.
    pub rows_absent: i64,
    pub rows_deferred: i64,
    pub rows_absent_uncertain: i64,
    pub deletion_mode: String,
    pub attribution_baseline: String,
    pub completed: bool,
}
fn open(paths: &StatePaths, write: bool) -> Result<rusqlite::Connection, Error> {
    let conn = open_initialized(paths, write)?;
    if super::migrations::version(&conn)? < 13 {
        return Err(Error::Archive(
            "deletion schema needs an upgrade; run `coldctl init`",
        ));
    }
    Ok(conn)
}
pub fn show(paths: &StatePaths, id: &str) -> Result<Option<Progress>, Error> {
    let conn = open(paths, false)?;
    let extras = if super::migrations::version(&conn)? >= 20 {
        "rows_deferred,rows_absent_uncertain,deletion_mode,attribution_baseline"
    } else {
        "0,0,'legacy','legacy_untracked'"
    };
    conn.query_row(&format!("SELECT job_id,manifest_sha256,batches,rows_processed,rows_deleted,rows_absent,completed,{extras} FROM archive_deletions WHERE job_id=?1"), [id], |r| Ok(Progress {
        job_id:r.get(0)?,manifest_sha256:r.get(1)?,batches:r.get(2)?,rows_processed:r.get(3)?,rows_deleted:r.get(4)?,rows_absent:r.get(5)?,completed:r.get(6)?,
        rows_deferred:r.get(7)?,rows_absent_uncertain:r.get(8)?,deletion_mode:r.get(9)?,attribution_baseline:r.get(10)?,
    })).optional().map_err(sql_error)
}
pub(crate) fn start(paths: &StatePaths, id: &str, hash: &str) -> Result<Progress, Error> {
    start_mode(paths, id, hash, false)
}
pub(crate) fn start_mode(
    paths: &StatePaths,
    id: &str,
    hash: &str,
    live: bool,
) -> Result<Progress, Error> {
    if live {
        let conn = open_live(paths)?;
        conn.execute("INSERT INTO archive_deletions(job_id,manifest_sha256,deletion_mode,attribution_baseline) VALUES(?1,?2,'live_values_v1','tracked_v1') ON CONFLICT(job_id) DO NOTHING", (id,hash)).map_err(sql_error)?;
    } else {
        open(paths,true)?.execute("INSERT INTO archive_deletions(job_id,manifest_sha256) VALUES(?1,?2) ON CONFLICT(job_id) DO NOTHING", (id,hash)).map_err(sql_error)?;
    }
    let saved = show(paths, id)?.ok_or(Error::Archive("missing deletion checkpoint"))?;
    if saved.manifest_sha256 != hash || (saved.deletion_mode == "live_values_v1") != live {
        return Err(Error::Archive("deletion manifest checkpoint mismatch"));
    }
    Ok(saved)
}
pub(crate) fn advance(
    paths: &StatePaths,
    id: &str,
    previous: i64,
    deleted: i64,
    absent: i64,
    completed: bool,
) -> Result<Progress, Error> {
    if deleted < 0 || absent < 0 {
        return Err(Error::Archive("invalid deletion counts"));
    }
    if show(paths, id)?.is_none_or(|p| p.deletion_mode != "legacy") {
        return Err(Error::Archive("legacy operation on live checkpoint"));
    }
    let changed = open(paths,true)?.execute("UPDATE archive_deletions SET batches=batches+1,rows_processed=rows_processed+?3+?4,rows_deleted=rows_deleted+?3,rows_absent=rows_absent+?4,completed=?5,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE job_id=?1 AND batches=?2 AND completed=0", (id,previous,deleted,absent,completed)).map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::Archive("deletion checkpoint changed unexpectedly"));
    }
    show(paths, id)?.ok_or(Error::Archive("missing deletion checkpoint"))
}
pub(crate) fn finish_empty(paths: &StatePaths, id: &str) -> Result<Progress, Error> {
    open(paths,true)?.execute("UPDATE archive_deletions SET completed=1 WHERE job_id=?1 AND batches=0 AND rows_processed=0",[id]).map_err(sql_error)?;
    show(paths, id)?.ok_or(Error::Archive("missing deletion checkpoint"))
}

fn open_live(paths: &StatePaths) -> Result<rusqlite::Connection, Error> {
    let conn = open(paths, true)?;
    if super::migrations::version(&conn)? < 20 {
        return Err(Error::Archive(
            "live deletion needs schema 20; run coldctl init",
        ));
    }
    Ok(conn)
}
pub(crate) fn require_live_schema(paths: &StatePaths) -> Result<(), Error> {
    open_live(paths).map(|_| ())
}

pub(crate) struct Attempt {
    sequence: i64,
    hash: String,
    count: i64,
    number: i64,
    uncertain: bool,
}

pub(crate) fn prepare_batch(
    paths: &StatePaths,
    id: &str,
    sequence: i64,
    hash: &str,
    count: i64,
) -> Result<Attempt, Error> {
    if !(1..=1000).contains(&count)
        || sequence <= 0
        || hash.len() != 64
        || !hash.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(Error::Archive("invalid live deletion batch identity"));
    }
    let mut conn = open_live(paths)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    let valid: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM archive_deletions WHERE job_id=?1 AND deletion_mode='live_values_v1' AND completed=0 AND batches=?2)", (id,sequence-1), |r|r.get(0)).map_err(sql_error)?;
    if !valid {
        return Err(Error::Archive("live deletion sequence changed"));
    }
    let previous: Option<(String,i64,String,i64)> = tx.query_row("SELECT object_sha256,row_count,state,attempts FROM archive_deletion_batches WHERE job_id=?1 AND sequence=?2", (id,sequence), |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional().map_err(sql_error)?;
    let (number, uncertain) = if let Some((saved, rows, state, attempts)) = previous {
        if saved != hash || rows != count || state != "pending" {
            return Err(Error::Archive("live deletion input changed"));
        }
        let number = attempts
            .checked_add(1)
            .ok_or(Error::Archive("deletion attempt overflow"))?;
        tx.execute("UPDATE archive_deletion_batches SET attempts=?3,recovery_uncertain=1,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE job_id=?1 AND sequence=?2", (id,sequence,number)).map_err(sql_error)?;
        (number, true)
    } else {
        tx.execute("INSERT INTO archive_deletion_batches(job_id,sequence,object_sha256,row_count,state,attempts,recovery_uncertain) VALUES(?1,?2,?3,?4,'pending',1,0)", (id,sequence,hash,count)).map_err(sql_error)?;
        (1, false)
    };
    tx.commit().map_err(sql_error)?;
    Ok(Attempt {
        sequence,
        hash: hash.into(),
        count,
        number,
        uncertain,
    })
}

pub(crate) fn apply_batch(
    paths: &StatePaths,
    id: &str,
    attempt: &Attempt,
    result: &crate::source::connector::LiveDeleteBatchResult,
    completed: bool,
) -> Result<Progress, Error> {
    let total = result
        .deleted
        .checked_add(result.absent)
        .and_then(|n| n.checked_add(result.deferred));
    if total != Some(attempt.count as u64) {
        return Err(Error::Archive("invalid live deletion result counts"));
    }
    let (deleted, absent, deferred) = (
        result.deleted as i64,
        result.absent as i64,
        result.deferred as i64,
    );
    let uncertain = if attempt.uncertain { absent } else { 0 };
    let mut conn = open_live(paths)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(sql_error)?;
    type StoredBatch = (
        String,
        i64,
        String,
        i64,
        bool,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
    );
    let stored: StoredBatch = tx.query_row("SELECT object_sha256,row_count,state,attempts,recovery_uncertain,deleted,absent,deferred,absent_uncertain FROM archive_deletion_batches WHERE job_id=?1 AND sequence=?2", (id,attempt.sequence), |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).map_err(sql_error)?;
    if stored.0 != attempt.hash
        || stored.1 != attempt.count
        || stored.3 != attempt.number
        || stored.4 != attempt.uncertain
    {
        return Err(Error::Archive("stale live deletion response"));
    }
    if stored.2 == "applied" {
        if (stored.5, stored.6, stored.7, stored.8)
            != (Some(deleted), Some(absent), Some(deferred), Some(uncertain))
        {
            return Err(Error::Archive("conflicting live deletion response"));
        }
        drop(tx);
        return show(paths, id)?.ok_or(Error::Archive("missing deletion checkpoint"));
    }
    let counters: (i64,i64,i64,i64,i64) = tx.query_row("SELECT rows_processed,rows_deleted,rows_absent,rows_deferred,rows_absent_uncertain FROM archive_deletions WHERE job_id=?1", [id], |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).map_err(sql_error)?;
    for (old, delta) in [
        (counters.0, attempt.count),
        (counters.1, deleted),
        (counters.2, absent),
        (counters.3, deferred),
        (counters.4, uncertain),
    ] {
        old.checked_add(delta)
            .ok_or(Error::Archive("deletion count overflow"))?;
    }
    let changed = tx.execute("UPDATE archive_deletions SET batches=batches+1,rows_processed=rows_processed+?3,rows_deleted=rows_deleted+?4,rows_absent=rows_absent+?5,rows_deferred=rows_deferred+?6,rows_absent_uncertain=rows_absent_uncertain+?7,completed=?8,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE job_id=?1 AND batches=?2 AND deletion_mode='live_values_v1' AND completed=0", rusqlite::params![id,attempt.sequence-1,attempt.count,deleted,absent,deferred,uncertain,completed]).map_err(sql_error)?;
    if changed != 1 {
        return Err(Error::Archive("live deletion checkpoint changed"));
    }
    tx.execute("UPDATE archive_deletion_batches SET state='applied',deleted=?3,absent=?4,deferred=?5,absent_uncertain=?6,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE job_id=?1 AND sequence=?2", rusqlite::params![id,attempt.sequence,deleted,absent,deferred,uncertain]).map_err(sql_error)?;
    tx.commit().map_err(sql_error)?;
    show(paths, id)?.ok_or(Error::Archive("missing deletion checkpoint"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::connector::LiveDeleteBatchResult as Counts;

    fn fixture() -> (tempfile::TempDir, StatePaths) {
        let temp = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(temp.path())).unwrap();
        crate::state::initialize(&paths, "test").unwrap();
        open_live(&paths).unwrap().execute("INSERT INTO jobs(id,policy_name,plan_json,status,started_at) VALUES('job','test','{}','completed','now')",[]).unwrap();
        start_mode(&paths, "job", &"a".repeat(64), true).unwrap();
        (temp, paths)
    }

    #[test]
    fn live_ledger_replays_pending_and_applies_once_with_uncertain_subset() {
        let (_temp, paths) = fixture();
        let first = prepare_batch(&paths, "job", 1, &"b".repeat(64), 3).unwrap();
        assert_eq!(show(&paths, "job").unwrap().unwrap().rows_processed, 0);
        let replay = prepare_batch(&paths, "job", 1, &"b".repeat(64), 3).unwrap();
        let counts = Counts {
            deleted: 1,
            absent: 1,
            deferred: 1,
        };
        assert!(apply_batch(&paths, "job", &first, &counts, true).is_err());
        let saved = apply_batch(&paths, "job", &replay, &counts, true).unwrap();
        assert!(saved.completed);
        assert_eq!(
            (
                saved.rows_processed,
                saved.rows_deleted,
                saved.rows_absent,
                saved.rows_deferred,
                saved.rows_absent_uncertain
            ),
            (3, 1, 1, 1, 1)
        );
        let again = apply_batch(&paths, "job", &replay, &counts, true).unwrap();
        assert_eq!(again.rows_processed, 3);
        assert!(prepare_batch(&paths, "job", 1, &"b".repeat(64), 3).is_err());
        assert!(
            apply_batch(
                &paths,
                "job",
                &replay,
                &Counts {
                    deleted: 2,
                    absent: 1,
                    deferred: 0
                },
                true
            )
            .is_err()
        );
    }

    #[test]
    fn live_ledger_rejects_changed_inputs_counts_and_legacy_operations() {
        let (_temp, paths) = fixture();
        let attempt = prepare_batch(&paths, "job", 1, &"b".repeat(64), 2).unwrap();
        assert!(prepare_batch(&paths, "job", 1, &"c".repeat(64), 2).is_err());
        assert!(prepare_batch(&paths, "job", 2, &"b".repeat(64), 2).is_err());
        assert!(start(&paths, "job", &"a".repeat(64)).is_err());
        assert!(advance(&paths, "job", 0, 2, 0, true).is_err());
        assert!(
            apply_batch(
                &paths,
                "job",
                &attempt,
                &Counts {
                    deleted: u64::MAX,
                    absent: 1,
                    deferred: 2
                },
                true
            )
            .is_err()
        );
        let saved = apply_batch(
            &paths,
            "job",
            &attempt,
            &Counts {
                deleted: 0,
                absent: 0,
                deferred: 2,
            },
            true,
        )
        .unwrap();
        assert_eq!((saved.rows_deferred, saved.rows_absent_uncertain), (2, 0));
        assert!(
            open_live(&paths)
                .unwrap()
                .execute(
                    "UPDATE archive_deletions SET rows_absent_uncertain=1 WHERE job_id='job'",
                    []
                )
                .is_err()
        );
    }

    #[test]
    fn live_migration_preserves_legacy_history_and_rolls_back_invalid_counts() {
        for invalid in [false, true] {
            let (_temp, paths) = fixture();
            let conn = open_live(&paths).unwrap();
            conn.execute_batch("DROP TABLE archive_deletion_batches; DROP TRIGGER deletion_counts_insert; DROP TRIGGER deletion_counts_update; ALTER TABLE archive_deletions DROP COLUMN rows_deferred; ALTER TABLE archive_deletions DROP COLUMN rows_absent_uncertain; ALTER TABLE archive_deletions DROP COLUMN deletion_mode; ALTER TABLE archive_deletions DROP COLUMN attribution_baseline; DELETE FROM schema_migrations WHERE version=20;").unwrap();
            conn.execute("UPDATE archive_deletions SET batches=1,rows_processed=?1,rows_deleted=2,rows_absent=1,completed=1",[if invalid {4} else {3}]).unwrap();
            let result = crate::state::initialize(&paths, "upgrade");
            if invalid {
                assert!(result.is_err());
                assert_eq!(super::super::migrations::version(&conn).unwrap(), 19);
                assert!(
                    conn.prepare("SELECT rows_deferred FROM archive_deletions")
                        .is_err()
                );
            } else {
                result.unwrap();
                let p = show(&paths, "job").unwrap().unwrap();
                assert_eq!(
                    (
                        p.rows_processed,
                        p.rows_deleted,
                        p.rows_absent,
                        p.rows_deferred
                    ),
                    (3, 2, 1, 0)
                );
                assert_eq!(p.attribution_baseline, "legacy_untracked");
                assert!(p.completed);
            }
        }
    }
}
