use crate::{error::Error, paths::StatePaths, state::archive as store};
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Connecting,
    Validating,
    Reading,
    Encoding,
    Staging,
    Publishing,
    Checkpointing,
    Waiting,
    Manifest,
    Verifying,
    Verified,
    VerificationFailed,
    Completed,
    Paused,
    Cancelled,
    Failed,
}

/// Counters include only durable Parquet batches. Elapsed time is per invocation.
/// No source values, keys, credentials, percentages, or estimated completion times.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Progress {
    pub stage: Stage,
    pub observed_at_unix_ms: u64,
    pub elapsed_seconds: f64,
    pub rows_committed: i64,
    pub bytes_committed: i64,
    pub recent_rows_per_second: f64,
    pub recent_bytes_per_second: f64,
}
pub type Observer = Arc<dyn Fn(&str, &Progress) + Send + Sync>;

pub(crate) struct Reporter<'a> {
    paths: &'a StatePaths,
    id: &'a str,
    started: Instant,
    sample: (Instant, i64, i64),
    rates: (f64, f64),
    observer: Option<Observer>,
}
impl<'a> Reporter<'a> {
    pub(crate) fn new(
        paths: &'a StatePaths,
        id: &'a str,
        started: Instant,
        observer: Option<Observer>,
    ) -> Result<Self, Error> {
        let job = store::job_show(paths, id)?;
        Ok(Self {
            paths,
            id,
            started,
            sample: (Instant::now(), job.rows_processed, job.bytes_written),
            rates: (0.0, 0.0),
            observer,
        })
    }
    pub(crate) fn stage(&mut self, stage: Stage) -> Result<(), Error> {
        let job = store::job_show(self.paths, self.id)?;
        if job.rows_processed != self.sample.1 {
            let seconds = self.sample.0.elapsed().as_secs_f64().max(0.001);
            self.rates = (
                (job.rows_processed - self.sample.1) as f64 / seconds,
                (job.bytes_written - self.sample.2) as f64 / seconds,
            );
            self.sample = (Instant::now(), job.rows_processed, job.bytes_written);
        }
        let progress = Progress {
            stage,
            observed_at_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u64::MAX as u128) as u64,
            elapsed_seconds: self.started.elapsed().as_secs_f64(),
            rows_committed: job.rows_processed,
            bytes_committed: job.bytes_written,
            recent_rows_per_second: self.rates.0,
            recent_bytes_per_second: self.rates.1,
        };
        store::job_progress(self.paths, self.id, &progress)?;
        if let Some(observer) = &self.observer {
            observer(self.id, &progress);
        }
        Ok(())
    }
    pub(crate) fn finish(&mut self) -> Result<(), Error> {
        use store::JobStatus;
        self.stage(match store::job_show(self.paths, self.id)?.status {
            JobStatus::Completed => Stage::Completed,
            JobStatus::Paused => Stage::Paused,
            JobStatus::Cancelled => Stage::Cancelled,
            JobStatus::Failed => Stage::Failed,
            JobStatus::Running => return Ok(()),
        })
    }
}
