use coldctl_core::archive::progress::{Observer, Progress, Stage};
use std::{
    io::Write,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

/// A separate thread continues reporting during synchronous Parquet/file work.
/// Only one snapshot is retained, regardless of batch count or execution time.
pub struct Display {
    latest: Arc<Mutex<Option<(String, Progress, Instant)>>>,
    stop: mpsc::Sender<()>,
    worker: Option<thread::JoinHandle<()>>,
}
impl Display {
    pub fn start() -> Self {
        let latest = Arc::new(Mutex::new(None::<(String, Progress, Instant)>));
        let shared = Arc::clone(&latest);
        let (stop, receive) = mpsc::channel();
        let worker = thread::spawn(move || {
            let started = Instant::now();
            loop {
                let done = !matches!(
                    receive.recv_timeout(Duration::from_secs(2)),
                    Err(mpsc::RecvTimeoutError::Timeout)
                );
                if let Ok(snapshot) = shared.lock() {
                    if let Some((id, progress, received)) = snapshot.as_ref() {
                        let _ = writeln!(
                            std::io::stderr().lock(),
                            "{}",
                            line(id, progress, received.elapsed())
                        );
                    } else if !done {
                        let _ = writeln!(
                            std::io::stderr().lock(),
                            "Archive preflight: {:.0}s elapsed; waiting for validation/connection.",
                            started.elapsed().as_secs_f64()
                        );
                    }
                }
                if done {
                    break;
                }
            }
        });
        Self {
            latest,
            stop,
            worker: Some(worker),
        }
    }
    pub fn observer(&self) -> Option<Observer> {
        let latest = Arc::clone(&self.latest);
        Some(Arc::new(move |id, progress| {
            if let Ok(mut snapshot) = latest.lock() {
                *snapshot = Some((id.into(), progress.clone(), Instant::now()));
            }
        }))
    }
}
impl Drop for Display {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
fn line(id: &str, p: &Progress, age: Duration) -> String {
    let active = !matches!(
        p.stage,
        Stage::Verified
            | Stage::VerificationFailed
            | Stage::Completed
            | Stage::Failed
            | Stage::Paused
            | Stage::Cancelled
    );
    format!(
        "Job {} | {:?} | {} rows, {} bytes committed | {:.1}s elapsed | last batch interval {:.1} rows/s, {:.1} bytes/s | last stage update {:.1}s ago",
        crate::output::text(id),
        p.stage,
        p.rows_committed,
        p.bytes_committed,
        p.elapsed_seconds + if active { age.as_secs_f64() } else { 0.0 },
        p.recent_rows_per_second,
        p.recent_bytes_per_second,
        age.as_secs_f64()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn heartbeat_shows_stale_stage_without_inventing_eta_or_rows() {
        let p = Progress {
            stage: Stage::Reading,
            observed_at_unix_ms: 0,
            elapsed_seconds: 3.0,
            rows_committed: 20,
            bytes_committed: 100,
            recent_rows_per_second: 2.0,
            recent_bytes_per_second: 10.0,
        };
        let output = line("job", &p, Duration::from_secs(12));
        assert!(output.contains("20 rows, 100 bytes committed"));
        assert!(output.contains("15.0s elapsed"));
        assert!(output.contains("12.0s ago"));
        assert!(!output.contains('%'));
        assert!(!output.contains("ETA"));
    }
}
