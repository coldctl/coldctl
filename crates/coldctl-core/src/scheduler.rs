//! A single local worker, with durable admission and checkpoint-based crash recovery.
use crate::{
    archive::{checkpoint, controls::Shutdown, executor},
    error::Error,
    paths::StatePaths,
    state::{
        archive::{self, JobStatus},
        scheduler as store,
    },
};
use std::time::Duration;

pub struct Worker {
    paths: StatePaths,
    // Do not delete the lock file: SQLite releases ownership when the process exits.
    _lock: rusqlite::Connection,
}
impl Worker {
    pub fn acquire(paths: &StatePaths) -> Result<Self, Error> {
        store::list(paths)?;
        let lock = checkpoint::scoped_lock(paths, "scheduler-locks", "worker")?;
        store::recover(paths)?;
        Ok(Self {
            _lock: lock,
            paths: paths.clone(),
        })
    }

    /// Execute at most one due time slice. A failed archive does not kill the worker.
    pub async fn tick(&self, shutdown: Shutdown) -> Result<Option<store::Entry>, Error> {
        let paths = &self.paths;
        if shutdown.requested() {
            return Ok(None);
        }
        crate::state::recurring::admit(paths, store::now())?;
        let Some(entry) = store::claim(paths, store::now())? else {
            return Ok(None);
        };
        let mut result = self.execute(paths, &entry, shutdown.clone()).await;
        if matches!(result, Ok(JobStatus::Completed)) && entry.options.delete_after_archive {
            let saved = store::show(paths, &entry.id)?;
            result = if saved.status == "cancelled" {
                Ok(JobStatus::Cancelled)
            } else if let Some(id) = &saved.job_id {
                crate::archive::deletion::run(
                    paths,
                    id,
                    crate::archive::deletion::Options {
                        confirm_delete: true,
                        confirm_original_source: entry.options.confirm_original_source,
                        max_batches: Some(1),
                        shutdown,
                    },
                )
                .await
                .map(|p| {
                    if p.completed {
                        JobStatus::Completed
                    } else {
                        JobStatus::Paused
                    }
                })
            } else {
                Err(Error::Archive(
                    "completed queue entry is missing its archive job",
                ))
            };
        }
        let (status, delay, failures, message) = outcome(&entry, result);
        store::finish(
            paths,
            &entry.id,
            status,
            store::now() + delay,
            failures,
            message.as_deref(),
        )?;
        Ok(Some(store::show(paths, &entry.id)?))
    }

    async fn execute(
        &self,
        paths: &StatePaths,
        entry: &store::Entry,
        shutdown: Shutdown,
    ) -> Result<JobStatus, Error> {
        if let Some(id) = &entry.job_id {
            let job = archive::job_show(paths, id)?;
            if job.status == JobStatus::Completed {
                return Ok(JobStatus::Completed);
            }
            // Manual cancellation always wins over automatic retry/resume.
            if job.cancel_requested || job.status == JobStatus::Cancelled {
                return Ok(JobStatus::Cancelled);
            }
            executor::resume_scheduled(
                paths,
                id,
                entry.options.confirm_original_source,
                entry.options.execution.clone(),
                shutdown,
                None,
                true,
            )
            .await
            .map(|j| {
                if j.cancel_requested {
                    JobStatus::Cancelled
                } else {
                    j.status
                }
            })
        } else {
            executor::run_queued(
                paths,
                &entry.policy_name,
                executor::RunOptions {
                    source_stability: entry.options.source_stability,
                    allow_repeat: entry.options.allow_repeat,
                    minimum_free_bytes: entry.options.execution.minimum_free_bytes,
                    execution: entry.options.execution.clone(),
                    shutdown,
                    progress: None,
                },
                |_| {},
                Some(&entry.id),
            )
            .await
            .map(|j| j.status)
        }
    }
}

fn outcome(
    entry: &store::Entry,
    result: Result<JobStatus, Error>,
) -> (&'static str, i64, u32, Option<String>) {
    match result {
        Ok(JobStatus::Completed) => ("completed", 0, 0, None),
        Ok(JobStatus::Cancelled) => ("cancelled", 0, 0, None),
        // Requeue at the tail so another due archive can make progress.
        Ok(JobStatus::Paused | JobStatus::Running) => ("pending", 1, 0, None),
        Ok(JobStatus::Failed) => (
            "failed",
            0,
            entry.failures,
            Some("archive failed; inspect the job".into()),
        ),
        Err(error) => {
            let failure = error.failure();
            let failures = entry.failures.saturating_add(1);
            if failure.retryable && failures <= entry.options.max_retries {
                (
                    "pending",
                    (30_i64 * (1_i64 << failures.min(8).saturating_sub(1))).min(3600),
                    failures,
                    Some(failure.message),
                )
            } else {
                ("failed", 0, failures, Some(failure.message))
            }
        }
    }
}

/// Interruptible idle wait; active archives observe shutdown at safe batch boundaries.
pub async fn idle(shutdown: &Shutdown) {
    for _ in 0..10 {
        if shutdown.requested() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        archive::{
            controls::ExecutionControls,
            planner::{ArchivePlan, SourceStability},
        },
        policy::model::PolicyConfig,
        source::SourceConnection,
        state::{self, destinations, sources},
    };

    fn fixture() -> (tempfile::TempDir, StatePaths, store::Options) {
        let temp = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(temp.path())).unwrap();
        state::initialize(&paths, "test").unwrap();
        sources::add(
            &paths,
            "source",
            SourceConnection::from_url_env("SCHEDULER_UNUSED_URL".into()).unwrap(),
        )
        .unwrap();
        destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
        archive::policy_create(
            &paths,
            "orders",
            "source",
            "disk",
            PolicyConfig {
                schema: "public".into(),
                table: "orders".into(),
                time_column: "created_at".into(),
                older_than_days: 365,
                batch_size: 2,
                equals_column: None,
                equals_value: None,
            },
        )
        .unwrap();
        (
            temp,
            paths,
            store::Options {
                delete_after_archive: false,
                source_stability: SourceStability::ImmutableRows,
                allow_repeat: false,
                confirm_original_source: true,
                max_retries: 2,
                execution: ExecutionControls {
                    max_duration_seconds: Some(60),
                    ..Default::default()
                },
            },
        )
    }

    fn plan(paths: &StatePaths) -> ArchivePlan {
        ArchivePlan {
            connector_pin: None,
            policy: archive::policy_show(paths, "orders").unwrap(),
            destination_config: None,
            destination_path: paths.data_dir.join("archives"),
            cutoff_utc: "2025-01-01T00:00:00Z".into(),
            primary_key: "id".into(),
            table_oid: 42,
            columns: vec![],
            estimated_rows: None,
            delete: false,
            warnings: vec![],
            safety: None,
        }
    }

    #[test]
    fn durable_admission_due_times_exclusion_and_restart_recovery() {
        let (_temp, paths, options) = fixture();
        let entry = store::enqueue(&paths, "orders", None, options.clone(), 120).unwrap();
        assert!(store::enqueue(&paths, "orders", None, options, 0).is_err());
        let worker = Worker::acquire(&paths).unwrap();
        assert!(Worker::acquire(&paths).is_err());
        assert!(store::claim(&paths, entry.due_at - 1).unwrap().is_none());
        assert_eq!(
            store::claim(&paths, entry.due_at).unwrap().unwrap().id,
            entry.id
        );
        assert!(store::claim(&paths, entry.due_at).unwrap().is_none());
        drop(worker);
        let _worker = Worker::acquire(&paths).unwrap();
        assert_eq!(store::show(&paths, &entry.id).unwrap().status, "pending");
        assert_eq!(store::list(&paths).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn job_assignment_is_atomic_and_completed_job_is_not_exported_again() {
        let (_temp, paths, options) = fixture();
        let entry = store::enqueue(&paths, "orders", None, options.clone(), 0).unwrap();
        let worker = Worker::acquire(&paths).unwrap();
        store::claim(&paths, store::now()).unwrap().unwrap();
        let plan = plan(&paths);
        let job = archive::job_create_queued(
            &paths,
            &plan,
            "source",
            "identity",
            None,
            &options.execution,
            Some(&entry.id),
        )
        .unwrap();
        assert_eq!(
            store::show(&paths, &entry.id).unwrap().job_id.as_deref(),
            Some(job.id.as_str())
        );
        assert!(
            archive::job_create_queued(
                &paths,
                &plan,
                "source",
                "identity",
                None,
                &options.execution,
                Some(&entry.id)
            )
            .is_err()
        );
        assert_eq!(archive::job_list(&paths).unwrap().len(), 1);
        archive::job_finish(&paths, &job.id, None).unwrap();
        // Crash after archive completion but before queue completion.
        drop(worker);
        let worker = Worker::acquire(&paths).unwrap();
        let result = worker.tick(Shutdown::default()).await.unwrap().unwrap();
        assert_eq!(result.status, "completed");
        assert_eq!(archive::job_list(&paths).unwrap().len(), 1);
    }

    #[test]
    fn cancellation_rolls_back_creation_and_cannot_be_overwritten_by_finish() {
        let (_temp, paths, options) = fixture();
        let entry = store::enqueue(&paths, "orders", None, options.clone(), 0).unwrap();
        let _worker = Worker::acquire(&paths).unwrap();
        store::claim(&paths, store::now()).unwrap();
        store::cancel(&paths, &entry.id).unwrap();
        assert!(
            archive::job_create_queued(
                &paths,
                &plan(&paths),
                "source",
                "identity",
                None,
                &options.execution,
                Some(&entry.id)
            )
            .is_err()
        );
        assert!(archive::job_list(&paths).unwrap().is_empty());
        store::finish(&paths, &entry.id, "pending", 0, 0, None).unwrap();
        assert_eq!(store::show(&paths, &entry.id).unwrap().status, "cancelled");
    }

    #[tokio::test]
    async fn cancellation_of_assigned_job_survives_restart_and_automatic_resume() {
        let (_temp, paths, options) = fixture();
        let entry = store::enqueue(&paths, "orders", None, options.clone(), 0).unwrap();
        let worker = Worker::acquire(&paths).unwrap();
        store::claim(&paths, store::now()).unwrap();
        let job = archive::job_create_queued(
            &paths,
            &plan(&paths),
            "source",
            "identity",
            None,
            &options.execution,
            Some(&entry.id),
        )
        .unwrap();
        checkpoint::cancel(&paths, &job.id).unwrap();
        assert!(!checkpoint::restart_uncancelled(&paths, &job.id).unwrap());
        drop(worker);
        let worker = Worker::acquire(&paths).unwrap();
        assert_eq!(
            worker
                .tick(Shutdown::default())
                .await
                .unwrap()
                .unwrap()
                .status,
            "cancelled"
        );
        assert!(archive::job_show(&paths, &job.id).unwrap().cancel_requested);
    }

    #[test]
    fn slices_retry_backoff_and_permanent_failures_have_distinct_outcomes() {
        let (_temp, paths, options) = fixture();
        let mut entry = store::enqueue(&paths, "orders", None, options, 0).unwrap();
        assert_eq!(outcome(&entry, Ok(JobStatus::Paused)).0, "pending");
        let transient = || Err(Error::Archive("operation deadline exceeded"));
        assert_eq!(outcome(&entry, transient()).1, 30);
        entry.failures = 1;
        assert_eq!(outcome(&entry, transient()).1, 60);
        entry.failures = 2;
        assert_eq!(outcome(&entry, transient()).0, "failed");
        assert_eq!(outcome(&entry, Ok(JobStatus::Paused)).2, 0);
        entry.options.max_retries = 100;
        entry.failures = 99;
        assert_eq!(outcome(&entry, transient()).1, 3600);
        let permanent = outcome(&entry, Err(Error::InvalidState("secret".into())));
        assert_eq!(permanent.0, "failed");
        assert!(!permanent.3.unwrap().contains("secret"));
    }

    #[tokio::test]
    async fn shutdown_does_not_claim_and_failed_entries_require_explicit_retry() {
        let (_temp, paths, options) = fixture();
        let entry = store::enqueue(&paths, "orders", None, options, 0).unwrap();
        let worker = Worker::acquire(&paths).unwrap();
        let shutdown = Shutdown::default();
        shutdown.request();
        assert!(worker.tick(shutdown).await.unwrap().is_none());
        assert_eq!(store::show(&paths, &entry.id).unwrap().status, "pending");
        store::claim(&paths, store::now()).unwrap();
        store::finish(
            &paths,
            &entry.id,
            "failed",
            0,
            1,
            Some("configuration rejected"),
        )
        .unwrap();
        assert!(store::retry(&paths, &entry.id, false).is_err());
        assert_eq!(
            store::retry(&paths, &entry.id, true).unwrap().status,
            "pending"
        );
    }
}
