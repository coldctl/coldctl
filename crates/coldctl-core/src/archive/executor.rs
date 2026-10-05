use super::controls::{ExecutionControls, Shutdown};
use super::progress::{Observer, Reporter, Stage};
use super::{checkpoint as journal, manifest::ManifestObject, planner::ArchivePlan, verifier};
use crate::{
    destination::{
        ArchiveDestination, StoredObject,
        local::{LocalDestination, fingerprint},
    },
    error::Error,
    format::parquet,
    paths::StatePaths,
    source::{ArchiveSource, postgres_archive::PostgresArchive},
    state::{archive as store, destinations, sources},
};
use std::io::Write;
use std::time::{Duration, Instant};

pub struct RunOptions {
    pub source_stability: super::planner::SourceStability,
    pub allow_repeat: bool,
    pub minimum_free_bytes: u64,
    pub execution: ExecutionControls,
    pub shutdown: Shutdown,
    pub progress: Option<Observer>,
}

fn check_prior_runs(paths: &StatePaths, policy_id: &str, allow_repeat: bool) -> Result<(), Error> {
    let jobs = store::job_list(paths)?;
    if jobs
        .iter()
        .any(|job| job.plan.policy.id == policy_id && job.status == store::JobStatus::Running)
    {
        return Err(Error::Archive(
            "policy has a running or interrupted job; resume it or cancel it before a new run",
        ));
    }
    if !allow_repeat && jobs.iter().any(|job| job.plan.policy.id == policy_id) {
        return Err(Error::Archive(
            "policy already has an archive job; use resume, or --allow-repeat to acknowledge possible duplicate exports",
        ));
    }
    Ok(())
}

pub async fn run(paths: &StatePaths, name: &str, options: RunOptions) -> Result<store::Job, Error> {
    run_with_preflight_report(paths, name, options, |_| {}).await
}

pub async fn run_with_preflight_report(
    paths: &StatePaths,
    name: &str,
    options: RunOptions,
    report: impl FnOnce(&ArchivePlan),
) -> Result<store::Job, Error> {
    let started = Instant::now();
    let mut controls = options.execution.clone();
    controls.minimum_free_bytes = options.minimum_free_bytes;
    controls.validate()?;
    let policy = store::policy_show(paths, name)?;
    let _policy_guard = journal::policy_lock(paths, &policy.id)?;
    check_prior_runs(paths, &policy.id, options.allow_repeat)?;
    let source = sources::show(paths, &policy.source)?;
    let destination = destinations::show(paths, &policy.destination)?;
    let mut reader = PostgresArchive::connect(source.connection).await?;
    reader.configure_timeouts(&controls).await?;
    let mut plan = reader.plan(policy, destination.path).await?;
    let source_identity = reader.source_identity(plan.table_oid).await?;
    let minimum_free_bytes = options.minimum_free_bytes.max(1);
    let available =
        LocalDestination::new(plan.destination_path.clone())?.preflight(minimum_free_bytes)?;
    plan.safety = Some(super::planner::SafetyContract {
        source_stability: options.source_stability,
        source_identity,
        available_bytes_at_preflight: available,
        minimum_free_bytes,
    });
    report(&plan);
    let upper = reader.upper_key(&plan).await?;
    let job = store::job_create(
        paths,
        &plan,
        &source.id,
        reader.identity(),
        upper,
        &controls,
    )?;
    let _guard = journal::lock(paths, &job.id)?;
    execute_observed(
        paths,
        &job.id,
        &mut reader,
        &controls,
        &options.shutdown,
        started,
        options.progress,
    )
    .await
}

/// Resume/retry keeps the same ID, immutable plan, maximum key, and object names.
pub async fn resume(
    paths: &StatePaths,
    id: &str,
    confirm_original_source: bool,
) -> Result<store::Job, Error> {
    let controls = store::job_show(paths, id)?.execution;
    resume_with_controls(
        paths,
        id,
        confirm_original_source,
        controls,
        Shutdown::default(),
    )
    .await
}

pub async fn resume_with_controls(
    paths: &StatePaths,
    id: &str,
    confirm_original_source: bool,
    controls: ExecutionControls,
    shutdown: Shutdown,
) -> Result<store::Job, Error> {
    resume_with_progress(paths, id, confirm_original_source, controls, shutdown, None).await
}

pub async fn resume_with_progress(
    paths: &StatePaths,
    id: &str,
    confirm_original_source: bool,
    controls: ExecutionControls,
    shutdown: Shutdown,
    observer: Option<Observer>,
) -> Result<store::Job, Error> {
    controls.validate()?;
    let started = Instant::now();
    let _guard = journal::lock(paths, id)?;
    let job = store::job_show(paths, id)?;
    if job.status == store::JobStatus::Completed {
        verifier::verify_observed(paths, id, observer)?;
        return store::job_show(paths, id);
    }
    let _policy_guard = journal::policy_lock(paths, &job.plan.policy.id)?;
    if !confirm_original_source {
        return Err(Error::Archive(
            "validate that this is the original database and that eligible rows remain stable, then pass --confirm-original-source; endpoint/OID checks cannot prove this",
        ));
    }
    let checkpoint = journal::load(paths, id)?;
    journal::execution(paths, id, &controls)?;
    journal::restart(paths, id)?;
    let mut progress = Reporter::new(paths, id, started, observer.clone())?;
    let result = async {
        progress.stage(Stage::Validating)?;
        let source = sources::show(paths, &job.plan.policy.source)?;
        if source.id != checkpoint.source_id {
            return Err(Error::Archive(
                "original source configuration no longer exists",
            ));
        }
        // Verify all committed files before connecting to or reading the source.
        verifier::verify_batches(paths, &job)?;
        progress.stage(Stage::Connecting)?;
        let mut reader = PostgresArchive::connect(source.connection).await?;
        if reader.identity() != checkpoint.source_identity {
            return Err(Error::Archive(
                "resolved source endpoint or user changed; refusing resume",
            ));
        }
        reader.configure_timeouts(&controls).await?;
        progress.stage(Stage::Validating)?;
        reader.validate_resume(&job.plan).await?;
        LocalDestination::new(job.plan.destination_path.clone())?.test_access()?;
        execute_observed(
            paths,
            id,
            &mut reader,
            &controls,
            &shutdown,
            started,
            observer.clone(),
        )
        .await
    }
    .await;
    match result {
        Err(error @ Error::JobFailed { .. }) => Err(error),
        Err(error) => {
            let failure = error.failure();
            store::job_failure(paths, id, &error)?;
            progress.finish()?;
            Err(Error::JobFailed {
                job_id: id.into(),
                reason: failure.message.clone(),
                failure,
            })
        }
        Ok(job) => Ok(job),
    }
}

#[cfg(test)]
async fn execute(
    paths: &StatePaths,
    id: &str,
    reader: &mut impl ArchiveSource,
) -> Result<store::Job, Error> {
    let controls = store::job_show(paths, id)?.execution;
    execute_controlled(
        paths,
        id,
        reader,
        &controls,
        &Shutdown::default(),
        Instant::now(),
    )
    .await
}

#[cfg(test)]
async fn execute_controlled(
    paths: &StatePaths,
    id: &str,
    reader: &mut impl ArchiveSource,
    controls: &ExecutionControls,
    shutdown: &Shutdown,
    started: Instant,
) -> Result<store::Job, Error> {
    execute_observed(paths, id, reader, controls, shutdown, started, None).await
}

async fn execute_observed(
    paths: &StatePaths,
    id: &str,
    reader: &mut impl ArchiveSource,
    controls: &ExecutionControls,
    shutdown: &Shutdown,
    started: Instant,
    observer: Option<Observer>,
) -> Result<store::Job, Error> {
    let mut progress = Reporter::new(paths, id, started, observer)?;
    let result = export(
        paths,
        id,
        reader,
        controls,
        shutdown,
        started,
        &mut progress,
    )
    .await;
    match result {
        Ok(()) => {
            progress.finish()?;
            store::job_show(paths, id)
        }
        Err(error) => {
            let failure = error.failure();
            let reason = failure.message.clone();
            store::job_failure(paths, id, &error)?;
            progress.finish()?;
            Err(Error::JobFailed {
                job_id: id.into(),
                reason,
                failure,
            })
        }
    }
}

pub(crate) fn publish(
    destination: &LocalDestination,
    expected: &StoredObject,
    source: &std::path::Path,
) -> Result<(), Error> {
    let target = destination.object_path(&expected.key)?;
    match target.try_exists() {
        Ok(true) => destination.verify(expected),
        Ok(false) => {
            let (bytes, sha256) = fingerprint(source)?;
            if bytes != expected.bytes || sha256 != expected.sha256 {
                return Err(Error::Archive("staged object failed verification"));
            }
            destination.put_file(&expected.key, source)?;
            destination.verify(expected)
        }
        Err(_) => Err(Error::Archive("cannot inspect destination object")),
    }
}

pub(crate) fn manifest_file(
    paths: &StatePaths,
    id: &str,
    plan: &ArchivePlan,
    upper: Option<i64>,
) -> Result<tempfile::NamedTempFile, Error> {
    let mut file =
        tempfile::NamedTempFile::new().map_err(|_| Error::Archive("cannot stage manifest"))?;
    let mut header = serde_json::to_string(
        &serde_json::json!({"format_version":2,"job_id":id,"plan":plan,"upper_key":upper}),
    )
    .map_err(|_| Error::Archive("cannot encode manifest"))?;
    header.pop();
    write!(file, "{header},\"objects\":[").map_err(|_| Error::Archive("cannot write manifest"))?;
    let job = store::job_show(paths, id)?;
    for sequence in 1..=job.objects_created {
        let (entry, committed) = journal::entry(paths, id, sequence)?
            .ok_or(Error::Archive("missing batch checkpoint"))?;
        if !committed {
            return Err(Error::Archive("uncommitted batch in manifest"));
        }
        if sequence > 1 {
            file.write_all(b",")
                .map_err(|_| Error::Archive("cannot write manifest"))?;
        }
        serde_json::to_writer(&mut file, &entry.batch)
            .map_err(|_| Error::Archive("cannot encode manifest entry"))?;
    }
    write!(
        file,
        "],\"rows\":{},\"source_deleted\":false}}",
        job.rows_processed
    )
    .map_err(|_| Error::Archive("cannot finish manifest"))?;
    file.as_file()
        .sync_all()
        .map_err(|_| Error::Archive("cannot sync manifest"))?;
    Ok(file)
}

async fn export(
    paths: &StatePaths,
    id: &str,
    reader: &mut impl ArchiveSource,
    controls: &ExecutionControls,
    shutdown: &Shutdown,
    started: Instant,
    progress: &mut Reporter<'_>,
) -> Result<(), Error> {
    let checkpoint = journal::load(paths, id)?;
    let mut job = store::job_show(paths, id)?;
    if job.status != store::JobStatus::Running {
        return Ok(());
    }
    let initial_rows = job.rows_processed;
    let mut next_batch = Instant::now();
    let destination = LocalDestination::new(job.plan.destination_path.clone())?;
    loop {
        if journal::cancelled(paths, id)? {
            return Ok(());
        }
        if stop(
            paths,
            id,
            controls,
            shutdown,
            started,
            job.rows_processed - initial_rows,
        )? {
            return Ok(());
        }
        if Instant::now() < next_batch {
            tokio::time::sleep(
                next_batch
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(100)),
            )
            .await;
            continue;
        }
        let remaining = controls
            .max_rows
            .map(|limit| limit - (job.rows_processed - initial_rows) as u64);
        let sequence = job.objects_created + 1;
        let entry = match journal::entry(paths, id, sequence)? {
            Some((entry, false)) => {
                if remaining.is_some_and(|n| entry.batch.rows as u64 > n) {
                    journal::pause(
                        paths,
                        id,
                        "pending batch exceeds this invocation's remaining row budget; resume with a larger --max-rows",
                    )?;
                    return Ok(());
                }
                entry
            }
            Some((_, true)) => {
                return Err(Error::Archive("inconsistent committed batch checkpoint"));
            }
            None => {
                // A manifest checkpoint means source reading already finished.
                if checkpoint.manifest.is_some() {
                    break;
                }
                let Some(upper) = checkpoint.upper else {
                    break;
                };
                if !destination.has_capacity(controls.minimum_free_bytes, 0)? {
                    journal::pause(paths, id, "destination free-space reserve reached")?;
                    return Ok(());
                }
                let mut read_plan = job.plan.clone();
                if let Some(n) = remaining {
                    read_plan.policy.config.batch_size = read_plan
                        .policy
                        .config
                        .batch_size
                        .min(n.min(i32::MAX as u64) as i32);
                }
                progress.stage(Stage::Reading)?;
                let Some(batch) = reader.read_batch(&read_plan, job.last_key, upper).await? else {
                    break;
                };
                progress.stage(Stage::Encoding)?;
                let encoded = parquet::encode(&job.plan.columns, &batch)?;
                let encoded_bytes = encoded
                    .as_file()
                    .metadata()
                    .map_err(|_| Error::Archive("cannot inspect encoded batch"))?
                    .len();
                if !destination
                    .has_capacity(controls.minimum_free_bytes, encoded_bytes.saturating_mul(2))?
                {
                    journal::pause(
                        paths,
                        id,
                        "insufficient free space to stage and publish the batch",
                    )?;
                    return Ok(());
                }
                let staged_key = format!("{id}/.staged-{}.parquet", uuid::Uuid::new_v4());
                progress.stage(Stage::Staging)?;
                let staged = destination.put_file(&staged_key, encoded.path())?;
                let entry = journal::Entry {
                    batch: ManifestObject {
                        object: StoredObject {
                            key: format!("{id}/batch-{sequence:08}.parquet"),
                            bytes: staged.bytes,
                            sha256: staged.sha256,
                        },
                        rows: batch.rows.len() as i64,
                        last_key: batch.last_key,
                    },
                    staged_key,
                };
                journal::prepare(paths, id, sequence, &entry)?;
                entry
            }
        };
        let staged_name = entry
            .staged_key
            .strip_prefix(&format!("{id}/.staged-"))
            .and_then(|name| name.strip_suffix(".parquet"));
        if entry.batch.object.key != format!("{id}/batch-{sequence:08}.parquet")
            || staged_name.is_none_or(|name| uuid::Uuid::parse_str(name).is_err())
        {
            return Err(Error::Archive("invalid pending object key"));
        }
        if !destination.has_capacity(controls.minimum_free_bytes, entry.batch.object.bytes)? {
            journal::pause(
                paths,
                id,
                "insufficient free space to publish pending batch",
            )?;
            return Ok(());
        }
        progress.stage(Stage::Publishing)?;
        publish(
            &destination,
            &entry.batch.object,
            &destination.object_path(&entry.staged_key)?,
        )?;
        progress.stage(Stage::Checkpointing)?;
        journal::commit(paths, id, sequence, &entry)?;
        progress.stage(Stage::Waiting)?;
        // Cleanup is best effort: a crash may leave an unreferenced staging file.
        let _ = std::fs::remove_file(destination.object_path(&entry.staged_key)?);
        job = store::job_show(paths, id)?;
        next_batch = Instant::now() + Duration::from_millis(controls.batch_delay_ms);
    }
    if journal::cancelled(paths, id)? {
        return Ok(());
    }
    if stop(
        paths,
        id,
        controls,
        shutdown,
        started,
        job.rows_processed - initial_rows,
    )? {
        return Ok(());
    }
    progress.stage(Stage::Manifest)?;
    let file = manifest_file(paths, id, &job.plan, checkpoint.upper)?;
    let (bytes, sha256) = fingerprint(file.path())?;
    let object = StoredObject {
        key: format!("{id}/manifest.json"),
        bytes,
        sha256,
    };
    if let Some(expected) = checkpoint.manifest {
        if expected.key != object.key
            || expected.bytes != object.bytes
            || expected.sha256 != object.sha256
        {
            return Err(Error::Archive("manifest differs from durable checkpoint"));
        }
    } else {
        journal::manifest(paths, id, &object)?;
    }
    if !destination.has_capacity(controls.minimum_free_bytes, object.bytes)? {
        journal::pause(paths, id, "insufficient free space to publish manifest")?;
        return Ok(());
    }
    publish(&destination, &object, file.path())?;
    // Cancellation is cooperative; once finalization wins, the job is complete.
    if !journal::cancelled(paths, id)? {
        store::job_finish(paths, id, None)?;
    }
    Ok(())
}

fn stop(
    paths: &StatePaths,
    id: &str,
    controls: &ExecutionControls,
    shutdown: &Shutdown,
    started: Instant,
    rows: i64,
) -> Result<bool, Error> {
    let reason = if shutdown.requested() {
        Some("shutdown requested")
    } else if controls.max_rows.is_some_and(|limit| rows as u64 >= limit) {
        Some("invocation row limit reached")
    } else if controls
        .max_duration_seconds
        .is_some_and(|seconds| started.elapsed() >= Duration::from_secs(seconds))
    {
        Some("invocation duration limit reached")
    } else {
        None
    };
    if let Some(reason) = reason {
        journal::pause(paths, id, reason)?;
        return Ok(true);
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        archive::batch::{ArchiveColumn, DataBatch},
        policy::model::{Policy, PolicyConfig},
        state,
    };

    fn fixture() -> (tempfile::TempDir, StatePaths, store::Job) {
        let temp = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
        state::initialize(&paths, "test").unwrap();
        let plan = ArchivePlan {
            safety: None,
            policy: Policy {
                id: uuid::Uuid::new_v4().to_string(),
                name: "test".into(),
                source: "source".into(),
                destination: "disk".into(),
                created_at: "test".into(),
                config: PolicyConfig {
                    schema: "public".into(),
                    table: "events".into(),
                    time_column: "created_at".into(),
                    older_than_days: 365,
                    batch_size: 2,
                    equals_column: None,
                    equals_value: None,
                },
            },
            destination_path: temp.path().join("archives"),
            cutoff_utc: "2025-01-01T00:00:00Z".into(),
            primary_key: "id".into(),
            table_oid: 42,
            columns: vec![ArchiveColumn {
                name: "id".into(),
                postgres_type: "int8".into(),
                nullable: false,
            }],
            estimated_rows: None,
            delete: false,
            warnings: vec![],
        };
        let job = store::job_create(
            &paths,
            &plan,
            "original-source",
            "identity",
            Some(3),
            &ExecutionControls::default(),
        )
        .unwrap();
        (temp, paths, job)
    }
    struct Reader {
        calls: Vec<(Option<i64>, i64)>,
        cancel: Option<(StatePaths, String)>,
    }
    impl ArchiveSource for Reader {
        async fn upper_key(&mut self, _: &ArchivePlan) -> Result<Option<i64>, Error> {
            panic!("resume must not recalculate the upper bound")
        }
        async fn read_batch(
            &mut self,
            plan: &ArchivePlan,
            last: Option<i64>,
            upper: i64,
        ) -> Result<Option<DataBatch>, Error> {
            self.calls.push((last, upper));
            if let Some((paths, id)) = self.cancel.take() {
                journal::cancel(&paths, &id)?;
            }
            let keys: Vec<i64> = (1..=4)
                .filter(|key| last.is_none_or(|v| *key > v) && *key <= upper)
                .take(plan.policy.config.batch_size as usize)
                .collect();
            Ok(keys.last().copied().map(|last_key| DataBatch {
                rows: keys.iter().map(|n| vec![Some(n.to_string())]).collect(),
                last_key,
            }))
        }
    }
    fn reader() -> Reader {
        Reader {
            calls: vec![],
            cancel: None,
        }
    }
    #[tokio::test]
    async fn completed_archive_recovers_after_state_loss_and_directory_move() {
        use crate::archive::recovery;
        let (temp, paths, job) = fixture();
        execute(&paths, &job.id, &mut reader()).await.unwrap();
        let old = job.plan.destination_path.join(&job.id);
        let original = std::fs::read(old.join("manifest.json")).unwrap();
        let moved_root = temp.path().join("moved archives");
        std::fs::create_dir(&moved_root).unwrap();
        let moved = moved_root.join(&job.id);
        std::fs::rename(&old, &moved).unwrap();
        std::fs::remove_dir_all(&paths.data_dir).unwrap();
        assert!(!recovery::inspect(&moved).unwrap().verified);
        let report = recovery::verify_directory(&moved, None).unwrap();
        assert!(report.verified);
        assert_eq!(report.rows, 3);
        assert_eq!(
            report.integrity_basis,
            "supplied_manifest_not_authenticated"
        );
        let fresh = StatePaths::resolve(Some(&temp.path().join("fresh"))).unwrap();
        state::initialize(&fresh, "test").unwrap();
        let imported = recovery::import(&fresh, &moved, None).unwrap();
        assert!(imported.imported);
        assert_eq!(imported.plan.destination_path, job.plan.destination_path);
        assert_eq!(imported.storage_root, moved_root.canonicalize().unwrap());
        assert_eq!(imported.status, store::JobStatus::Completed);
        assert_eq!(imported.rows_processed, 3);
        assert!(imported.verified_at.is_some());
        assert_eq!(
            verifier::verify(&fresh, &job.id).unwrap().integrity_basis,
            "supplied_manifest_not_authenticated"
        );
        assert!(resume(&fresh, &job.id, false).await.is_ok());
        assert!(
            recovery::import(&fresh, &moved, None)
                .unwrap_err()
                .to_string()
                .contains("already exists")
        );
        assert_eq!(store::job_list(&fresh).unwrap().len(), 1);
        assert!(state::sources::list(&fresh).unwrap().is_empty());
        assert!(store::policy_list(&fresh).unwrap().is_empty());
        assert_eq!(
            std::fs::read(moved.join("manifest.json")).unwrap(),
            original
        );
    }

    #[tokio::test]
    async fn recovery_rejects_bad_manifests_and_damage_without_partial_import() {
        use crate::archive::recovery;
        let (temp, paths, job) = fixture();
        execute(&paths, &job.id, &mut reader()).await.unwrap();
        let directory = job.plan.destination_path.join(&job.id);
        let file = directory.join("manifest.json");
        let original = std::fs::read(&file).unwrap();
        let value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        let fresh = StatePaths::resolve(Some(&temp.path().join("fresh"))).unwrap();
        state::initialize(&fresh, "test").unwrap();
        for mutate in 0..6 {
            let mut bad = value.clone();
            match mutate {
                0 => bad["format_version"] = 999.into(),
                1 => bad["objects"][0]["object"]["key"] = "../outside.parquet".into(),
                2 => bad["rows"] = 100.into(),
                3 => bad["upper_key"] = serde_json::Value::Null,
                4 => bad["source_deleted"] = true.into(),
                _ => bad["plan"]["columns"][0]["nullable"] = true.into(),
            }
            std::fs::write(&file, serde_json::to_vec(&bad).unwrap()).unwrap();
            assert!(recovery::import(&fresh, &directory, None).is_err());
            assert!(store::job_list(&fresh).unwrap().is_empty());
        }
        std::fs::write(&file, &original).unwrap();
        std::fs::write(directory.join("batch-00000001.parquet"), b"damaged").unwrap();
        assert!(recovery::verify_directory(&directory, None).is_err());
        assert!(recovery::import(&fresh, &directory, None).is_err());
        assert!(store::job_list(&fresh).unwrap().is_empty());
    }

    #[tokio::test]
    async fn standalone_recovery_handles_empty_archives_and_rejects_partial_jobs() {
        use crate::archive::recovery;
        let (_temp, paths, job) = fixture();
        let directory = job.plan.destination_path.join(&job.id);
        pending(&paths, &job);
        assert!(recovery::verify_directory(&directory, None).is_err());
        // Remove the synthetic pending record/file before exercising an empty export.
        let conn = store::open(&paths, true).unwrap();
        conn.execute("DELETE FROM archive_objects WHERE job_id=?1", [&job.id])
            .unwrap();
        conn.execute(
            "UPDATE archive_checkpoints SET upper_key=NULL WHERE job_id=?1",
            [&job.id],
        )
        .unwrap();
        execute(&paths, &job.id, &mut reader()).await.unwrap();
        let verified = recovery::verify_directory(&directory, None).unwrap();
        assert!(verified.verified);
        assert_eq!(verified.rows, 0);
        assert_eq!(verified.objects, 0);
    }

    #[test]
    fn staging_dry_run_excludes_references_and_refuses_active_workers() {
        use crate::archive::recovery;
        let (_temp, paths, job) = fixture();
        let referenced = pending(&paths, &job);
        let directory = job.plan.destination_path.join(&job.id);
        let orphan = directory.join(format!(".staged-{}.parquet", uuid::Uuid::new_v4()));
        std::fs::write(&orphan, b"orphan").unwrap();
        std::fs::write(directory.join("unrelated.txt"), b"keep").unwrap();
        let guard = journal::lock(&paths, &job.id).unwrap();
        assert!(recovery::staging_candidates(&paths, &job.id).is_err());
        drop(guard);
        let report = recovery::staging_candidates(&paths, &job.id).unwrap();
        assert!(report.dry_run);
        assert_eq!(report.candidates, vec![orphan.clone()]);
        assert!(orphan.exists());
        assert!(
            job.plan
                .destination_path
                .join(referenced.staged_key)
                .exists()
        );
    }

    #[tokio::test]
    async fn v7_upgrade_preserves_original_verification_basis() {
        let (_temp, paths, job) = fixture();
        execute(&paths, &job.id, &mut reader()).await.unwrap();
        let conn = rusqlite::Connection::open(paths.database()).unwrap();
        conn.execute_batch("ALTER TABLE jobs DROP COLUMN imported_root; DELETE FROM schema_migrations WHERE version=8;").unwrap();
        assert!(store::job_show(&paths, &job.id).is_err());
        state::initialize(&paths, "test").unwrap();
        let upgraded = store::job_show(&paths, &job.id).unwrap();
        assert!(!upgraded.imported);
        assert_eq!(upgraded.storage_root, job.plan.destination_path);
        assert_eq!(
            verifier::verify(&paths, &job.id).unwrap().integrity_basis,
            "existing_local_journal"
        );
    }
    #[tokio::test]
    async fn progress_reports_only_committed_rows_and_finishes_with_saved_totals() {
        let (_temp, paths, job) = fixture();
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = events.clone();
        let observer: Observer = std::sync::Arc::new(move |_, progress| {
            captured.lock().unwrap().push(progress.clone());
        });
        let complete = execute_observed(
            &paths,
            &job.id,
            &mut reader(),
            &job.execution,
            &Shutdown::default(),
            Instant::now(),
            Some(observer),
        )
        .await
        .unwrap();
        let events = events.lock().unwrap();
        let first_publish = events
            .iter()
            .find(|p| p.stage == Stage::Publishing)
            .unwrap();
        assert_eq!(first_publish.rows_committed, 0);
        assert!(
            events
                .iter()
                .any(|p| p.stage == Stage::Reading && p.rows_committed == 2)
        );
        let final_progress = complete.progress.unwrap();
        assert_eq!(final_progress.stage, Stage::Completed);
        assert_eq!(final_progress.rows_committed, complete.rows_processed);
        assert_eq!(final_progress.bytes_committed, complete.bytes_written);
        assert!(final_progress.recent_rows_per_second.is_finite());
        assert!(final_progress.recent_rows_per_second > 0.0);
        assert!(
            events
                .windows(2)
                .all(|p| p[0].elapsed_seconds <= p[1].elapsed_seconds)
        );
    }

    struct FailingReader;
    impl ArchiveSource for FailingReader {
        async fn upper_key(&mut self, _: &ArchivePlan) -> Result<Option<i64>, Error> {
            unreachable!()
        }
        async fn read_batch(
            &mut self,
            _: &ArchivePlan,
            _: Option<i64>,
            _: i64,
        ) -> Result<Option<DataBatch>, Error> {
            Err(Error::Filesystem {
                path: "PRIVATE_ROW_OR_PASSWORD".into(),
                source: std::io::Error::other("PRIVATE_ROW_OR_PASSWORD"),
            })
        }
    }
    #[tokio::test]
    async fn failure_is_safe_persisted_and_cleared_on_successful_retry() {
        let (_temp, paths, job) = fixture();
        let error = execute(&paths, &job.id, &mut FailingReader)
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("PRIVATE_ROW_OR_PASSWORD"));
        let failed = store::job_show(&paths, &job.id).unwrap();
        let json = serde_json::to_string(&failed).unwrap();
        assert!(!json.contains("PRIVATE_ROW_OR_PASSWORD"));
        assert_eq!(failed.status, store::JobStatus::Failed);
        assert_eq!(failed.progress.unwrap().stage, Stage::Failed);
        let failure = failed.failure.unwrap();
        assert_eq!(failure.category, crate::error::FailureCategory::Storage);
        assert!(!failure.retryable);
        assert!(!failure.next_step.is_empty());
        journal::restart(&paths, &job.id).unwrap();
        let completed = execute(&paths, &job.id, &mut reader()).await.unwrap();
        assert!(completed.failure.is_none());
        assert!(completed.error.is_none());
        assert_eq!(completed.rows_processed, 3);
    }

    #[tokio::test]
    async fn populated_v6_upgrade_preserves_archive_and_starts_without_diagnostics() {
        let (_temp, paths, job) = fixture();
        execute(&paths, &job.id, &mut reader()).await.unwrap();
        let conn = rusqlite::Connection::open(paths.database()).unwrap();
        conn.execute_batch("ALTER TABLE jobs DROP COLUMN imported_root; ALTER TABLE jobs DROP COLUMN progress_json; ALTER TABLE jobs DROP COLUMN failure_json; DELETE FROM schema_migrations WHERE version>=7;").unwrap();
        assert!(store::job_show(&paths, &job.id).is_err());
        state::initialize(&paths, "test").unwrap();
        let upgraded = store::job_show(&paths, &job.id).unwrap();
        assert!(upgraded.progress.is_none());
        assert!(upgraded.failure.is_none());
        assert_eq!(upgraded.rows_processed, 3);
        assert!(verifier::verify(&paths, &job.id).unwrap().verified);
    }
    #[tokio::test]
    async fn exact_row_budget_pauses_and_resumes_without_gaps() {
        let (_temp, paths, job) = fixture();
        let controls = ExecutionControls {
            max_rows: Some(1),
            ..Default::default()
        };
        journal::execution(&paths, &job.id, &controls).unwrap();
        for expected in 1..=3 {
            if expected > 1 {
                journal::restart(&paths, &job.id).unwrap();
            }
            let mut source = reader();
            let stopped = execute(&paths, &job.id, &mut source).await.unwrap();
            assert_eq!(stopped.status, store::JobStatus::Paused);
            assert_eq!(stopped.rows_processed, expected);
            assert_eq!(source.calls.len(), 1);
            assert_eq!(stopped.execution.max_rows, Some(1));
            assert!(
                !job.plan
                    .destination_path
                    .join(&job.id)
                    .join("manifest.json")
                    .exists()
            );
        }
        journal::restart(&paths, &job.id).unwrap();
        let completed = execute(&paths, &job.id, &mut reader()).await.unwrap();
        assert_eq!(completed.status, store::JobStatus::Completed);
        assert_eq!(completed.rows_processed, 3);
        verifier::verify(&paths, &job.id).unwrap();
    }
    #[tokio::test]
    async fn shutdown_during_read_finishes_only_the_inflight_batch() {
        struct Interrupting {
            inner: Reader,
            shutdown: Shutdown,
        }
        impl ArchiveSource for Interrupting {
            async fn upper_key(&mut self, _: &ArchivePlan) -> Result<Option<i64>, Error> {
                unreachable!()
            }
            async fn read_batch(
                &mut self,
                plan: &ArchivePlan,
                last: Option<i64>,
                upper: i64,
            ) -> Result<Option<DataBatch>, Error> {
                let result = self.inner.read_batch(plan, last, upper).await;
                self.shutdown.request();
                result
            }
        }
        let (_temp, paths, job) = fixture();
        let shutdown = Shutdown::default();
        let mut source = Interrupting {
            inner: reader(),
            shutdown: shutdown.clone(),
        };
        let stopped = execute_controlled(
            &paths,
            &job.id,
            &mut source,
            &ExecutionControls::default(),
            &shutdown,
            Instant::now(),
        )
        .await
        .unwrap();
        assert_eq!(stopped.status, store::JobStatus::Paused);
        assert_eq!(stopped.rows_processed, 2);
        assert_eq!(source.inner.calls.len(), 1);
        assert_eq!(stopped.error.as_deref(), Some("shutdown requested"));
        journal::restart(&paths, &job.id).unwrap();
        execute(&paths, &job.id, &mut reader()).await.unwrap();
        verifier::verify(&paths, &job.id).unwrap();
    }

    #[tokio::test]
    async fn time_shutdown_space_and_pending_budget_stop_safely() {
        let (_temp, paths, job) = fixture();
        let mut source = reader();
        let controls = ExecutionControls {
            max_duration_seconds: Some(1),
            ..Default::default()
        };
        let stopped = execute_controlled(
            &paths,
            &job.id,
            &mut source,
            &controls,
            &Shutdown::default(),
            Instant::now() - Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(stopped.status, store::JobStatus::Paused);
        assert!(source.calls.is_empty());
        journal::restart(&paths, &job.id).unwrap();
        let shutdown = Shutdown::default();
        shutdown.request();
        execute_controlled(
            &paths,
            &job.id,
            &mut source,
            &ExecutionControls::default(),
            &shutdown,
            Instant::now(),
        )
        .await
        .unwrap();
        assert!(source.calls.is_empty());
        journal::restart(&paths, &job.id).unwrap();
        let controls = ExecutionControls {
            minimum_free_bytes: u64::MAX,
            ..Default::default()
        };
        execute_controlled(
            &paths,
            &job.id,
            &mut source,
            &controls,
            &Shutdown::default(),
            Instant::now(),
        )
        .await
        .unwrap();
        assert!(source.calls.is_empty());
        journal::restart(&paths, &job.id).unwrap();
        pending(&paths, &job);
        let controls = ExecutionControls {
            max_rows: Some(1),
            ..Default::default()
        };
        let stopped = execute_controlled(
            &paths,
            &job.id,
            &mut source,
            &controls,
            &Shutdown::default(),
            Instant::now(),
        )
        .await
        .unwrap();
        assert_eq!(stopped.rows_processed, 0);
        assert_eq!(stopped.status, store::JobStatus::Paused);
        assert!(source.calls.is_empty());
        journal::restart(&paths, &job.id).unwrap();
        execute(&paths, &job.id, &mut source).await.unwrap();
        verifier::verify(&paths, &job.id).unwrap();
    }

    #[tokio::test]
    async fn batch_delay_is_applied_and_populated_v5_upgrade_preserves_archive() {
        let (_temp, paths, job) = fixture();
        let controls = ExecutionControls {
            batch_delay_ms: 60,
            ..Default::default()
        };
        let start = Instant::now();
        execute_controlled(
            &paths,
            &job.id,
            &mut reader(),
            &controls,
            &Shutdown::default(),
            start,
        )
        .await
        .unwrap();
        assert!(start.elapsed() >= Duration::from_millis(120));
        let conn = rusqlite::Connection::open(paths.database()).unwrap();
        conn.execute_batch("ALTER TABLE jobs DROP COLUMN imported_root; ALTER TABLE jobs DROP COLUMN progress_json; ALTER TABLE jobs DROP COLUMN failure_json; ALTER TABLE jobs DROP COLUMN execution_json; DELETE FROM schema_migrations WHERE version>=6;").unwrap();
        state::initialize(&paths, "upgrade").unwrap();
        verifier::verify(&paths, &job.id).unwrap();
        assert_eq!(store::job_show(&paths, &job.id).unwrap().rows_processed, 3);
        let violations: i64 = conn
            .query_row("SELECT count(*) FROM pragma_foreign_key_check", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(violations, 0);
    }
    #[test]
    fn policy_lock_and_repeat_guards_cover_all_prior_states() {
        let (_temp, paths, job) = fixture();
        let guard = journal::policy_lock(&paths, &job.plan.policy.id).unwrap();
        assert!(journal::policy_lock(&paths, &job.plan.policy.id).is_err());
        assert!(journal::policy_lock(&paths, &uuid::Uuid::new_v4().to_string()).is_ok());
        assert!(check_prior_runs(&paths, &job.plan.policy.id, true).is_err());
        drop(guard);
        assert!(journal::policy_lock(&paths, &job.plan.policy.id).is_ok());
        store::job_finish(&paths, &job.id, Some("test failure")).unwrap();
        assert!(check_prior_runs(&paths, &job.plan.policy.id, false).is_err());
        assert!(check_prior_runs(&paths, &job.plan.policy.id, true).is_ok());
        journal::restart(&paths, &job.id).unwrap();
        store::job_finish(&paths, &job.id, None).unwrap();
        assert!(check_prior_runs(&paths, &job.plan.policy.id, false).is_err());
        assert!(check_prior_runs(&paths, &job.plan.policy.id, true).is_ok());
    }

    #[tokio::test]
    async fn resume_requires_confirmation_before_modifying_job() {
        let (_temp, paths, job) = fixture();
        store::job_finish(&paths, &job.id, Some("original failure")).unwrap();
        let error = resume(&paths, &job.id, false).await.unwrap_err();
        assert!(error.to_string().contains("--confirm-original-source"));
        let after = store::job_show(&paths, &job.id).unwrap();
        assert_eq!(after.status, store::JobStatus::Failed);
        assert_eq!(after.error.as_deref(), Some("original failure"));
    }
    #[test]
    fn migration_preserves_legacy_jobs_and_explains_recovery_limit() {
        let (_temp, paths, job) = fixture();
        store::job_finish(&paths, &job.id, None).unwrap();
        let installation = state::status(&paths).unwrap();
        let conn = rusqlite::Connection::open(paths.database()).unwrap();
        conn.execute_batch("DROP TABLE archive_objects; DROP TABLE archive_checkpoints; ALTER TABLE jobs RENAME TO saved_jobs;").unwrap();
        let old = include_str!("../../migrations/0004_archive.sql");
        let start = old.find("CREATE TABLE jobs").unwrap();
        conn.execute_batch(&old[start..]).unwrap();
        conn.execute_batch("INSERT INTO jobs SELECT id,policy_name,plan_json,status,started_at,completed_at,rows_processed,bytes_written,objects_created,last_key,error FROM saved_jobs; DROP TABLE saved_jobs; DELETE FROM schema_migrations WHERE version>=5;").unwrap();
        state::initialize(&paths, "upgraded").unwrap();
        assert_eq!(state::status(&paths).unwrap(), installation);
        let preserved = store::job_show(&paths, &job.id).unwrap();
        assert_eq!(preserved.status, store::JobStatus::Completed);
        assert_eq!(preserved.plan.cutoff_utc, job.plan.cutoff_utc);
        assert!(
            verifier::verify(&paths, &job.id)
                .unwrap_err()
                .to_string()
                .contains("legacy")
        );
    }
    #[test]
    fn subprocess_lock_helper() {
        let Ok(directory) = std::env::var("COLDCTL_INTERNAL_LOCK_TEST") else {
            return;
        };
        let id = std::env::var("COLDCTL_INTERNAL_LOCK_ID").unwrap();
        let paths = StatePaths::resolve(Some(std::path::Path::new(&directory))).unwrap();
        let _guard = journal::lock(&paths, &id).unwrap();
        std::fs::write(paths.data_dir.join("lock-ready"), b"ready").unwrap();
        std::thread::sleep(std::time::Duration::from_secs(30));
    }
    #[test]
    fn killed_worker_releases_lock_for_another_process() {
        let (_temp, paths, job) = fixture();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "archive::executor::tests::subprocess_lock_helper",
            ])
            .env("COLDCTL_INTERNAL_LOCK_TEST", &paths.data_dir)
            .env("COLDCTL_INTERNAL_LOCK_ID", &job.id)
            .stdout(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let ready = paths.data_dir.join("lock-ready");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !ready.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let was_ready = ready.exists();
        let was_locked = journal::lock(&paths, &job.id).is_err();
        let _ = child.kill();
        child.wait().unwrap();
        assert!(was_ready && was_locked);
        assert!(journal::lock(&paths, &job.id).is_ok());
    }
    fn pending(paths: &StatePaths, job: &store::Job) -> journal::Entry {
        let dest = LocalDestination::new(job.plan.destination_path.clone()).unwrap();
        let batch = DataBatch {
            rows: vec![vec![Some("1".into())], vec![Some("2".into())]],
            last_key: 2,
        };
        let file = parquet::encode(&job.plan.columns, &batch).unwrap();
        let staged_key = format!("{}/.staged-{}.parquet", job.id, uuid::Uuid::new_v4());
        let obj = dest.put_file(&staged_key, file.path()).unwrap();
        let entry = journal::Entry {
            batch: ManifestObject {
                object: StoredObject {
                    key: format!("{}/batch-00000001.parquet", job.id),
                    bytes: obj.bytes,
                    sha256: obj.sha256,
                },
                rows: 2,
                last_key: 2,
            },
            staged_key,
        };
        journal::prepare(paths, &job.id, 1, &entry).unwrap();
        entry
    }
    #[tokio::test]
    async fn recovers_prepared_and_published_batches_without_rereading_or_duplicates() {
        for already_published in [false, true] {
            let (_temp, paths, job) = fixture();
            let entry = pending(&paths, &job);
            let dest = LocalDestination::new(job.plan.destination_path.clone()).unwrap();
            if already_published {
                publish(
                    &dest,
                    &entry.batch.object,
                    &dest.object_path(&entry.staged_key).unwrap(),
                )
                .unwrap();
                std::fs::remove_file(dest.object_path(&entry.staged_key).unwrap()).unwrap();
            }
            let mut input = reader();
            let result = execute(&paths, &job.id, &mut input).await.unwrap();
            assert_eq!(result.status, store::JobStatus::Completed);
            assert_eq!(result.rows_processed, 3);
            assert_eq!(result.objects_created, 2);
            assert_eq!(input.calls, [(Some(2), 3), (Some(3), 3)]);
            verifier::verify(&paths, &job.id).unwrap();
            assert!(
                store::job_show(&paths, &job.id)
                    .unwrap()
                    .verified_at
                    .is_some()
            );
        }
    }
    #[tokio::test]
    async fn recovers_manifest_publication_without_reading_source() {
        for already_published in [false, true] {
            let (_temp, paths, job) = fixture();
            let entry = pending(&paths, &job);
            let dest = LocalDestination::new(job.plan.destination_path.clone()).unwrap();
            publish(
                &dest,
                &entry.batch.object,
                &dest.object_path(&entry.staged_key).unwrap(),
            )
            .unwrap();
            journal::commit(&paths, &job.id, 1, &entry).unwrap();
            let file = manifest_file(&paths, &job.id, &job.plan, Some(3)).unwrap();
            let (bytes, sha256) = fingerprint(file.path()).unwrap();
            let object = StoredObject {
                key: format!("{}/manifest.json", job.id),
                bytes,
                sha256,
            };
            journal::manifest(&paths, &job.id, &object).unwrap();
            if already_published {
                publish(&dest, &object, file.path()).unwrap();
            }
            let mut input = reader();
            execute(&paths, &job.id, &mut input).await.unwrap();
            assert!(input.calls.is_empty());
            verifier::verify(&paths, &job.id).unwrap();
        }
    }
    #[tokio::test]
    async fn cancellation_commits_inflight_batch_and_resume_keeps_bounds() {
        let (_temp, paths, job) = fixture();
        let guard = journal::lock(&paths, &job.id).unwrap();
        assert!(journal::lock(&paths, &job.id).is_err());
        let mut input = Reader {
            calls: vec![],
            cancel: Some((
                StatePaths::resolve(Some(&paths.data_dir)).unwrap(),
                job.id.clone(),
            )),
        };
        let stopped = execute(&paths, &job.id, &mut input).await.unwrap();
        assert_eq!(stopped.status, store::JobStatus::Cancelled);
        assert_eq!(stopped.rows_processed, 2);
        assert!(
            !job.plan
                .destination_path
                .join(&job.id)
                .join("manifest.json")
                .exists()
        );
        drop(guard);
        let _new_guard = journal::lock(&paths, &job.id).unwrap();
        journal::restart(&paths, &job.id).unwrap();
        let mut input = reader();
        let completed = execute(&paths, &job.id, &mut input).await.unwrap();
        assert_eq!(completed.rows_processed, 3);
        assert_eq!(input.calls[0], (Some(2), 3));
    }
    #[tokio::test]
    async fn corruption_is_not_overwritten_and_failed_verification_revokes_timestamp() {
        let (_temp, paths, job) = fixture();
        execute(&paths, &job.id, &mut reader()).await.unwrap();
        verifier::verify(&paths, &job.id).unwrap();
        let file = job
            .plan
            .destination_path
            .join(&job.id)
            .join("batch-00000001.parquet");
        std::fs::write(&file, b"damaged").unwrap();
        assert!(verifier::verify(&paths, &job.id).is_err());
        let failed = store::job_show(&paths, &job.id).unwrap();
        assert_eq!(failed.status, store::JobStatus::Completed);
        assert_eq!(
            failed.failure.unwrap().category,
            crate::error::FailureCategory::Corruption
        );
        assert_eq!(failed.progress.unwrap().stage, Stage::VerificationFailed);
        assert!(
            store::job_show(&paths, &job.id)
                .unwrap()
                .verified_at
                .is_none()
        );
        assert!(resume(&paths, &job.id, false).await.is_err());
        assert_eq!(std::fs::read(file).unwrap(), b"damaged");
    }
    #[tokio::test]
    async fn missing_pending_files_fail_instead_of_skipping_rows() {
        let (_temp, paths, job) = fixture();
        let entry = pending(&paths, &job);
        std::fs::remove_file(job.plan.destination_path.join(entry.staged_key)).unwrap();
        let mut input = reader();
        assert!(execute(&paths, &job.id, &mut input).await.is_err());
        assert!(input.calls.is_empty());
        let failed = store::job_show(&paths, &job.id).unwrap();
        assert_eq!(failed.status, store::JobStatus::Failed);
        assert_eq!(failed.rows_processed, 0);
    }
}
