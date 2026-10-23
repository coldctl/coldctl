//! Verified source deletion is separate from the immutable archive manifest.
use crate::{
    archive::{checkpoint, controls::Shutdown, verifier},
    error::Error,
    paths::StatePaths,
    source::{
        connector::{DeleteBatchResult, DeleteSpec, Request, launch_in_state},
        postgres_restore::read_batch,
    },
    state::{archive as jobs, deletion as store, sources},
};

pub struct Options {
    pub confirm_delete: bool,
    pub confirm_original_source: bool,
    pub max_batches: Option<u64>,
    pub shutdown: Shutdown,
}

pub async fn run(paths: &StatePaths, id: &str, options: Options) -> Result<store::Progress, Error> {
    run_mode(paths, id, options, false).await
}

pub(crate) async fn run_mode(
    paths: &StatePaths,
    id: &str,
    options: Options,
    require_empty_children: bool,
) -> Result<store::Progress, Error> {
    if !options.confirm_delete || !options.confirm_original_source || options.max_batches == Some(0)
    {
        return Err(Error::Archive(
            "deletion requires --confirm-delete, --confirm-original-source and a positive batch limit if supplied",
        ));
    }
    let _job_lock = checkpoint::lock(paths, id)?;
    let job = jobs::job_show(paths, id)?;
    if job.imported || job.status != jobs::JobStatus::Completed || job.plan.safety.is_none() {
        return Err(Error::Archive(
            "deletion requires a completed original archive with its local safety contract; imported archives cannot authorize deletion",
        ));
    }
    let engine = job
        .plan
        .connector_pin
        .as_ref()
        .map_or("postgres", |p| p.id.as_str());
    if !matches!(engine, "postgres" | "mysql" | "mongodb") {
        return Err(Error::Archive(
            "archive engine does not support verified source deletion",
        ));
    }
    let _policy_lock = checkpoint::policy_lock(paths, &job.plan.policy.id)?;
    verifier::verify_observed(paths, id, None)?;
    let journal = checkpoint::load(paths, id)?;
    let manifest = journal
        .manifest
        .ok_or(Error::Archive("missing archive manifest"))?;
    crate::protection::authorize_delete(paths, &job, &manifest.sha256)?;
    crate::protection::admit(paths, &job.plan.policy.source)?;
    let mut progress = store::start(paths, id, &manifest.sha256)?;
    if progress.batches > job.objects_created
        || progress.rows_processed > job.rows_processed
        || progress.rows_processed != progress.rows_deleted + progress.rows_absent
        || (progress.completed
            && (progress.batches != job.objects_created
                || progress.rows_processed != job.rows_processed))
    {
        return Err(Error::Archive("invalid deletion checkpoint"));
    }
    if progress.completed || options.shutdown.requested() {
        return Ok(progress);
    }
    if job.objects_created == 0 {
        return store::finish_empty(paths, id);
    }
    let source = sources::show(paths, &job.plan.policy.source)?;
    if source.id != journal.source_id || source.connection.engine() != engine {
        return Err(Error::Archive(
            "original source configuration or database engine changed",
        ));
    }
    // A newer approved connector may add deletion support to old archives. The
    // recorded source identity and schema are checked by that connector per batch.
    crate::protection::admit(paths, &job.plan.policy.source)?;
    let mut client = launch_in_state(&source.connection, paths, None).await?;
    if !client.hello.features.contains("verified-delete-v1") {
        return Err(Error::Archive(
            "connector does not support verified deletion; install an updated connector for this engine",
        ));
    }
    if require_empty_children
        && !client
            .hello
            .features
            .contains("mysql-empty-children-delete-v1")
        && !client
            .hello
            .features
            .contains("postgres-empty-children-delete-v1")
    {
        return Err(Error::Archive(
            "connector does not support coordinated child guards",
        ));
    }
    client
        .call::<_, ()>(
            Request::DeleteOpen(DeleteSpec {
                plan: job.plan.clone(),
                source_identity: journal.source_identity,
                require_empty_children,
            }),
            30_000,
            false,
        )
        .await?;
    let mut invocation = 0;
    while progress.batches < job.objects_created {
        if options.shutdown.requested() || options.max_batches.is_some_and(|n| invocation >= n) {
            break;
        }
        if let Err(error) =
            crate::protection::boundary(paths, &job.plan.policy.source, &options.shutdown).await
        {
            if crate::protection::is_pause(&error) {
                break;
            }
            return Err(error);
        }
        let sequence = progress.batches + 1;
        let rows = read_batch(paths, &job, sequence)?;
        let count = rows.len() as u64;
        if sequence == job.objects_created
            && progress.rows_processed + count as i64 != job.rows_processed
        {
            return Err(Error::Archive("deletion row checkpoint mismatch"));
        }
        // Archive I/O can outlive a telemetry sample; check again before source mutation.
        if let Err(error) = crate::protection::admit(paths, &job.plan.policy.source) {
            if crate::protection::is_pause(&error) {
                break;
            }
            return Err(error);
        }
        let result: DeleteBatchResult = client
            .call(Request::DeleteBatch { rows }, 120_000, true)
            .await?;
        if result.deleted > count
            || result.absent > count
            || result.deleted + result.absent != count
        {
            return Err(Error::Archive(
                "connector returned invalid deletion counts; replay the same batch after inspection",
            ));
        }
        progress = store::advance(
            paths,
            id,
            progress.batches,
            result.deleted as i64,
            result.absent as i64,
            sequence == job.objects_created,
        )?;
        invocation += 1;
    }
    Ok(progress)
}
