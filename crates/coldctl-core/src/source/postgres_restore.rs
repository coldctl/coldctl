//! Restore orchestration. Files stay in the host; native transactions stay in the connector.
use super::connector::{Request, RestoreProgress, RestoreSpec, launch_in_state};
use crate::{
    archive::{checkpoint, controls::Shutdown, verifier},
    destination::{ArchiveDestination, local::LocalDestination},
    error::Error,
    paths::StatePaths,
    state::{archive as store, sources},
};
use arrow_array::{
    Array, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
pub type Observer = std::sync::Arc<dyn Fn(&str, i64, i64) + Send + Sync>;

pub struct Options {
    pub target: String,
    pub schema: String,
    pub table: String,
    pub confirm_separate_target: bool,
    pub max_batches: Option<u64>,
    pub shutdown: Shutdown,
    pub validate_only: bool,
    pub progress: Option<Observer>,
}
#[derive(Debug, Serialize)]
pub struct Report {
    pub job_id: String,
    pub status: &'static str,
    pub rows: i64,
    pub batches: i64,
    pub values_validated: bool,
    pub integrity_basis: &'static str,
}
fn bad() -> Error {
    Error::Archive(
        "restore validation failed; archive/target values, schema or checkpoint differ; row values are omitted",
    )
}
fn identifier(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 63 || value.chars().any(char::is_control) {
        return Err(Error::Archive(
            "restore identifiers must be nonempty, at most 63 UTF-8 bytes, without control characters",
        ));
    }
    Ok(())
}

pub async fn run(paths: &StatePaths, id: &str, options: Options) -> Result<Report, Error> {
    if !options.validate_only && !options.confirm_separate_target {
        return Err(Error::Archive(
            "restore requires --confirm-separate-target and a dedicated target database with no external writers",
        ));
    }
    identifier(&options.schema)?;
    identifier(&options.table)?;
    if options.schema.starts_with("pg_")
        || options.schema == "information_schema"
        || options.table.starts_with("_coldctl_restore_")
        || options.max_batches == Some(0)
    {
        return Err(Error::Archive(
            "invalid restore schema/table or zero batch limit",
        ));
    }
    // Hold the job lock through verification and restore. Archive files must also
    // remain immutable against writers outside this state directory.
    let _guard = checkpoint::lock(paths, id)?;
    if let Some(report) = &options.progress {
        report("verifying_archive", 0, 0);
    }
    verifier::verify_observed(paths, id, None)?;
    let job = store::job_show(paths, id)?;
    if options.target == job.plan.policy.source {
        return Err(Error::Archive(
            "restore target must be separate from the archive source",
        ));
    }
    let source = sources::show(paths, &options.target)?;
    if source.connection.engine()
        != job
            .plan
            .connector_pin
            .as_ref()
            .map_or("postgres", |p| p.id.as_str())
    {
        return Err(Error::Archive(
            "restore requires a target of the same database engine",
        ));
    }
    let resolved = source.connection.resolve()?;
    if job
        .plan
        .safety
        .as_ref()
        .is_some_and(|s| s.source_identity.database_name == resolved.database)
    {
        return Err(Error::Archive(
            "restore target database name must differ from the recorded source database",
        ));
    }

    let mut client = launch_in_state(&source.connection, paths, None).await?;
    let spec = RestoreSpec {
        plan: job.plan.clone(),
        objects_created: job.objects_created,
        rows_processed: job.rows_processed,
        schema: options.schema.clone(),
        table: options.table.clone(),
        manifest_hash: checkpoint::load(paths, id)?
            .manifest
            .ok_or_else(bad)?
            .sha256,
        validate_only: options.validate_only,
    };
    client
        .call::<_, ()>(Request::RestoreOpen(spec), 120_000, !options.validate_only)
        .await?;
    let mut invocation_batches = 0u64;
    let mut initial = true;
    loop {
        let saved: RestoreProgress = client
            .call(
                Request::RestoreBegin {
                    verify_count: initial
                        || options.validate_only
                        || options.shutdown.requested()
                        || options.max_batches.is_some_and(|n| invocation_batches >= n),
                },
                120_000,
                false,
            )
            .await?;
        if saved.batches < 0
            || saved.batches > job.objects_created
            || saved.rows < 0
            || saved.rows > job.rows_processed
            || (saved.completed && saved.batches != job.objects_created)
        {
            return Err(bad());
        }
        let done = saved.batches == job.objects_created;
        if initial || done || options.validate_only {
            let mut count = 0i64;
            for sequence in 1..=saved.batches {
                if options.shutdown.requested() {
                    return Err(Error::Archive(
                        "restore validation interrupted; rerun validation",
                    ));
                }
                let rows = read_batch(paths, &job, sequence)?;
                count = count.checked_add(rows.len() as i64).ok_or_else(bad)?;
                client
                    .call::<_, ()>(Request::RestoreValidate { rows }, 120_000, false)
                    .await?;
                if let Some(report) = &options.progress {
                    report("validating", count, sequence)
                }
            }
            if count != saved.rows || (done && count != job.rows_processed) {
                return Err(bad());
            }
        }
        if done
            || options.validate_only
            || options.shutdown.requested()
            || options.max_batches.is_some_and(|n| invocation_batches >= n)
        {
            client
                .call::<_, ()>(
                    Request::RestoreFinish { completed: done },
                    120_000,
                    !options.validate_only,
                )
                .await?;
            if let Some(report) = &options.progress {
                report(
                    if done { "completed" } else { "paused" },
                    saved.rows,
                    saved.batches,
                )
            }
            return Ok(Report {
                job_id: id.into(),
                status: if done { "completed" } else { "paused" },
                rows: saved.rows,
                batches: saved.batches,
                values_validated: initial || done || options.validate_only,
                integrity_basis: if job.imported {
                    "supplied_manifest_not_authenticated"
                } else {
                    "existing_local_journal"
                },
            });
        }
        let rows = read_batch(paths, &job, saved.batches + 1)?;
        let count = rows.len() as i64;
        client
            .call::<_, ()>(
                Request::RestoreApply {
                    sequence: saved.batches + 1,
                    rows,
                },
                120_000,
                true,
            )
            .await?;
        if let Some(report) = &options.progress {
            report("committed", saved.rows + count, saved.batches + 1)
        }
        initial = false;
        invocation_batches += 1;
    }
}
type Values = Vec<Option<String>>;
fn read_batch(paths: &StatePaths, job: &store::Job, sequence: i64) -> Result<Vec<Values>, Error> {
    let (entry, committed) = checkpoint::entry(paths, &job.id, sequence)?.ok_or_else(bad)?;
    if !committed {
        return Err(bad());
    }
    let destination = LocalDestination::new(job.storage_root.clone())?;
    destination.verify(&entry.batch.object)?;
    let file = std::fs::File::open(destination.object_path(&entry.batch.object.key)?)
        .map_err(|_| bad())?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|_| bad())?
        .with_batch_size(1000)
        .build()
        .map_err(|_| bad())?;
    let mut rows = Vec::new();
    for record in reader {
        let record = record.map_err(|_| bad())?;
        for index in 0..record.num_rows() {
            let mut values = Vec::new();
            let mut bytes = 0usize;
            for array in record.columns() {
                let value = if array.is_null(index) {
                    None
                } else {
                    macro_rules! primitive {
                        ($ty:ty) => {
                            array
                                .as_any()
                                .downcast_ref::<$ty>()
                                .map(|a| a.value(index).to_string())
                        };
                    }
                    primitive!(Int16Array)
                        .or_else(|| primitive!(Int32Array))
                        .or_else(|| primitive!(Int64Array))
                        .or_else(|| primitive!(BooleanArray))
                        .or_else(|| primitive!(Float32Array))
                        .or_else(|| primitive!(Float64Array))
                        .or_else(|| {
                            array
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .map(|a| a.value(index).to_owned())
                        })
                        .ok_or_else(bad)
                        .map(Some)?
                };
                bytes = bytes
                    .checked_add(value.as_ref().map_or(0, String::len))
                    .ok_or_else(bad)?;
                if bytes > 65536 {
                    return Err(Error::Archive(
                        "restore row exceeds the 64 KiB archive limit",
                    ));
                }
                values.push(value);
            }
            rows.push(values);
            if rows.len() > 1000 {
                return Err(bad());
            }
        }
    }
    if rows.len() as i64 != entry.batch.rows {
        return Err(bad());
    }
    Ok(rows)
}
