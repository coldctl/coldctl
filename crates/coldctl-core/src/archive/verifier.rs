use super::{
    checkpoint as journal, executor,
    progress::{Observer, Reporter, Stage},
};
use crate::{
    destination::{
        ArchiveDestination,
        local::{LocalDestination, fingerprint},
    },
    error::Error,
    paths::StatePaths,
    state::archive as store,
};
use arrow_array::{Array, Int16Array, Int32Array, Int64Array};
use arrow_schema::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct Verification {
    pub job_id: String,
    pub rows: i64,
    pub objects: i64,
    pub bytes: i64,
    pub verified: bool,
    pub integrity_basis: &'static str,
}
fn invalid<T>(_: T) -> Error {
    Error::Archive("Parquet verification failed; row values are omitted from diagnostics")
}

pub fn verify(paths: &StatePaths, id: &str) -> Result<Verification, Error> {
    verify_with_progress(paths, id, None)
}
pub fn verify_with_progress(
    paths: &StatePaths,
    id: &str,
    observer: Option<Observer>,
) -> Result<Verification, Error> {
    let _guard = journal::lock(paths, id)?;
    verify_observed(paths, id, observer)
}
pub(crate) fn verify_observed(
    paths: &StatePaths,
    id: &str,
    observer: Option<Observer>,
) -> Result<Verification, Error> {
    if store::job_show(paths, id)?.status != store::JobStatus::Completed {
        return Err(Error::Archive(
            "verification requires a completed job; resume interrupted jobs first",
        ));
    }
    let mut progress = Reporter::new(paths, id, std::time::Instant::now(), observer)?;
    progress.stage(Stage::Verifying)?;
    let result = verify_contents(paths, id);
    match &result {
        Ok(_) => {
            store::job_clear_failure(paths, id)?;
            progress.stage(Stage::Verified)?;
        }
        Err(error) => {
            store::job_failure(paths, id, error)?;
            progress.stage(Stage::VerificationFailed)?;
        }
    }
    result
}
fn verify_contents(paths: &StatePaths, id: &str) -> Result<Verification, Error> {
    journal::verified(paths, id, false)?;
    let job = store::job_show(paths, id)?;
    if job.status != store::JobStatus::Completed {
        return Err(Error::Archive(
            "verification requires a completed job; resume interrupted jobs first",
        ));
    }
    let checkpoint = journal::load(paths, id)?;
    let manifest = checkpoint
        .manifest
        .ok_or(Error::Archive("missing manifest checkpoint"))?;
    let destination = LocalDestination::new(job.storage_root.clone())?;
    if job.imported {
        super::recovery::regular(&job.storage_root.join(id), true)?;
        super::recovery::regular(&destination.object_path(&manifest.key)?, false)?;
    }
    destination.verify(&manifest)?;
    let expected = executor::manifest_file(paths, id, &job.plan, checkpoint.upper)?;
    let (bytes, hash) = fingerprint(expected.path())?;
    if manifest.key != format!("{id}/manifest.json")
        || bytes != manifest.bytes
        || hash != manifest.sha256
    {
        return Err(Error::Archive("manifest does not match job checkpoints"));
    }
    verify_batches(paths, &job)?;
    journal::verified(paths, id, true)?;
    Ok(Verification {
        job_id: id.into(),
        rows: job.rows_processed,
        objects: job.objects_created,
        bytes: job.bytes_written,
        verified: true,
        integrity_basis: if job.imported {
            "supplied_manifest_not_authenticated"
        } else {
            "existing_local_journal"
        },
    })
}

pub(crate) fn verify_batches(paths: &StatePaths, job: &store::Job) -> Result<(), Error> {
    let checkpoint = journal::load(paths, &job.id)?;
    let destination = LocalDestination::new(job.storage_root.clone())?;
    let key_index = job
        .plan
        .columns
        .iter()
        .position(|c| c.name == job.plan.primary_key)
        .ok_or(Error::Archive("missing primary key column"))?;
    let mut rows = 0i64;
    let mut bytes = 0i64;
    let mut last = None;
    for sequence in 1..=job.objects_created {
        let (entry, committed) = journal::entry(paths, &job.id, sequence)?
            .ok_or(Error::Archive("missing batch checkpoint"))?;
        if !committed
            || entry.batch.object.key != format!("{}/batch-{sequence:08}.parquet", job.id)
            || entry.batch.rows <= 0
            || entry.batch.rows > i64::from(job.plan.policy.config.batch_size)
        {
            return Err(Error::Archive("invalid batch checkpoint"));
        }
        if job.imported {
            super::recovery::regular(&destination.object_path(&entry.batch.object.key)?, false)?;
        }
        destination.verify(&entry.batch.object)?;
        let file = std::fs::File::open(destination.object_path(&entry.batch.object.key)?)
            .map_err(invalid)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(invalid)?;
        if builder.schema().fields().len() != job.plan.columns.len() {
            return Err(invalid(()));
        }
        for (field, column) in builder.schema().fields().iter().zip(&job.plan.columns) {
            let kind = match column.postgres_type.as_str() {
                "int2" => DataType::Int16,
                "int4" => DataType::Int32,
                "int8" => DataType::Int64,
                "float4" => DataType::Float32,
                "float8" => DataType::Float64,
                "bool" => DataType::Boolean,
                _ => DataType::Utf8,
            };
            if field.name() != &column.name
                || field.is_nullable() != column.nullable
                || field.data_type() != &kind
                || field.metadata().get("coldctl.postgres_type") != Some(&column.postgres_type)
            {
                return Err(invalid(()));
            }
        }
        let mut count = 0i64;
        for batch in builder.with_batch_size(1000).build().map_err(invalid)? {
            let batch = batch.map_err(invalid)?;
            count += batch.num_rows() as i64;
            if count > entry.batch.rows {
                return Err(invalid(()));
            }
            let keys = batch.column(key_index);
            if keys.null_count() != 0 {
                return Err(invalid(()));
            }
            for index in 0..batch.num_rows() {
                let key = if let Some(a) = keys.as_any().downcast_ref::<Int64Array>() {
                    a.value(index)
                } else if let Some(a) = keys.as_any().downcast_ref::<Int32Array>() {
                    i64::from(a.value(index))
                } else if let Some(a) = keys.as_any().downcast_ref::<Int16Array>() {
                    i64::from(a.value(index))
                } else {
                    return Err(invalid(()));
                };
                if last.is_some_and(|v| key <= v) || checkpoint.upper.is_none_or(|v| key > v) {
                    return Err(Error::Archive(
                        "archive primary keys are out of order or bounds",
                    ));
                }
                last = Some(key);
            }
        }
        if count != entry.batch.rows || last != Some(entry.batch.last_key) {
            return Err(Error::Archive(
                "archive batch row count or boundary mismatch",
            ));
        }
        rows = rows.checked_add(count).ok_or_else(|| invalid(()))?;
        bytes = bytes
            .checked_add(i64::try_from(entry.batch.object.bytes).map_err(invalid)?)
            .ok_or_else(|| invalid(()))?;
    }
    if rows != job.rows_processed || bytes != job.bytes_written || last != job.last_key {
        return Err(Error::Archive("archive totals do not match job progress"));
    }
    Ok(())
}
