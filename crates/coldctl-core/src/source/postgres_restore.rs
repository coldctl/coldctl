//! Explicit, target-side transactional restore. No source session enables writes.
use super::{
    postgres::{PostgresSource, postgres_error},
    postgres_archive::quote_identifier as quote,
};
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
use std::time::Duration;
use tokio_postgres::{GenericClient, types::ToSql};

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
fn pg(error: tokio_postgres::Error) -> Error {
    match error.code().map(|code| code.code()) {
        Some("42501") => Error::Postgres {
            operation: "restore target",
            reason: "permission denied; check target schema USAGE/CREATE and restore table SELECT/INSERT/UPDATE privileges",
        },
        Some("23505" | "23502" | "23514" | "22003" | "22P02") => Error::Postgres {
            operation: "restore target",
            reason: "restore rejected by a constraint or value conversion; preserve the target and journal, inspect the target schema, and retry after fixing the cause; row values are omitted",
        },
        _ => postgres_error("restore target", error),
    }
}
fn kind(value: &str) -> Result<&str, Error> {
    // Length/precision typmods were not recorded by format v2. Text retains the
    // exported bpchar representation (source ::text already removed padding).
    match value {
        "varchar" | "bpchar" => Ok("text"),
        "int2" | "int4" | "int8" | "bool" | "float4" | "float8" | "text" | "numeric" | "date"
        | "timestamp" | "timestamptz" | "uuid" | "json" | "jsonb" => Ok(value),
        _ => Err(Error::Archive("unsupported restore column type")),
    }
}
fn identifier(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 63 || value.chars().any(char::is_control) {
        return Err(Error::Archive(
            "restore identifiers must be nonempty, at most 63 UTF-8 bytes, without control characters",
        ));
    }
    Ok(())
}

struct Target {
    relation: String,
    journal: String,
    manifest_hash: String,
    types: Vec<String>,
}
async fn oid(client: &impl GenericClient, schema: &str, table: &str) -> Result<Option<u32>, Error> {
    Ok(client.query_opt("SELECT c.oid FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2", &[&schema,&table]).await.map_err(pg)?.map(|r|r.get(0)))
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
    for column in &job.plan.columns {
        identifier(&column.name)?;
        kind(&column.postgres_type)?;
    }
    let connection = PostgresSource::new(source.connection);
    let mut session = if options.validate_only {
        connection.connect().await?
    } else {
        connection.connect_restore().await?
    };
    session.client.batch_execute("SET search_path=pg_catalog; SET TIME ZONE 'UTC'; SET DateStyle='ISO, YMD'; SET extra_float_digits=3;").await.map_err(pg)?;
    use sha2::{Digest, Sha256};
    let journal_name = format!(
        "_coldctl_restore_{:x}",
        Sha256::digest(format!("{}\0{}", options.schema, options.table).as_bytes())
    );
    let journal_name = &journal_name[..49];
    let locked: bool = session
        .client
        .query_one(
            "SELECT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtextextended($1,0))",
            &[&format!(
                "coldctl-restore:{}:{}",
                options.schema, options.table
            )],
        )
        .await
        .map_err(pg)?
        .get(0);
    if !locked {
        return Err(Error::Archive(
            "restore target is busy; another restore owns it",
        ));
    }
    let target = Target {
        relation: format!("{}.{}", quote(&options.schema), quote(&options.table)),
        journal: format!("{}.{}", quote(&options.schema), quote(journal_name)),
        manifest_hash: checkpoint::load(paths, id)?
            .manifest
            .ok_or_else(bad)?
            .sha256,
        types: job
            .plan
            .columns
            .iter()
            .map(|c| kind(&c.postgres_type).map(str::to_owned))
            .collect::<Result<_, _>>()?,
    };
    let tx = session.client.transaction().await.map_err(pg)?;
    let table_oid = oid(&tx, &options.schema, &options.table).await?;
    let journal_oid = oid(&tx, &options.schema, journal_name).await?;
    match (table_oid, journal_oid) {
        (None, None) if !options.validate_only => {
            let definitions = job
                .plan
                .columns
                .iter()
                .zip(&target.types)
                .map(|(c, t)| {
                    format!(
                        "{} pg_catalog.{}{}",
                        quote(&c.name),
                        t,
                        if c.nullable { "" } else { " NOT NULL" }
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            tx.batch_execute(&format!("CREATE TABLE {} ({definitions}, PRIMARY KEY ({})); CREATE TABLE {} (singleton bool PRIMARY KEY CHECK(singleton), manifest_hash text NOT NULL, table_oid oid NOT NULL, batches bigint NOT NULL, rows bigint NOT NULL, completed bool NOT NULL);",target.relation,quote(&job.plan.primary_key),target.journal)).await.map_err(pg)?;
            let created = oid(&tx, &options.schema, &options.table)
                .await?
                .ok_or_else(bad)?;
            tx.execute(
                &format!(
                    "INSERT INTO {} VALUES (true,$1,$2,0,0,false)",
                    target.journal
                ),
                &[&target.manifest_hash, &created],
            )
            .await
            .map_err(pg)?;
        }
        (Some(_), Some(_)) => {}
        _ => {
            return Err(Error::Archive(
                "restore refuses an existing table without its matching journal, or a missing restore table; choose a new target table",
            ));
        }
    }
    // Initial DDL and empty journal commit atomically, including an empty archive.
    tx.commit().await.map_err(pg)?;
    let mut invocation_batches = 0u64;
    let mut initial = true;
    loop {
        let step = async {
            let tx = session
                .client
                .build_transaction()
                .isolation_level(if options.validate_only {
                    tokio_postgres::IsolationLevel::RepeatableRead
                } else {
                    tokio_postgres::IsolationLevel::ReadCommitted
                })
                .start()
                .await
                .map_err(pg)?;
            if !options.validate_only {
                tx.batch_execute(&format!(
                    "LOCK TABLE {}, {} IN EXCLUSIVE MODE",
                    target.journal, target.relation
                ))
                .await
                .map_err(pg)?;
            } else {
                // Repeatable-read validation gets a consistent snapshot; the lock prevents DDL.
                tx.batch_execute(&format!(
                    "LOCK TABLE {}, {} IN ACCESS SHARE MODE",
                    target.journal, target.relation
                ))
                .await
                .map_err(pg)?;
            }
            let row = tx.query_opt(&format!("SELECT manifest_hash,table_oid,batches,rows,completed FROM {} WHERE singleton=true", target.journal),&[]).await.map_err(pg)?.ok_or_else(bad)?;
            let saved_hash: String = row.try_get(0).map_err(|_| bad())?;
            let saved_oid: u32 = row.try_get(1).map_err(|_| bad())?;
            let batches: i64 = row.try_get(2).map_err(|_| bad())?;
            let rows: i64 = row.try_get(3).map_err(|_| bad())?;
            let completed: bool = row.try_get(4).map_err(|_| bad())?;
            if saved_hash != target.manifest_hash
                || batches < 0
                || batches > job.objects_created
                || rows < 0
                || (completed && batches != job.objects_created)
            {
                return Err(bad());
            }
            if oid(&tx, &options.schema, &options.table).await? != Some(saved_oid) {
                return Err(bad());
            }
            check_schema(&tx, saved_oid, &job, &target).await?;
            if initial
                || batches == job.objects_created
                || options.validate_only
                || options.max_batches.is_some_and(|n| invocation_batches >= n)
                || options.shutdown.requested()
            {
                let count: i64 = tx
                    .query_one(&format!("SELECT count(*) FROM {}", target.relation), &[])
                    .await
                    .map_err(pg)?
                    .get(0);
                if count != rows {
                    return Err(bad());
                }
            }
            if initial || batches == job.objects_created || options.validate_only {
                validate_prefix(paths, &job, &target, &tx, batches, rows, &options).await?;
            }
            let done = batches == job.objects_created;
            if done
                || options.validate_only
                || options.shutdown.requested()
                || options.max_batches.is_some_and(|n| invocation_batches >= n)
            {
                if done && !options.validate_only {
                    tx.execute(
                        &format!(
                            "UPDATE {} SET completed=true WHERE singleton=true",
                            target.journal
                        ),
                        &[],
                    )
                    .await
                    .map_err(pg)?;
                }
                tx.commit().await.map_err(pg)?;
                if let Some(report) = &options.progress {
                    report(if done { "completed" } else { "paused" }, rows, batches);
                }
                return Ok::<_, Error>(Some(Report {
                    job_id: id.into(),
                    status: if done { "completed" } else { "paused" },
                    rows,
                    batches,
                    values_validated: initial || done || options.validate_only,
                    integrity_basis: if job.imported {
                        "supplied_manifest_not_authenticated"
                    } else {
                        "existing_local_journal"
                    },
                }));
            }
            let write_batch = async {
                let batch = read_batch(paths, &job, batches + 1)?;
                let columns = job
                    .plan
                    .columns
                    .iter()
                    .map(|c| quote(&c.name))
                    .collect::<Vec<_>>()
                    .join(",");
                let params = target
                    .types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| format!("${}::text::pg_catalog.{t}", i + 1))
                    .collect::<Vec<_>>()
                    .join(",");
                let statement = tx
                    .prepare(&format!(
                        "INSERT INTO {} ({columns}) VALUES ({params})",
                        target.relation
                    ))
                    .await
                    .map_err(pg)?;
                for values in &batch {
                    let parameters: Vec<&(dyn ToSql + Sync)> =
                        values.iter().map(|v| v as &(dyn ToSql + Sync)).collect();
                    tx.execute(&statement, &parameters).await.map_err(pg)?;
                }
                validate_batch(&job, &target, &tx, &batch).await?;
                let next_rows = rows.checked_add(batch.len() as i64).ok_or_else(bad)?;
                tx.execute(
                    &format!(
                        "UPDATE {} SET batches=$1,rows=$2 WHERE singleton=true",
                        target.journal
                    ),
                    &[&(batches + 1), &next_rows],
                )
                .await
                .map_err(pg)?;
                tx.commit().await.map_err(pg)?;
                if let Some(report) = &options.progress {
                    report("committed", next_rows, batches + 1);
                }
                Ok::<_, Error>(())
            };
            tokio::time::timeout(Duration::from_secs(120),write_batch).await.map_err(|_|Error::Archive("restore batch timed out; rerun the same command to recover its committed checkpoint"))??;
            Ok(None)
        };
        let result = step.await?;
        if let Some(report) = result {
            return Ok(report);
        }
        initial = false;
        invocation_batches += 1;
    }
}

async fn check_schema(
    client: &impl GenericClient,
    oid: u32,
    job: &store::Job,
    target: &Target,
) -> Result<(), Error> {
    let relation = client.query_one("SELECT relkind::text,relpersistence::text,relrowsecurity,EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=$1),EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=$1 OR inhparent=$1) FROM pg_catalog.pg_class WHERE oid=$1", &[&oid]).await.map_err(pg)?;
    if relation.get::<_, String>(0) != "r"
        || relation.get::<_, String>(1) != "p"
        || relation.get::<_, bool>(2)
        || relation.get::<_, bool>(3)
        || relation.get::<_, bool>(4)
    {
        return Err(bad());
    }
    let keys=client.query("SELECT a.attname::text FROM pg_catalog.pg_index i JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] WHERE i.indrelid=$1 AND i.indisprimary AND i.indisvalid AND i.indnkeyatts=1 AND i.indnatts=1", &[&oid]).await.map_err(pg)?;
    if keys.len() != 1 || keys[0].get::<_, String>(0) != job.plan.primary_key {
        return Err(bad());
    }
    let columns = client.query("SELECT a.attname::text,t.typname::text,a.attnotnull,a.atttypmod,a.atthasdef,a.attidentity::text,a.attgenerated::text,t.typnamespace='pg_catalog'::regnamespace FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", &[&oid]).await.map_err(pg)?;
    if columns.len() != job.plan.columns.len() {
        return Err(bad());
    }
    for ((actual, expected), kind) in columns.iter().zip(&job.plan.columns).zip(&target.types) {
        if actual.get::<_, String>(0) != expected.name
            || actual.get::<_, String>(1) != *kind
            || actual.get::<_, bool>(2) == expected.nullable
            || actual.get::<_, i32>(3) != -1
            || actual.get::<_, bool>(4)
            || !actual.get::<_, String>(5).is_empty()
            || !actual.get::<_, String>(6).is_empty()
            || !actual.get::<_, bool>(7)
        {
            return Err(bad());
        }
    }
    Ok(())
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
fn same(kind: &str, expected: &Option<String>, actual: &Option<String>) -> bool {
    match (expected, actual) {
        (None, None) => true,
        (Some(a), Some(b)) if kind == "float4" => match (a.parse::<f32>(), b.parse::<f32>()) {
            (Ok(a), Ok(b)) => (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits(),
            _ => false,
        },
        (Some(a), Some(b)) if kind == "float8" => match (a.parse::<f64>(), b.parse::<f64>()) {
            (Ok(a), Ok(b)) => (a.is_nan() && b.is_nan()) || a.to_bits() == b.to_bits(),
            _ => false,
        },
        _ => expected == actual,
    }
}
async fn validate_batch(
    job: &store::Job,
    target: &Target,
    client: &impl GenericClient,
    batch: &[Values],
) -> Result<(), Error> {
    let key_index = job
        .plan
        .columns
        .iter()
        .position(|c| c.name == job.plan.primary_key)
        .ok_or_else(bad)?;
    let columns = job
        .plan
        .columns
        .iter()
        .map(|c| format!("{}::text", quote(&c.name)))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT {columns} FROM {} WHERE {}=$1::text::pg_catalog.{}",
        target.relation,
        quote(&job.plan.primary_key),
        target.types[key_index]
    );
    let statement = client.prepare(&sql).await.map_err(pg)?;
    for values in batch {
        let row = client
            .query_opt(&statement, &[&values[key_index]])
            .await
            .map_err(pg)?
            .ok_or_else(bad)?;
        for (index, expected) in values.iter().enumerate() {
            let actual: Option<String> = row.get(index);
            if !same(&target.types[index], expected, &actual) {
                return Err(bad());
            }
        }
    }
    Ok(())
}
async fn validate_prefix(
    paths: &StatePaths,
    job: &store::Job,
    target: &Target,
    client: &impl GenericClient,
    batches: i64,
    rows: i64,
    options: &Options,
) -> Result<(), Error> {
    let mut count = 0i64;
    for sequence in 1..=batches {
        if options.shutdown.requested() {
            return Err(Error::Archive(
                "restore validation interrupted; rerun the same command after checking the target",
            ));
        }
        let batch = read_batch(paths, job, sequence)?;
        tokio::time::timeout(
            Duration::from_secs(120),
            validate_batch(job, target, client, &batch),
        )
        .await
        .map_err(|_| Error::Archive("restore validation batch timed out; rerun validation"))??;
        count = count.checked_add(batch.len() as i64).ok_or_else(bad)?;
        if let Some(report) = &options.progress {
            report("validating", count, sequence);
        }
    }
    if count != rows || (batches == job.objects_created && count != job.rows_processed) {
        return Err(bad());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn value_comparison_keeps_nulls_precision_and_float_signs() {
        let value = |s: &str| Some(s.to_owned());
        assert!(!same("text", &None, &value("")));
        assert!(!same("json", &None, &value("null")));
        assert!(same(
            "numeric",
            &value("12345678901234567890.123456789"),
            &value("12345678901234567890.123456789")
        ));
        assert!(!same(
            "numeric",
            &value("12345678901234567890.123456789"),
            &value("12345678901234567890.123456788")
        ));
        assert!(same("float8", &value("NaN"), &value("NaN")));
        assert!(same("float4", &value("inf"), &value("Infinity")));
        assert!(!same("float8", &value("-0"), &value("0")));
        assert!(same("float4", &value("1"), &value("1.0")));
    }
    #[test]
    fn restore_types_and_identifier_limits_are_explicit() {
        assert_eq!(kind("numeric").unwrap(), "numeric");
        assert_eq!(kind("bpchar").unwrap(), "text");
        assert_eq!(kind("varchar").unwrap(), "text");
        assert!(kind("text); DROP TABLE x").is_err());
        assert!(identifier("quoted\"name").is_ok());
        assert!(identifier(&"x".repeat(64)).is_err());
        assert!(identifier(&"雪".repeat(22)).is_err());
        assert!(identifier("bad\0name").is_err());
    }
}
