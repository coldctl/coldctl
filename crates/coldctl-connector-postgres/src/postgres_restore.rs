//! PostgreSQL owns target transactions and journal semantics, never archive files/state.
use super::{
    postgres::{PostgresSource, Session, postgres_error},
    postgres_archive::quote_identifier as quote,
};
use coldctl_core::{
    error::Error,
    source::{
        config::ResolvedConnection,
        connector::{RestoreProgress, RestoreSpec},
    },
};
use tokio_postgres::{GenericClient, types::ToSql};
type Values = Vec<Option<String>>;
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

pub struct RestoreSession {
    session: Session,
    target: Target,
    job: RestoreSpec,
    active: bool,
    progress: Option<RestoreProgress>,
}
impl RestoreSession {
    pub async fn open(connection: ResolvedConnection, options: RestoreSpec) -> Result<Self, Error> {
        identifier(&options.schema)?;
        identifier(&options.table)?;
        if options.schema.starts_with("pg_")
            || options.schema == "information_schema"
            || options.table.starts_with("_coldctl_restore_")
            || options.manifest_hash.len() != 64
            || !options.manifest_hash.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(bad());
        }
        if options
            .plan
            .safety
            .as_ref()
            .is_some_and(|s| s.source_identity.database_name == connection.database)
        {
            return Err(Error::Archive(
                "restore target database name must differ from source",
            ));
        }
        let job = &options;
        for column in &job.plan.columns {
            identifier(&column.name)?;
            kind(&column.postgres_type)?;
        }
        let connection = PostgresSource::new(connection);
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
            manifest_hash: options.manifest_hash.clone(),
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

        Ok(Self {
            session,
            target,
            job: options,
            active: false,
            progress: None,
        })
    }
    pub async fn begin(&mut self, verify_count: bool) -> Result<RestoreProgress, Error> {
        if self.active {
            return Err(bad());
        }
        let client = &self.session.client;
        client
            .batch_execute(if self.job.validate_only {
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"
            } else {
                "BEGIN"
            })
            .await
            .map_err(pg)?;
        self.active = true;
        client
            .batch_execute(&format!(
                "LOCK TABLE {}, {} IN {} MODE",
                self.target.journal,
                self.target.relation,
                if self.job.validate_only {
                    "ACCESS SHARE"
                } else {
                    "EXCLUSIVE"
                }
            ))
            .await
            .map_err(pg)?;
        let row=client.query_opt(&format!("SELECT manifest_hash,table_oid,batches,rows,completed FROM {} WHERE singleton=true",self.target.journal),&[]).await.map_err(pg)?.ok_or_else(bad)?;
        let hash: String = row.try_get(0).map_err(|_| bad())?;
        let saved_oid: u32 = row.try_get(1).map_err(|_| bad())?;
        let batches: i64 = row.try_get(2).map_err(|_| bad())?;
        let rows: i64 = row.try_get(3).map_err(|_| bad())?;
        let completed: bool = row.try_get(4).map_err(|_| bad())?;
        if hash != self.target.manifest_hash
            || batches < 0
            || batches > self.job.objects_created
            || rows < 0
            || (completed && batches != self.job.objects_created)
            || oid(client, &self.job.schema, &self.job.table).await? != Some(saved_oid)
        {
            return Err(bad());
        }
        check_schema(client, saved_oid, &self.job, &self.target).await?;
        if verify_count || batches == self.job.objects_created {
            let count: i64 = client
                .query_one(
                    &format!("SELECT count(*) FROM {}", self.target.relation),
                    &[],
                )
                .await
                .map_err(pg)?
                .get(0);
            if count != rows {
                return Err(bad());
            }
        }
        self.progress = Some(RestoreProgress {
            batches,
            rows,
            completed,
        });
        Ok(RestoreProgress {
            batches,
            rows,
            completed,
        })
    }
    pub async fn validate(&self, rows: &[Values]) -> Result<(), Error> {
        if !self.active {
            return Err(bad());
        }
        self.validate_rows(rows)?;
        validate_batch(&self.job, &self.target, &self.session.client, rows).await
    }
    fn validate_rows(&self, rows: &[Values]) -> Result<(), Error> {
        if rows.is_empty() || rows.len() > 1000 {
            return Err(bad());
        }
        let mut bytes = 0usize;
        for row in rows {
            let size = row.iter().flatten().map(String::len).sum::<usize>();
            bytes = bytes.saturating_add(size);
            if row.len() != self.job.plan.columns.len()
                || size > 65536
                || bytes > 32 * 1024 * 1024
                || row
                    .iter()
                    .zip(&self.job.plan.columns)
                    .any(|(v, c)| v.is_none() && !c.nullable)
            {
                return Err(bad());
            }
        }
        Ok(())
    }
    pub async fn apply(&mut self, sequence: i64, batch: &[Values]) -> Result<(), Error> {
        if !self.active || self.job.validate_only {
            return Err(bad());
        }
        self.validate_rows(batch)?;
        let saved = self.progress.as_ref().ok_or_else(bad)?;
        if sequence != saved.batches + 1 || sequence > self.job.objects_created {
            return Err(bad());
        }
        let columns = self
            .job
            .plan
            .columns
            .iter()
            .map(|c| quote(&c.name))
            .collect::<Vec<_>>()
            .join(",");
        let params = self
            .target
            .types
            .iter()
            .enumerate()
            .map(|(i, t)| format!("${}::text::pg_catalog.{t}", i + 1))
            .collect::<Vec<_>>()
            .join(",");
        let client = &self.session.client;
        let statement = client
            .prepare(&format!(
                "INSERT INTO {} ({columns}) VALUES ({params})",
                self.target.relation
            ))
            .await
            .map_err(pg)?;
        for row in batch {
            let values: Vec<&(dyn ToSql + Sync)> =
                row.iter().map(|v| v as &(dyn ToSql + Sync)).collect();
            client.execute(&statement, &values).await.map_err(pg)?;
        }
        validate_batch(&self.job, &self.target, client, batch).await?;
        let rows = saved.rows.checked_add(batch.len() as i64).ok_or_else(bad)?;
        if rows > self.job.rows_processed {
            return Err(bad());
        }
        client
            .execute(
                &format!(
                    "UPDATE {} SET batches=$1,rows=$2 WHERE singleton=true",
                    self.target.journal
                ),
                &[&sequence, &rows],
            )
            .await
            .map_err(pg)?;
        client.batch_execute("COMMIT").await.map_err(pg)?;
        self.active = false;
        self.progress = None;
        Ok(())
    }
    pub async fn finish(&mut self, completed: bool) -> Result<(), Error> {
        if !self.active {
            return Err(bad());
        }
        if completed {
            let saved = self.progress.as_ref().ok_or_else(bad)?;
            if saved.batches != self.job.objects_created || saved.rows != self.job.rows_processed {
                return Err(bad());
            }
            if !self.job.validate_only {
                self.session
                    .client
                    .execute(
                        &format!(
                            "UPDATE {} SET completed=true WHERE singleton=true",
                            self.target.journal
                        ),
                        &[],
                    )
                    .await
                    .map_err(pg)?;
            }
        }
        self.session
            .client
            .batch_execute("COMMIT")
            .await
            .map_err(pg)?;
        self.active = false;
        self.progress = None;
        Ok(())
    }
}
async fn check_schema(
    client: &impl GenericClient,
    oid: u32,
    job: &RestoreSpec,
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
    job: &RestoreSpec,
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
