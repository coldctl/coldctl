//! PostgreSQL-specific policy validation, estimates, and bounded keyset reads.
use super::postgres::{PostgresSource, Session, postgres_error};
use coldctl_core::{
    archive::{
        batch::{ArchiveColumn, DataBatch},
        planner::ArchivePlan,
    },
    error::Error,
    policy::model::Policy,
};
use std::{path::PathBuf, time::Duration};
use tokio_postgres::{GenericClient, types::ToSql};

pub struct PostgresArchive {
    session: Session,
    operation_timeout: Duration,
}

pub fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
fn relation(policy: &Policy) -> String {
    format!(
        "{}.{}",
        quote_identifier(&policy.config.schema),
        quote_identifier(&policy.config.table)
    )
}
fn predicate(plan: &ArchivePlan) -> String {
    let equality = match &plan.policy.config.equals_column {
        Some(column) => format!("{}::text = $2::text", quote_identifier(column)),
        None => "$2::text IS NULL".into(),
    };
    format!(
        "{} < $1::text::timestamptz AND {equality}",
        quote_identifier(&plan.policy.config.time_column)
    )
}
pub(crate) fn supported(kind: &str) -> bool {
    matches!(
        kind,
        "int2"
            | "int4"
            | "int8"
            | "bool"
            | "float4"
            | "float8"
            | "text"
            | "varchar"
            | "bpchar"
            | "numeric"
            | "date"
            | "timestamp"
            | "timestamptz"
            | "uuid"
            | "json"
            | "jsonb"
    )
}

async fn inspect(
    client: &impl GenericClient,
    policy: &Policy,
) -> Result<(u32, String, Vec<ArchiveColumn>), Error> {
    let rows=client.query("SELECT c.oid,c.relkind::text,EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid),pg_catalog.has_table_privilege(c.oid,'SELECT') AND pg_catalog.has_schema_privilege(n.oid,'USAGE') FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2", &[&policy.config.schema,&policy.config.table]).await.map_err(|e|postgres_error("validate table",e))?;
    let row = rows
        .first()
        .ok_or(Error::Archive("policy table not found"))?;
    let oid: u32 = row.get(0);
    let rls: bool = client
        .query_one(
            "SELECT relrowsecurity OR relforcerowsecurity FROM pg_catalog.pg_class WHERE oid=$1",
            &[&oid],
        )
        .await
        .map_err(|e| postgres_error("validate row security", e))?
        .get(0);
    if rls {
        return Err(Error::Archive(
            "tables with row-level security are not supported for archival",
        ));
    }
    if row.get::<_, String>(1) != "r" || row.get::<_, bool>(2) {
        return Err(Error::Archive(
            "this archive milestone supports ordinary tables without inheritance or partitions",
        ));
    }
    if !row.get::<_, bool>(3) {
        return Err(Error::Archive(
            "source user needs schema USAGE and table SELECT",
        ));
    }
    let mut columns = Vec::new();
    for row in client.query("SELECT a.attname::text,t.typname::text,NOT a.attnotnull,t.typnamespace='pg_catalog'::regnamespace AND t.typtype='b' FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", &[&oid]).await.map_err(|e|postgres_error("validate columns",e))? {
        let kind:String=row.get(1);
        if !supported(&kind) || !row.get::<_,bool>(3) { return Err(Error::Archive("unsupported column type; supported: integers, bool, floats, text, numeric, date/timestamps, UUID, JSON/JSONB")); }
        columns.push(ArchiveColumn{name:row.get(0),postgres_type:kind,nullable:row.get(2)});
    }
    if !columns.iter().any(|c| {
        c.name == policy.config.time_column
            && matches!(
                c.postgres_type.as_str(),
                "date" | "timestamp" | "timestamptz"
            )
    }) {
        return Err(Error::Archive(
            "time column must exist and have date, timestamp, or timestamptz type",
        ));
    }
    if let Some(filter) = &policy.config.equals_column {
        if !columns.iter().any(|c| {
            c.name == *filter && matches!(c.postgres_type.as_str(), "text" | "varchar" | "bpchar")
        }) {
            return Err(Error::Archive(
                "equals-column must be a text/varchar/char column",
            ));
        }
    }
    let keys=client.query("SELECT a.attname::text,t.typname::text FROM pg_catalog.pg_index i JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum,position) ON k.position<=i.indnkeyatts JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum JOIN pg_catalog.pg_type t ON t.oid=a.atttypid WHERE i.indrelid=$1 AND i.indisprimary AND i.indisvalid", &[&oid]).await.map_err(|e|postgres_error("validate primary key",e))?;
    if keys.len() != 1
        || !matches!(
            keys[0].get::<_, String>(1).as_str(),
            "int2" | "int4" | "int8"
        )
    {
        return Err(Error::Archive(
            "this archive milestone requires a single smallint/integer/bigint primary key",
        ));
    }
    Ok((oid, keys[0].get(0), columns))
}

impl PostgresArchive {
    pub(crate) async fn configure_timeouts(
        &mut self,
        controls: &coldctl_core::archive::controls::ExecutionControls,
    ) -> Result<(), Error> {
        controls.validate()?;
        tokio::time::timeout(Duration::from_secs(10),self.session.client.query_one(
            "SELECT set_config('statement_timeout',$1,false),set_config('lock_timeout',$2,false)",
            &[&format!("{}ms",controls.query_timeout_ms),&format!("{}ms",controls.lock_timeout_ms)]))
            .await.map_err(|_|Error::Archive("setting archive timeouts timed out"))?
            .map_err(|e|postgres_error("configure archive timeouts",e))?;
        self.operation_timeout =
            Duration::from_millis(u64::from(controls.query_timeout_ms) + 5_000);
        Ok(())
    }
    pub(crate) async fn source_identity(
        &self,
        table_oid: u32,
    ) -> Result<coldctl_core::archive::planner::SourceIdentity, Error> {
        tokio::time::timeout(self.operation_timeout, async {
        let row = self.session.client.query_one(
            "SELECT oid, datname::text FROM pg_catalog.pg_database WHERE datname=current_database()", &[])
            .await
            .map_err(|e|postgres_error("identify database",e))?;
        Ok(coldctl_core::archive::planner::SourceIdentity {
            database_oid: row.get(0),
            database_name: row.get(1),
            schema_signature: schema_signature(&self.session.client, table_oid).await?,
        })
        }).await.map_err(|_|Error::Archive("source identity lookup timed out"))?
    }

    pub(crate) async fn validate_resume(&mut self, plan: &ArchivePlan) -> Result<(), Error> {
        if let Some(safety) = &plan.safety {
            if self.source_identity(plan.table_oid).await? != safety.source_identity {
                return Err(Error::Archive(
                    "source database or column identity changed; refusing resume",
                ));
            }
        }
        // Validate even if no further source rows are needed (e.g. pending publication).
        tokio::time::timeout(self.operation_timeout, async {
            let (oid, key, columns) = inspect(&self.session.client, &plan.policy).await?;
            if oid != plan.table_oid || key != plan.primary_key || columns != plan.columns {
                return Err(Error::Archive(
                    "table identity or schema changed; refusing resume",
                ));
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Archive("resume validation timed out"))?
    }
    pub(crate) fn identity(&self) -> &str {
        &self.session.identity
    }
    pub async fn connect(
        connection: coldctl_core::source::config::ResolvedConnection,
    ) -> Result<Self, Error> {
        let session = PostgresSource::new(connection).connect().await?;
        session
            .client
            .batch_execute(
                "SET search_path = pg_catalog; SET TIME ZONE 'UTC'; SET DateStyle TO ISO, YMD;",
            )
            .await
            .map_err(|e| postgres_error("configure archive session", e))?;
        Ok(Self {
            session,
            operation_timeout: Duration::from_secs(30),
        })
    }
    pub async fn plan(
        &self,
        policy: Policy,
        destination_path: PathBuf,
    ) -> Result<ArchivePlan, Error> {
        tokio::time::timeout(
            self.operation_timeout,
            self.plan_inner(policy, destination_path),
        )
        .await
        .map_err(|_| Error::Archive("archive planning exceeded the operation deadline"))?
    }
    async fn plan_inner(
        &self,
        policy: Policy,
        destination_path: PathBuf,
    ) -> Result<ArchivePlan, Error> {
        policy.config.validate()?;
        let (table_oid, primary_key, columns) = inspect(&self.session.client, &policy).await?;
        let cutoff_utc: String = self
            .session
            .client
            .query_one(
                "SELECT (CURRENT_TIMESTAMP - $1::int * INTERVAL '1 day')::text",
                &[&policy.config.older_than_days],
            )
            .await
            .map_err(|e| postgres_error("determine cutoff", e))?
            .get(0);
        let mut plan=ArchivePlan{connector_pin:None,safety:None,policy,destination_path,cutoff_utc,primary_key,columns,table_oid,estimated_rows:None,delete:false,warnings:vec!["Export uses per-batch snapshots; archive immutable cold rows. Concurrent changes can alter eligibility or values.".into(),"Numeric/date/timestamp/UUID/JSON columns are stored losslessly as UTF-8 text with PostgreSQL type metadata.".into(),"Rows larger than 64 KiB in JSON text-array form fail rather than being truncated.".into()]};
        let sql = format!(
            "EXPLAIN (FORMAT JSON) SELECT * FROM {} WHERE {}",
            relation(&plan.policy),
            predicate(&plan)
        );
        let row = self
            .session
            .client
            .query_one(&sql, &[&plan.cutoff_utc, &plan.policy.config.equals_value])
            .await
            .map_err(|e| postgres_error("estimate archive", e))?;
        let explain: serde_json::Value = row.get(0);
        plan.estimated_rows = explain[0]["Plan"]["Plan Rows"].as_i64();
        let indexed:bool=self.session.client.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class idx ON idx.oid=i.indexrelid JOIN pg_catalog.pg_am am ON am.oid=idx.relam JOIN pg_catalog.pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=i.indkey[0] WHERE i.indrelid=$1 AND a.attname=$2 AND i.indisvalid AND i.indisready AND i.indpred IS NULL AND am.amname IN ('btree','brin'))", &[&plan.table_oid,&plan.policy.config.time_column]).await.map_err(|e|postgres_error("inspect filter index",e))?.get(0);
        if !indexed {
            plan.warnings.push("No valid non-partial B-tree/BRIN index leads with the time column; review the filter and keyset query plans before a large export. Partial or composite indexes may still help.".into());
        }
        if let Some(cost) = explain[0]["Plan"]["Total Cost"].as_f64() {
            plan.warnings.push(format!(
                "Planner total cost: {cost:.2} (relative units, not seconds)."
            ));
        }
        fn sequential(node: &serde_json::Value) -> bool {
            node["Node Type"]
                .as_str()
                .is_some_and(|s| s.contains("Seq Scan"))
                || node["Plans"]
                    .as_array()
                    .is_some_and(|children| children.iter().any(sequential))
        }
        if sequential(&explain[0]["Plan"]) {
            plan.warnings.push("Planner selected a sequential scan; review filter indexes and source load before exporting.".into());
        }
        plan.warnings.push("Estimated rows and source bytes do not predict Parquet size. Free-space preflight is not a capacity reservation.".into());
        plan.warnings.push("Endpoint and database/table OIDs cannot reliably detect every database replacement. Resume requires --confirm-original-source after operator validation; stable eligibility must include no backdated inserts.".into());
        Ok(plan)
    }

    async fn read_inner(
        &mut self,
        plan: &ArchivePlan,
        last: Option<i64>,
        upper: i64,
    ) -> Result<Option<DataBatch>, Error> {
        let tx = self
            .session
            .client
            .transaction()
            .await
            .map_err(|e| postgres_error("read batch", e))?;
        // A short ACCESS SHARE lock prevents DDL between schema validation and this batch read.
        tx.batch_execute(&format!(
            "LOCK TABLE {} IN ACCESS SHARE MODE",
            relation(&plan.policy)
        ))
        .await
        .map_err(|e| postgres_error("lock archive table", e))?;
        let (oid, key, columns) = inspect(&tx, &plan.policy).await?;
        validate_signature(&tx, plan).await?;
        if oid != plan.table_oid || key != plan.primary_key || columns != plan.columns {
            return Err(Error::Archive(
                "table schema changed after planning; start a new archive job",
            ));
        }
        let values = plan
            .columns
            .iter()
            .map(|c| format!("{}::text", quote_identifier(&c.name)))
            .collect::<Vec<_>>()
            .join(",");
        let json = format!("pg_catalog.array_to_json(ARRAY[{values}])::text");
        let key = quote_identifier(&plan.primary_key);
        let sql = format!(
            "SELECT {key}::bigint,CASE WHEN pg_catalog.octet_length({json})<=65536 THEN {json} ELSE NULL END FROM {} WHERE {} AND ($3::bigint IS NULL OR {key}>$3::bigint) AND {key}<=$4::bigint ORDER BY {key} LIMIT $5::bigint",
            relation(&plan.policy),
            predicate(plan)
        );
        let size = i64::from(plan.policy.config.batch_size);
        let params: &[&(dyn ToSql + Sync)] = &[
            &plan.cutoff_utc,
            &plan.policy.config.equals_value,
            &last,
            &upper,
            &size,
        ];
        let selected = tx
            .query(&sql, params)
            .await
            .map_err(|e| postgres_error("read archive batch", e))?;
        let mut rows = Vec::new();
        let mut last_key = None;
        for row in selected {
            last_key = Some(coldctl_core::archive::key::ArchiveKey::from(
                row.get::<_, i64>(0),
            ));
            let json: Option<String> = row.get(1);
            let json = json.ok_or(Error::Archive(
                "row exceeds the 64 KiB archive limit; no truncation was performed",
            ))?;
            let values: Vec<Option<String>> = serde_json::from_str(&json)
                .map_err(|_| Error::Archive("unable to decode archive row"))?;
            if values.len() != plan.columns.len() {
                return Err(Error::Archive("archive row shape changed"));
            }
            rows.push(values);
        }
        tx.commit()
            .await
            .map_err(|e| postgres_error("read archive batch", e))?;
        Ok(last_key.map(|last_key| DataBatch { rows, last_key }))
    }
}
impl super::ArchiveSource for PostgresArchive {
    async fn upper_key(
        &mut self,
        plan: &ArchivePlan,
    ) -> Result<Option<coldctl_core::archive::key::ArchiveKey>, Error> {
        tokio::time::timeout(self.operation_timeout, async {
            let tx = self
                .session
                .client
                .transaction()
                .await
                .map_err(|e| postgres_error("determine upper key", e))?;
            tx.batch_execute(&format!(
                "LOCK TABLE {} IN ACCESS SHARE MODE",
                relation(&plan.policy)
            ))
            .await
            .map_err(|e| postgres_error("lock archive table", e))?;
            let (oid, key, columns) = inspect(&tx, &plan.policy).await?;
            validate_signature(&tx, plan).await?;
            if oid != plan.table_oid || key != plan.primary_key || columns != plan.columns {
                return Err(Error::Archive("table schema changed after planning"));
            }
            let sql = format!(
                "SELECT max({})::bigint FROM {}",
                quote_identifier(&plan.primary_key),
                relation(&plan.policy)
            );
            let result: Option<i64> = tx
                .query_one(&sql, &[])
                .await
                .map_err(|e| postgres_error("determine upper key", e))?
                .get(0);
            tx.commit()
                .await
                .map_err(|e| postgres_error("determine upper key", e))?;
            Ok(result.map(Into::into))
        })
        .await
        .map_err(|_| Error::Archive("upper-key lookup timed out"))?
    }
    async fn read_batch(
        &mut self,
        plan: &ArchivePlan,
        last: Option<coldctl_core::archive::key::ArchiveKey>,
        upper: coldctl_core::archive::key::ArchiveKey,
    ) -> Result<Option<DataBatch>, Error> {
        tokio::time::timeout(
            self.operation_timeout,
            self.read_inner(
                plan,
                last.map(|k| k.integer()).transpose()?,
                upper.integer()?,
            ),
        )
        .await
        .map_err(|_| Error::Archive("archive batch exceeded the operation deadline"))?
    }
}

async fn schema_signature(client: &impl GenericClient, oid: u32) -> Result<String, Error> {
    let row=client.query_one("SELECT COALESCE(jsonb_agg(jsonb_build_array(a.attnum,a.attname,a.atttypid::text,a.atttypmod,a.attcollation::text,a.attnotnull,a.attidentity::text,a.attgenerated::text) ORDER BY a.attnum)::text,'[]') FROM pg_catalog.pg_attribute a WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped", &[&oid]).await.map_err(|e|postgres_error("identify column definitions",e))?;
    use sha2::{Digest, Sha256};
    let definition: String = row.get(0);
    Ok(format!("{:x}", Sha256::digest(definition.as_bytes())))
}
async fn validate_signature(client: &impl GenericClient, plan: &ArchivePlan) -> Result<(), Error> {
    if let Some(safety) = &plan.safety {
        if schema_signature(client, plan.table_oid).await?
            != safety.source_identity.schema_signature
        {
            return Err(Error::Archive(
                "column definition changed after planning; refusing export",
            ));
        }
    }
    Ok(())
}
