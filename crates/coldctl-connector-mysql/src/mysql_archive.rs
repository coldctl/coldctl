use crate::mysql::{self, bad, err, q, table};
use coldctl_core::{
    archive::{
        batch::{ArchiveColumn, DataBatch},
        controls::ExecutionControls,
        planner::{ArchivePlan, SourceIdentity},
    },
    error::Error,
    policy::model::Policy,
    source::{config::ResolvedConnection, mysql_types},
};
use mysql_async::{Conn, Params, Row, Value, prelude::Queryable};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
pub struct MysqlArchive {
    pub conn: Conn,
    pub database: String,
    identity: String,
    plan: Option<ArchivePlan>,
}
impl MysqlArchive {
    pub async fn connect(c: ResolvedConnection) -> Result<Self, Error> {
        let mut conn = mysql::connect(&c, true).await?;
        let server: String = conn
            .query_first("SELECT @@server_uuid")
            .await
            .map_err(err)?
            .ok_or_else(bad)?;
        Ok(Self {
            conn,
            database: c.database,
            identity: server,
            plan: None,
        })
    }
    pub fn identity(&self) -> &str {
        &self.identity
    }
    pub async fn configure_timeouts(&mut self, c: &ExecutionControls) -> Result<(), Error> {
        c.validate()?;
        let seconds = c.lock_timeout_ms.div_ceil(1000);
        self.conn
            .query_drop(format!("SET SESSION lock_wait_timeout={seconds}"))
            .await
            .map_err(err)?;
        self.conn
            .query_drop(format!("SET SESSION innodb_lock_wait_timeout={seconds}"))
            .await
            .map_err(err)?;
        self.conn
            .query_drop(format!(
                "SET SESSION max_execution_time={}",
                c.query_timeout_ms
            ))
            .await
            .map_err(err)?;
        Ok(())
    }
    pub async fn plan(
        &mut self,
        policy: Policy,
        destination_path: PathBuf,
    ) -> Result<ArchivePlan, Error> {
        policy.config.validate()?;
        let c = &policy.config;
        mysql::name(&c.schema)?;
        mysql::name(&c.table)?;
        if c.schema != self.database {
            return Err(Error::Archive(
                "MySQL policy schema must match the configured database",
            ));
        }
        let (columns, key, oid) = schema(&mut self.conn, &c.schema, &c.table).await?;
        if !columns.iter().any(|col| {
            col.name == c.time_column
                && mysql_types::parts(&col.postgres_type).is_ok_and(|(t, _)| {
                    matches!(t.split('(').next(), Some("date" | "datetime" | "timestamp"))
                })
        }) {
            return Err(Error::Archive(
                "MySQL retention requires DATE, DATETIME or TIMESTAMP",
            ));
        }
        if let Some(equal) = &c.equals_column {
            if !columns.iter().any(|col| col.name == *equal) {
                return Err(bad());
            }
        }
        let cutoff_utc: String = self
            .conn
            .exec_first(
                "SELECT DATE_FORMAT(UTC_TIMESTAMP(6)-INTERVAL ? DAY,'%Y-%m-%d %H:%i:%s.%f')",
                (c.older_than_days,),
            )
            .await
            .map_err(err)?
            .ok_or_else(bad)?;
        let estimated_rows:Option<i64>=self.conn.exec_first("SELECT TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",(&c.schema,&c.table)).await.map_err(err)?.flatten();
        let plan=ArchivePlan{connector_pin:None,policy,destination_path,cutoff_utc,primary_key:key,table_oid:oid as u32,columns,estimated_rows,delete:false,warnings:vec!["MySQL 8.4 InnoDB: freeze eligible rows and membership through export/resume; no reusable snapshot is claimed. Data-only restore preserves primary key and column types, not secondary indexes, defaults or application constraints.".into()],safety:None};
        self.plan = Some(plan.clone());
        Ok(plan)
    }
    pub async fn source_identity(&mut self, oid: u32) -> Result<SourceIdentity, Error> {
        let plan = self.plan.as_ref().ok_or_else(bad)?;
        let (columns, key, table_id) = schema(
            &mut self.conn,
            &plan.policy.config.schema,
            &plan.policy.config.table,
        )
        .await?;
        if table_id as u32 != oid {
            return Err(bad());
        }
        let signature = serde_json::to_vec(&(
            &self.identity,
            table_id,
            &self.database,
            &plan.policy.config.table,
            &columns,
            key,
        ))
        .map_err(|_| bad())?;
        Ok(SourceIdentity {
            database_oid: 0,
            database_name: self.database.clone(),
            schema_signature: format!("{:x}", Sha256::digest(signature)),
        })
    }
    pub async fn validate_resume(&mut self, plan: &ArchivePlan) -> Result<(), Error> {
        if plan.delete
            || plan.policy.config.schema != self.database
            || plan.connector_pin.as_ref().is_none_or(|p| p.id != "mysql")
        {
            return Err(bad());
        }
        let (columns, key, id) =
            schema(&mut self.conn, &self.database, &plan.policy.config.table).await?;
        if columns != plan.columns || key != plan.primary_key || id as u32 != plan.table_oid {
            return Err(bad());
        }
        self.plan = Some(plan.clone());
        if let Some(safety) = &plan.safety {
            if self.source_identity(plan.table_oid).await? != safety.source_identity {
                return Err(Error::Archive("MySQL source identity or schema changed"));
            }
        }
        Ok(())
    }
    pub async fn upper_key(&mut self, plan: &ArchivePlan) -> Result<Option<i64>, Error> {
        let (predicate, params) = predicate(plan);
        let key = key_type(plan)?;
        let row: Option<Row> = self
            .conn
            .exec_first(
                format!(
                    "SELECT {} FROM {} WHERE {predicate} ORDER BY {} DESC LIMIT 1",
                    q(&plan.primary_key),
                    table(&self.database, &plan.policy.config.table),
                    q(&plan.primary_key)
                ),
                Params::Positional(params),
            )
            .await
            .map_err(err)?;
        row.map(|r| {
            let vals = mysql::encode(
                r,
                &[plan
                    .columns
                    .iter()
                    .find(|c| c.name == plan.primary_key)
                    .ok_or_else(bad)?
                    .clone()],
            )?;
            mysql_types::cursor(key, vals[0].as_deref().ok_or_else(bad)?)
        })
        .transpose()
    }
    pub async fn read_batch(
        &mut self,
        plan: &ArchivePlan,
        last: Option<i64>,
        upper: i64,
    ) -> Result<Option<DataBatch>, Error> {
        let key = key_type(plan)?;
        let (mut where_sql, mut params) = predicate(plan);
        where_sql.push_str(&format!(" AND {}<=?", q(&plan.primary_key)));
        params.push(key_parameter(key, upper)?);
        if let Some(last) = last {
            where_sql.push_str(&format!(" AND {}>?", q(&plan.primary_key)));
            params.push(key_parameter(key, last)?);
        }
        // A first bounded query returns only keys and sizes. Never fetch an oversized cell.
        let bytes = plan
            .columns
            .iter()
            .map(|c| format!("COALESCE(OCTET_LENGTH({}),0)", q(&c.name)))
            .collect::<Vec<_>>()
            .join("+");
        let limit = plan.policy.config.batch_size.min(400);
        let query = format!(
            "SELECT {},({bytes}) FROM {} WHERE {where_sql} ORDER BY {} LIMIT {limit}",
            q(&plan.primary_key),
            table(&self.database, &plan.policy.config.table),
            q(&plan.primary_key)
        );
        let keys: Vec<Row> = self
            .conn
            .exec(query, Params::Positional(params))
            .await
            .map_err(err)?;
        if keys.is_empty() {
            return Ok(None);
        }
        let mut values = Vec::new();
        for row in keys {
            let mut r = row.unwrap();
            let size = r.pop().ok_or_else(bad)?;
            let n = mysql_async::from_value_opt::<u64>(size).map_err(|_| bad())?;
            if n > 30000 {
                return Err(Error::Archive("MySQL row exceeds supported byte limit"));
            }
            values.push(r.pop().ok_or_else(bad)?);
        }
        let query = format!(
            "SELECT {} FROM {} WHERE {} IN ({}) AND ({bytes})<=30000 ORDER BY {}",
            plan.columns
                .iter()
                .map(|c| q(&c.name))
                .collect::<Vec<_>>()
                .join(","),
            table(&self.database, &plan.policy.config.table),
            q(&plan.primary_key),
            vec!["?"; values.len()].join(","),
            q(&plan.primary_key)
        );
        let expected = values.len();
        let rows: Vec<Row> = self
            .conn
            .exec(query, Params::Positional(values))
            .await
            .map_err(err)?;
        if rows.len() != expected {
            return Err(Error::Archive(
                "MySQL source changed or row exceeded byte limit",
            ));
        }
        let rows = rows
            .into_iter()
            .map(|r| mysql::encode(r, &plan.columns))
            .collect::<Result<Vec<_>, _>>()?;
        let index = plan
            .columns
            .iter()
            .position(|c| c.name == plan.primary_key)
            .ok_or_else(bad)?;
        let last_key = mysql_types::cursor(
            key,
            rows.last().ok_or_else(bad)?[index]
                .as_deref()
                .ok_or_else(bad)?,
        )?;
        Ok(Some(DataBatch {
            rows,
            last_key: last_key.into(),
        }))
    }
}
pub fn key_type(plan: &ArchivePlan) -> Result<&str, Error> {
    plan.columns
        .iter()
        .find(|c| c.name == plan.primary_key)
        .map(|c| c.postgres_type.as_str())
        .ok_or_else(bad)
}
fn predicate(plan: &ArchivePlan) -> (String, Vec<Value>) {
    let mut sql = format!("{} < ?", q(&plan.policy.config.time_column));
    let mut values = vec![Value::from(plan.cutoff_utc.clone())];
    if let (Some(col), Some(value)) = (
        &plan.policy.config.equals_column,
        &plan.policy.config.equals_value,
    ) {
        sql.push_str(&format!(" AND {}=?", q(col)));
        values.push(value.clone().into());
    }
    (sql, values)
}
pub async fn table_id(conn: &mut Conn, schema: &str, name: &str) -> Result<u64, Error> {
    conn.exec_first(
        "SELECT TABLE_ID FROM information_schema.innodb_tables WHERE NAME=?",
        (format!("{schema}/{name}"),),
    )
    .await
    .map_err(err)?
    .ok_or_else(bad)
}
pub async fn schema(
    conn: &mut Conn,
    db: &str,
    name: &str,
) -> Result<(Vec<ArchiveColumn>, String, u64), Error> {
    mysql::name(db)?;
    mysql::name(name)?;
    let engine:Option<String>=conn.exec_first("SELECT ENGINE FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND TABLE_TYPE='BASE TABLE'",(db,name)).await.map_err(err)?;
    if engine.as_deref() != Some("InnoDB") {
        return Err(Error::Archive(
            "MySQL archive requires an InnoDB base table",
        ));
    }
    let partition:Option<u8>=conn.exec_first("SELECT 1 FROM information_schema.partitions WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND PARTITION_NAME IS NOT NULL LIMIT 1",(db,name)).await.map_err(err)?;
    if partition.is_some() {
        return Err(Error::Archive(
            "partitioned MySQL tables are not yet qualified",
        ));
    }
    let rows:Vec<(String,String,String,String,Option<String>,String)>=conn.exec("SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,EXTRA,COLLATION_NAME,COALESCE(CHARACTER_SET_NAME,'') FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION LIMIT 1601",(db,name)).await.map_err(err)?;
    if rows.is_empty() || rows.len() > 1600 {
        return Err(bad());
    }
    let mut columns = Vec::new();
    for (name, ty, null, extra, collation, charset) in rows {
        if extra.contains("GENERATED")
            || extra.contains("INVISIBLE")
            || (!charset.is_empty() && charset != "utf8mb4" && charset != "binary")
        {
            return Err(Error::Archive(
                "unsupported MySQL generated/invisible column or character set",
            ));
        }
        let postgres_type = format!("mysql:{ty}|{}", collation.unwrap_or_default());
        mysql_types::parts(&postgres_type)?;
        columns.push(ArchiveColumn {
            name,
            postgres_type,
            nullable: null == "YES",
        });
    }
    let keys:Vec<String>=conn.exec("SELECT COLUMN_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX",(db,name)).await.map_err(err)?;
    if keys.len() != 1
        || !columns
            .iter()
            .any(|c| c.name == keys[0] && !c.nullable && mysql_types::integer(&c.postgres_type))
    {
        return Err(Error::Archive(
            "MySQL archive requires one signed or unsigned integer primary key",
        ));
    }
    Ok((columns, keys[0].clone(), table_id(conn, db, name).await?))
}

fn key_parameter(kind: &str, token: i64) -> Result<Value, Error> {
    let value = mysql_types::key_value(kind, token);
    if mysql_types::parts(kind)?.0.ends_with(" unsigned") {
        Ok(Value::UInt(value.parse().map_err(|_| bad())?))
    } else {
        Ok(Value::Int(value.parse().map_err(|_| bad())?))
    }
}
