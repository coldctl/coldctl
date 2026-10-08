//! MySQL DDL implicitly commits. Setup uses deterministic ownership markers and
//! reconciles only empty owned tables; data and progress always commit together.
use crate::{
    mysql::{self, bad, err, q, table},
    mysql_archive::{key_type, schema},
};
use coldctl_core::{
    error::Error,
    source::{
        config::ResolvedConnection,
        connector::{RestoreProgress, RestoreSpec},
        mysql_types,
    },
};
use mysql_async::{Conn, Params, Row, Value, prelude::Queryable};
use sha2::{Digest, Sha256};
pub struct RestoreSession {
    conn: Conn,
    spec: RestoreSpec,
    journal: String,
    target: String,
    target_id: u64,
    saved: RestoreProgress,
    in_transaction: bool,
}
fn marker(spec: &RestoreSpec) -> String {
    format!("coldctl:{}", spec.manifest_hash)
}
async fn comment(conn: &mut Conn, db: &str, name: &str) -> Result<Option<String>, Error> {
    conn.exec_first(
        "SELECT TABLE_COMMENT FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_NAME=?",
        (db, name),
    )
    .await
    .map_err(err)
}
impl RestoreSession {
    pub async fn open(c: ResolvedConnection, spec: RestoreSpec) -> Result<Self, Error> {
        mysql::name(&spec.schema)?;
        mysql::name(&spec.table)?;
        if spec.schema != c.database
            || spec
                .plan
                .connector_pin
                .as_ref()
                .is_none_or(|p| p.id != "mysql")
            || spec
                .plan
                .safety
                .as_ref()
                .is_none_or(|s| s.source_identity.database_name == c.database)
            || !coldctl_core::connectors::model::digest(&spec.manifest_hash)
            || spec.objects_created < 0
            || spec.rows_processed < 0
            || spec.table.starts_with("_coldctl_")
        {
            return Err(bad());
        }
        let mut conn = mysql::connect(&c, false).await?;
        let target = table(&spec.schema, &spec.table);
        let name = format!(
            "_coldctl_restore_{:x}",
            Sha256::digest(format!("{}:{}", spec.schema, spec.table))
        );
        let name = &name[..58];
        let journal = table(&spec.schema, name);
        let lock: Option<u8> = conn
            .exec_first("SELECT GET_LOCK(?,0)", (name,))
            .await
            .map_err(err)?;
        if lock != Some(1) {
            return Err(Error::Archive("MySQL restore target is busy"));
        }
        let owner = marker(&spec);
        let journal_comment = comment(&mut conn, &spec.schema, name).await?;
        let target_comment = comment(&mut conn, &spec.schema, &spec.table).await?;
        if journal_comment.as_ref().is_some_and(|s| s != &owner)
            || target_comment.as_ref().is_some_and(|s| s != &owner)
        {
            return Err(Error::Archive(
                "MySQL target or journal already exists and is not owned by this archive",
            ));
        }
        if spec.validate_only && (journal_comment.is_none() || target_comment.is_none()) {
            return Err(bad());
        }
        if journal_comment.is_none() {
            // Refuse adoption when ownership evidence has been lost.
            if target_comment.is_some() {
                return Err(bad());
            }
            conn.query_drop(format!("CREATE TABLE {journal}(id TINYINT PRIMARY KEY, batches BIGINT NOT NULL, row_count BIGINT NOT NULL, completed BOOLEAN NOT NULL, target_id BIGINT UNSIGNED NULL) ENGINE=InnoDB COMMENT='{owner}'")).await.map_err(err)?;
        }
        let exists: Option<u8> = conn
            .query_first(format!("SELECT id FROM {journal} WHERE id=1"))
            .await
            .map_err(err)?;
        if exists.is_none() {
            if target_comment.is_some() || spec.validate_only {
                return Err(bad());
            }
            conn.query_drop(format!("INSERT INTO {journal} VALUES(1,0,0,0,NULL)"))
                .await
                .map_err(err)?;
        }
        if target_comment.is_none() {
            let state: Option<(i64, i64, u8, Option<u64>)> = conn
                .query_first(format!(
                    "SELECT batches,row_count,completed,target_id FROM {journal} WHERE id=1"
                ))
                .await
                .map_err(err)?;
            if state != Some((0, 0, 0, None)) {
                return Err(bad());
            }
            let mut cols = Vec::new();
            for col in &spec.plan.columns {
                let (ty, collation) = mysql_types::parts(&col.postgres_type)?;
                let charset = if collation.is_empty() {
                    String::new()
                } else {
                    format!(" CHARACTER SET utf8mb4 COLLATE {}", q(collation))
                };
                cols.push(format!(
                    "{} {ty}{charset} {}",
                    q(&col.name),
                    if col.nullable { "NULL" } else { "NOT NULL" }
                ));
            }
            cols.push(format!("PRIMARY KEY ({})", q(&spec.plan.primary_key)));
            conn.query_drop(format!(
                "CREATE TABLE {target}({}) ENGINE=InnoDB COMMENT='{owner}'",
                cols.join(",")
            ))
            .await
            .map_err(err)?;
        }
        let (columns, key, id) = schema(&mut conn, &spec.schema, &spec.table).await?;
        if columns != spec.plan.columns || key != spec.plan.primary_key {
            return Err(bad());
        }
        let recorded: Option<Option<u64>> = conn
            .query_first(format!("SELECT target_id FROM {journal} WHERE id=1"))
            .await
            .map_err(err)?;
        match recorded {
            Some(Some(saved)) if saved == id => {}
            Some(None) if !spec.validate_only => {
                let count: u64 = conn
                    .query_first(format!("SELECT COUNT(*) FROM {target}"))
                    .await
                    .map_err(err)?
                    .ok_or_else(bad)?;
                if count != 0 {
                    return Err(bad());
                }
                conn.exec_drop(format!("UPDATE {journal} SET target_id=? WHERE id=1 AND batches=0 AND row_count=0 AND completed=0"),(id,)).await.map_err(err)?;
                if conn.affected_rows() != 1 {
                    return Err(bad());
                }
            }
            _ => return Err(bad()),
        }
        Ok(Self {
            conn,
            spec,
            journal,
            target,
            target_id: id,
            saved: RestoreProgress {
                batches: 0,
                rows: 0,
                completed: false,
            },
            in_transaction: false,
        })
    }
    pub async fn begin(&mut self, verify_count: bool) -> Result<RestoreProgress, Error> {
        if self.in_transaction {
            return Err(bad());
        }
        self.conn
            .query_drop("START TRANSACTION")
            .await
            .map_err(err)?;
        self.in_transaction = true;
        let (batches, rows, completed, target_id): (i64, i64, u8, u64) = self
            .conn
            .query_first(format!(
                "SELECT batches,row_count,completed,target_id FROM {} WHERE id=1 FOR UPDATE",
                self.journal
            ))
            .await
            .map_err(err)?
            .ok_or_else(bad)?;
        if target_id != self.target_id
            || batches < 0
            || rows < 0
            || batches > self.spec.objects_created
            || rows > self.spec.rows_processed
            || completed > 1
        {
            return Err(bad());
        }
        if verify_count {
            let count: u64 = self
                .conn
                .query_first(format!("SELECT COUNT(*) FROM {}", self.target))
                .await
                .map_err(err)?
                .ok_or_else(bad)?;
            if count != rows as u64 {
                return Err(bad());
            }
        }
        let (columns, key, id) =
            schema(&mut self.conn, &self.spec.schema, &self.spec.table).await?;
        if id != target_id || columns != self.spec.plan.columns || key != self.spec.plan.primary_key
        {
            return Err(bad());
        }
        self.saved = RestoreProgress {
            batches,
            rows,
            completed: completed == 1,
        };
        Ok(RestoreProgress {
            batches,
            rows,
            completed: completed == 1,
        })
    }
    fn values(&self, rows: &[Vec<Option<String>>]) -> Result<Vec<Vec<Value>>, Error> {
        if rows.is_empty() || rows.len() > 1000 {
            return Err(bad());
        }
        let mut total = 0usize;
        rows.iter()
            .map(|row| {
                if row.len() != self.spec.plan.columns.len() {
                    return Err(bad());
                }
                let size = row.iter().flatten().map(String::len).sum::<usize>();
                total = total.saturating_add(size);
                if size > 65536 || total > 32 * 1024 * 1024 {
                    return Err(bad());
                }
                row.iter()
                    .zip(&self.spec.plan.columns)
                    .map(|(v, c)| match v {
                        None if c.nullable => Ok(Value::NULL),
                        None => Err(bad()),
                        Some(s) if mysql_types::integer(&c.postgres_type) => {
                            if mysql_types::parts(&c.postgres_type)?
                                .0
                                .ends_with(" unsigned")
                            {
                                Ok(Value::UInt(s.parse().map_err(|_| bad())?))
                            } else {
                                Ok(Value::Int(s.parse().map_err(|_| bad())?))
                            }
                        }
                        Some(s) if mysql_types::hex_value(&c.postgres_type) => {
                            Ok(Value::Bytes(mysql::unhex(s)?))
                        }
                        Some(s) => Ok(Value::Bytes(s.as_bytes().to_vec())),
                    })
                    .collect()
            })
            .collect()
    }
    pub async fn validate(&mut self, rows: &[Vec<Option<String>>]) -> Result<(), Error> {
        if !self.in_transaction {
            return Err(bad());
        }
        let values = self.values(rows)?;
        let key_index = self
            .spec
            .plan
            .columns
            .iter()
            .position(|c| c.name == self.spec.plan.primary_key)
            .ok_or_else(bad)?;
        let keys = values
            .into_iter()
            .map(|r| r[key_index].clone())
            .collect::<Vec<_>>();
        let byte_size = self
            .spec
            .plan
            .columns
            .iter()
            .map(|c| format!("COALESCE(OCTET_LENGTH({}),0)", q(&c.name)))
            .collect::<Vec<_>>()
            .join("+");
        let sql = format!(
            "SELECT {} FROM {} WHERE {} IN ({}) AND ({byte_size})<=30000 ORDER BY {}",
            self.spec
                .plan
                .columns
                .iter()
                .map(|c| q(&c.name))
                .collect::<Vec<_>>()
                .join(","),
            self.target,
            q(&self.spec.plan.primary_key),
            vec!["?"; keys.len()].join(","),
            q(&self.spec.plan.primary_key)
        );
        let actual: Vec<Row> = self
            .conn
            .exec(sql, Params::Positional(keys))
            .await
            .map_err(err)?;
        let actual = actual
            .into_iter()
            .map(|r| mysql::encode(r, &self.spec.plan.columns))
            .collect::<Result<Vec<_>, _>>()?;
        if actual != rows {
            return Err(Error::Archive("MySQL restore values differ from archive"));
        }
        Ok(())
    }
    pub async fn apply(
        &mut self,
        sequence: i64,
        rows: &[Vec<Option<String>>],
    ) -> Result<(), Error> {
        if !self.in_transaction
            || self.spec.validate_only
            || self.saved.completed
            || sequence != self.saved.batches + 1
            || sequence > self.spec.objects_created
        {
            return Err(bad());
        }
        let values = self.values(rows)?;
        let key_index = self
            .spec
            .plan
            .columns
            .iter()
            .position(|c| c.name == self.spec.plan.primary_key)
            .ok_or_else(bad)?;
        let key_type = key_type(&self.spec.plan)?;
        let mut previous = None;
        for row in rows {
            let key = mysql_types::cursor(key_type, row[key_index].as_deref().ok_or_else(bad)?)?;
            if previous.is_some_and(|p| p >= key) {
                return Err(bad());
            }
            previous = Some(key);
        }
        let sql = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            self.target,
            self.spec
                .plan
                .columns
                .iter()
                .map(|c| q(&c.name))
                .collect::<Vec<_>>()
                .join(","),
            vec!["?"; self.spec.plan.columns.len()].join(",")
        );
        self.conn
            .exec_batch(sql, values.into_iter().map(Params::Positional))
            .await
            .map_err(err)?;
        let count = self
            .saved
            .rows
            .checked_add(rows.len() as i64)
            .ok_or_else(bad)?;
        if count > self.spec.rows_processed {
            return Err(bad());
        }
        // Read back before committing: rejects silent coercion/truncation or float/time drift.
        self.validate(rows).await?;
        self.conn
            .exec_drop(
                format!(
                    "UPDATE {} SET batches=?,row_count=? WHERE id=1",
                    self.journal
                ),
                (sequence, count),
            )
            .await
            .map_err(err)?;
        self.conn.query_drop("COMMIT").await.map_err(err)?;
        self.in_transaction = false;
        Ok(())
    }
    pub async fn finish(&mut self, completed: bool) -> Result<(), Error> {
        if !self.in_transaction {
            return Err(bad());
        }
        if completed
            && (self.saved.batches != self.spec.objects_created
                || self.saved.rows != self.spec.rows_processed)
        {
            return Err(bad());
        }
        if !self.spec.validate_only && completed {
            self.conn
                .query_drop(format!(
                    "UPDATE {} SET completed=1 WHERE id=1",
                    self.journal
                ))
                .await
                .map_err(err)?;
        }
        self.conn.query_drop("COMMIT").await.map_err(err)?;
        self.in_transaction = false;
        Ok(())
    }
}
