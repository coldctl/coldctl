//! Exact FK tuples streamed from verified parent archives. Only temporary tables
//! are written; their lifetime is the supervised scan connection.
use crate::{
    mysql::{self, bad, err, q, table},
    mysql_archive::MysqlArchive,
};
use coldctl_core::{
    archive::{batch::ArchiveColumn, planner::ArchivePlan},
    error::Error,
    source::mysql_types,
};
use mysql_async::{Params, Value, prelude::Queryable};

pub struct Selection {
    contract: String,
    tables: Vec<String>,
    columns: Vec<Vec<ArchiveColumn>>,
    expected: Vec<i64>,
    received: Vec<i64>,
    ready: bool,
}
impl MysqlArchive {
    pub async fn selection_begin(&mut self, plan: &ArchivePlan) -> Result<(), Error> {
        if self.selection.is_some()
            || plan.parent_selection.is_empty()
            || plan.parent_selection.len() > 64
        {
            return Err(bad());
        }
        self.validate_resume(plan).await?;
        let visible = mysql::relationships(&mut self.conn, &self.database).await?;
        let token: String = self
            .conn
            .query_first("SELECT REPLACE(UUID(),'-','')")
            .await
            .map_err(err)?
            .ok_or_else(bad)?;
        mysql::name(&token)?;
        let mut state = Selection {
            contract: serde_json::to_string(&plan.parent_selection).map_err(|_| bad())?,
            tables: vec![],
            columns: vec![],
            expected: vec![],
            received: vec![],
            ready: false,
        };
        for (i, parent) in plan.parent_selection.iter().enumerate() {
            let fk = &parent.relationship;
            if fk.child_schema != self.database
                || fk.child_table != plan.policy.config.table
                || fk.child_columns.is_empty()
                || fk.child_columns.len() > 16
                || fk.child_columns.len() != fk.parent_columns.len()
                || parent.rows < 0
                || !visible.foreign_keys.contains(fk)
            {
                return Err(Error::Archive(
                    "parent selection relationship changed or is not visible",
                ));
            }
            let columns = fk
                .child_columns
                .iter()
                .map(|n| {
                    plan.columns
                        .iter()
                        .find(|c| &c.name == n)
                        .cloned()
                        .ok_or_else(bad)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let name = format!("coldctl_sel_{token}_{i}");
            let index = (0..columns.len())
                .map(|n| format!("k{n}"))
                .collect::<Vec<_>>()
                .join(",");
            let select = columns
                .iter()
                .enumerate()
                .map(|(n, c)| format!("{} AS k{n}", q(&c.name)))
                .collect::<Vec<_>>()
                .join(",");
            // MySQL refuses temporary DDL in a read-only transaction. Scope the
            // writable mode to this fixed TEMPORARY statement; restore read-only
            // even on creation failure. Aiven also requires a primary key.
            self.conn
                .query_drop("SET SESSION TRANSACTION READ WRITE")
                .await
                .map_err(err)?;
            let created = self.conn.query_drop(format!("CREATE TEMPORARY TABLE {} (_coldctl_row BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,INDEX ({index})) ENGINE=InnoDB AS SELECT {select} FROM {} WHERE 0",q(&name),table(&self.database,&fk.child_table))).await;
            let readonly = self
                .conn
                .query_drop("SET SESSION TRANSACTION READ ONLY")
                .await;
            readonly.map_err(err)?;
            created.map_err(err)?;
            state.tables.push(name);
            state.columns.push(columns);
            state.expected.push(parent.rows);
            state.received.push(0);
        }
        self.selection = Some(state);
        Ok(())
    }
    pub async fn selection_rows(
        &mut self,
        index: usize,
        rows: Vec<Vec<Option<String>>>,
    ) -> Result<(), Error> {
        let state = self.selection.as_mut().ok_or_else(bad)?;
        if state.ready || index >= state.tables.len() || rows.is_empty() || rows.len() > 400 {
            return Err(bad());
        }
        if state.received[index] + rows.len() as i64 > state.expected[index] {
            return Err(bad());
        }
        let columns = &state.columns[index];
        let mut values = Vec::new();
        let mut count = 0;
        for row in &rows {
            if row.len() != columns.len()
                || row.iter().flatten().map(String::len).sum::<usize>() > 65536
            {
                return Err(bad());
            }
            // NULL FK tuples cannot reference a parent, including composite keys.
            if row.iter().any(Option::is_none) {
                continue;
            }
            for (value, column) in row.iter().zip(columns) {
                let value = value.as_deref().ok_or_else(bad)?;
                values.push(if mysql_types::integer(&column.postgres_type) {
                    if mysql_types::parts(&column.postgres_type)?
                        .0
                        .ends_with(" unsigned")
                    {
                        Value::UInt(value.parse().map_err(|_| bad())?)
                    } else {
                        Value::Int(value.parse().map_err(|_| bad())?)
                    }
                } else if mysql_types::hex_value(&column.postgres_type) {
                    Value::Bytes(mysql::unhex(value)?)
                } else {
                    Value::Bytes(value.as_bytes().to_vec())
                });
            }
            count += 1;
        }
        if count > 0 {
            let tuple = format!("({})", vec!["?"; columns.len()].join(","));
            self.conn
                .exec_drop(
                    format!(
                        "INSERT INTO {} ({}) VALUES {}",
                        q(&state.tables[index]),
                        (0..columns.len())
                            .map(|n| format!("k{n}"))
                            .collect::<Vec<_>>()
                            .join(","),
                        vec![tuple; count].join(",")
                    ),
                    Params::Positional(values),
                )
                .await
                .map_err(err)?;
        }
        state.received[index] += rows.len() as i64;
        Ok(())
    }
    pub fn selection_finish(&mut self) -> Result<(), Error> {
        let state = self.selection.as_mut().ok_or_else(bad)?;
        if state.ready || state.expected != state.received {
            return Err(bad());
        }
        state.ready = true;
        Ok(())
    }
    pub fn selection_predicate(&self, plan: &ArchivePlan) -> Result<(String, Vec<Value>), Error> {
        if plan.parent_selection.is_empty() {
            if plan
                .warnings
                .iter()
                .any(|w| w.starts_with("Automatic child selection:"))
            {
                return Err(Error::Archive(
                    "automatic child selection metadata missing; use a compatible agent",
                ));
            }
            return Ok(crate::mysql_archive::predicate(plan));
        }
        let state = self.selection.as_ref().ok_or_else(bad)?;
        if !state.ready
            || state.contract != serde_json::to_string(&plan.parent_selection).map_err(|_| bad())?
        {
            return Err(bad());
        }
        let outer = table(&self.database, &plan.policy.config.table);
        let predicates = state
            .tables
            .iter()
            .zip(&plan.parent_selection)
            .map(|(temp, parent)| {
                let condition = parent
                    .relationship
                    .child_columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("s.k{i}={outer}.{}", q(c)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("EXISTS (SELECT 1 FROM {} s WHERE {condition})", q(temp))
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        Ok((format!("({predicates})"), vec![]))
    }
}
