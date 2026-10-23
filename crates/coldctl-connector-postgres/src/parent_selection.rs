//! Bounded archived FK tuples in typed, indexed, connection-local temporary tables.
use crate::{
    postgres_archive::{PostgresArchive, predicate, quote_identifier as q, relation},
    relationships::{self, pg},
};
use coldctl_core::{archive::planner::ArchivePlan, error::Error};
pub struct Selection {
    contract: String,
    tables: Vec<String>,
    types: Vec<Vec<String>>,
    expected: Vec<i64>,
    received: Vec<i64>,
    ready: bool,
}
fn bad() -> Error {
    Error::Archive("invalid or incomplete PostgreSQL parent selection")
}
impl PostgresArchive {
    pub async fn selection_begin(&mut self, plan: &ArchivePlan) -> Result<(), Error> {
        if self.selection.is_some()
            || plan.parent_selection.is_empty()
            || plan.parent_selection.len() > 64
        {
            return Err(bad());
        }
        self.validate_resume(plan).await?;
        let visible = relationships::discover(&self.session.client).await?;
        let mut state = Selection {
            contract: serde_json::to_string(&plan.parent_selection).map_err(|_| bad())?,
            tables: vec![],
            types: vec![],
            expected: vec![],
            received: vec![],
            ready: false,
        };
        for (i, parent) in plan.parent_selection.iter().enumerate() {
            let fk = &parent.relationship;
            if fk.child_schema != plan.policy.config.schema
                || fk.child_table != plan.policy.config.table
                || fk.child_columns.is_empty()
                || fk.child_columns.len() > 16
                || fk.child_columns.len() != fk.parent_columns.len()
                || parent.rows < 0
                || !visible.foreign_keys.contains(fk)
            {
                return Err(bad());
            }
            relationships::validate(&self.session.client, fk).await?;
            let types = fk
                .child_columns
                .iter()
                .map(|n| {
                    plan.columns
                        .iter()
                        .find(|c| &c.name == n)
                        .map(|c| c.postgres_type.clone())
                        .ok_or_else(bad)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if types.iter().any(|t| !crate::postgres_archive::supported(t)) {
                return Err(bad());
            }
            let name = format!("coldctl_selection_{i}");
            let columns = fk
                .child_columns
                .iter()
                .enumerate()
                .map(|(n, c)| format!("{} AS k{n}", q(c)))
                .collect::<Vec<_>>()
                .join(",");
            let keys = (0..types.len())
                .map(|n| format!("k{n}"))
                .collect::<Vec<_>>()
                .join(",");
            self.session
                .client
                .batch_execute("SET default_transaction_read_only=off")
                .await
                .map_err(pg)?;
            let created=self.session.client.batch_execute(&format!("CREATE TEMP TABLE {} AS SELECT {columns} FROM {} WITH NO DATA; CREATE INDEX ON pg_temp.{} ({keys})",q(&name),relation(&plan.policy),q(&name))).await;
            let restored = self
                .session
                .client
                .batch_execute("SET default_transaction_read_only=on")
                .await;
            restored.map_err(pg)?;
            created.map_err(pg)?;
            state.tables.push(name);
            state.types.push(types);
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
        let s = self.selection.as_mut().ok_or_else(bad)?;
        if s.ready
            || index >= s.tables.len()
            || rows.is_empty()
            || rows.len() > 400
            || s.received[index] + rows.len() as i64 > s.expected[index]
        {
            return Err(bad());
        }
        if rows.iter().any(|r| {
            r.len() != s.types[index].len()
                || r.iter().flatten().map(String::len).sum::<usize>() > 65536
        }) {
            return Err(bad());
        }
        let count = rows.len() as i64;
        let rows: Vec<_> = rows
            .into_iter()
            .filter(|r| r.iter().all(Option::is_some))
            .collect();
        let values = s.types[index]
            .iter()
            .enumerate()
            .map(|(i, t)| format!("(v->>{i})::pg_catalog.{}", q(t)))
            .collect::<Vec<_>>()
            .join(",");
        let data = serde_json::to_value(rows).map_err(|_| bad())?;
        self.session.client.execute(&format!("INSERT INTO pg_temp.{} SELECT {values} FROM pg_catalog.jsonb_array_elements($1::jsonb) v",q(&s.tables[index])),&[&data]).await.map_err(pg)?;
        s.received[index] += count;
        Ok(())
    }
    pub fn selection_finish(&mut self) -> Result<(), Error> {
        let s = self.selection.as_mut().ok_or_else(bad)?;
        if s.ready || s.expected != s.received {
            return Err(bad());
        }
        s.ready = true;
        Ok(())
    }
    pub fn selection_predicate(&self, plan: &ArchivePlan) -> Result<String, Error> {
        if plan.parent_selection.is_empty() {
            if plan
                .warnings
                .iter()
                .any(|s| s.starts_with("Automatic child selection:"))
            {
                return Err(bad());
            }
            return Ok(predicate(plan));
        }
        let s = self.selection.as_ref().ok_or_else(bad)?;
        if !s.ready
            || s.contract != serde_json::to_string(&plan.parent_selection).map_err(|_| bad())?
        {
            return Err(bad());
        }
        let clauses = s
            .tables
            .iter()
            .zip(&plan.parent_selection)
            .map(|(t, p)| {
                let eq = p
                    .relationship
                    .child_columns
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("s.k{i}={}.{}", relation(&plan.policy), q(c)))
                    .collect::<Vec<_>>()
                    .join(" AND ");
                format!("EXISTS(SELECT 1 FROM pg_temp.{} s WHERE {eq})", q(t))
            })
            .collect::<Vec<_>>()
            .join(" OR ");
        // Keep the protocol's fixed parameter positions while ignoring child retention.
        Ok(format!(
            "($1::text IS NOT NULL AND ($2::text IS NULL OR $2::text IS NOT NULL) AND ({clauses}))"
        ))
    }
}
