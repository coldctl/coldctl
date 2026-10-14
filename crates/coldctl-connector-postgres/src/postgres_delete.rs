//! Compare and delete only supplied archived keys, atomically per batch.
use crate::{
    postgres::{PostgresSource, Session, postgres_error},
    postgres_archive::{inspect, quote_identifier as quote, validate_signature},
};
use coldctl_core::{
    error::Error,
    source::{
        config::ResolvedConnection,
        connector::{DeleteBatchResult, DeleteSpec},
    },
};

pub struct DeleteSession {
    session: Session,
    spec: DeleteSpec,
}
fn pg(error: tokio_postgres::Error) -> Error {
    postgres_error("delete archived rows", error)
}
fn changed() -> Error {
    Error::Archive("source row differs from verified archive; deletion batch rolled back")
}

fn same(kind: &str, expected: &Option<String>, actual: &Option<String>) -> bool {
    if kind == "bool" {
        let normalize = |v: &Option<String>| match v.as_deref() {
            Some("true" | "t") => Some(Some(true)),
            Some("false" | "f") => Some(Some(false)),
            None => Some(None),
            _ => None,
        };
        let a = normalize(expected);
        return a.is_some() && a == normalize(actual);
    }
    crate::postgres_restore::same(kind, expected, actual)
}

impl DeleteSession {
    pub async fn open(connection: ResolvedConnection, spec: DeleteSpec) -> Result<Self, Error> {
        spec.plan.policy.config.validate()?;
        if spec.plan.safety.is_none() || spec.plan.delete {
            return Err(Error::Archive(
                "deletion requires an original archive safety contract",
            ));
        }
        let session = PostgresSource::new(connection).connect_restore().await?;
        if session.identity != spec.source_identity {
            return Err(Error::Archive(
                "source endpoint or user changed; refusing deletion",
            ));
        }
        session.client.batch_execute("SET application_name='coldctl-delete'; SET search_path=pg_catalog; SET TIME ZONE 'UTC'; SET DateStyle='ISO, YMD'; SET extra_float_digits=3;").await.map_err(pg)?;
        Ok(Self { session, spec })
    }

    pub async fn batch(
        &mut self,
        rows: &[Vec<Option<String>>],
    ) -> Result<DeleteBatchResult, Error> {
        let plan = &self.spec.plan;
        let key_index = plan
            .columns
            .iter()
            .position(|c| c.name == plan.primary_key)
            .ok_or_else(changed)?;
        if rows.is_empty()
            || rows.len() > plan.policy.config.batch_size as usize
            || rows.len() > 1000
        {
            return Err(Error::Archive("invalid deletion batch size"));
        }
        let mut keys = std::collections::BTreeSet::new();
        for row in rows {
            if row.len() != plan.columns.len()
                || row.iter().flatten().map(String::len).sum::<usize>() > 65536
            {
                return Err(changed());
            }
            let key = row[key_index]
                .as_ref()
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(changed)?;
            if !keys.insert(key) {
                return Err(changed());
            }
        }
        let relation = format!(
            "{}.{}",
            quote(&plan.policy.config.schema),
            quote(&plan.policy.config.table)
        );
        let tx = self.session.client.transaction().await.map_err(pg)?;
        // Bound the write exclusion to one batch. This prevents row changes and
        // DDL/trigger/FK changes without requiring UPDATE privileges for row locks.
        tx.batch_execute(&format!(
            "LOCK TABLE {relation} IN SHARE ROW EXCLUSIVE MODE"
        ))
        .await
        .map_err(pg)?;
        let (oid, key, columns) = inspect(&tx, &plan.policy).await?;
        validate_signature(&tx, plan).await?;
        if oid != plan.table_oid || key != plan.primary_key || columns != plan.columns {
            return Err(Error::Archive(
                "source schema or table identity changed; refusing deletion",
            ));
        }
        let identity = tx.query_one("SELECT oid,datname::text FROM pg_catalog.pg_database WHERE datname=current_database()", &[]).await.map_err(pg)?;
        let safety = plan.safety.as_ref().ok_or_else(changed)?;
        if identity.get::<_, u32>(0) != safety.source_identity.database_oid
            || identity.get::<_, String>(1) != safety.source_identity.database_name
        {
            return Err(Error::Archive(
                "source database identity changed; refusing deletion",
            ));
        }
        let unsafe_table: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal) OR EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE confrelid=$1 AND contype='f') OR EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class=$1)", &[&oid]).await.map_err(pg)?.get(0);
        if unsafe_table {
            return Err(Error::Archive(
                "deletion refuses tables with user triggers, rules, or incoming foreign keys",
            ));
        }
        let values = plan
            .columns
            .iter()
            .map(|c| format!("{}::text", quote(&c.name)))
            .collect::<Vec<_>>()
            .join(",");
        let json = format!("pg_catalog.array_to_json(ARRAY[{values}])::text");
        let select = tx.prepare(&format!("SELECT CASE WHEN pg_catalog.octet_length({json})<=65536 THEN {json} ELSE NULL END FROM {relation} WHERE {}=$1::text::bigint", quote(&key))).await.map_err(pg)?;
        let delete = tx
            .prepare(&format!(
                "DELETE FROM {relation} WHERE {}=$1::text::bigint",
                quote(&key)
            ))
            .await
            .map_err(pg)?;
        let mut result = DeleteBatchResult {
            deleted: 0,
            absent: 0,
        };
        for expected in rows {
            let Some(row) = tx
                .query_opt(&select, &[&expected[key_index]])
                .await
                .map_err(pg)?
            else {
                result.absent += 1;
                continue;
            };
            let json: Option<String> = row.get(0);
            let actual: Vec<Option<String>> =
                serde_json::from_str(&json.ok_or_else(changed)?).map_err(|_| changed())?;
            if actual.len() != expected.len()
                || !plan.columns.iter().zip(expected.iter().zip(&actual)).all(
                    |(column, (expected, actual))| same(&column.postgres_type, expected, actual),
                )
            {
                return Err(changed());
            }
            if tx
                .execute(&delete, &[&expected[key_index]])
                .await
                .map_err(pg)?
                != 1
            {
                return Err(changed());
            }
            result.deleted += 1;
        }
        tx.commit().await.map_err(pg)?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn comparison_preserves_nulls_exact_text_and_float_values() {
        let v = |s: &str| Some(s.to_owned());
        assert!(same("bool", &v("true"), &v("t")));
        assert!(same("bool", &v("false"), &v("f")));
        assert!(!same("bool", &None, &v("f")));
        assert!(!same("text", &v("old"), &v("changed")));
        assert!(!same("numeric", &v("1.0000000001"), &v("1.0000000002")));
        assert!(same("float8", &v("1.0"), &v("1")));
        assert!(!same("float8", &v("-0"), &v("0")));
    }
}
