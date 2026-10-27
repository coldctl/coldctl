//! Delete supplied archived keys transactionally; verify returned values before commit.
use crate::{
    postgres::{PostgresSource, Session, postgres_error},
    postgres_archive::{inspect, quote_identifier as quote, validate_signature},
};
use coldctl_core::{
    error::Error,
    source::{
        config::ResolvedConnection,
        connector::{DeleteBatchResult, DeleteSpec, LiveDeleteBatchResult},
    },
};

pub struct DeleteSession {
    session: Session,
    spec: DeleteSpec,
    live: bool,
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
        Self::open_mode(connection, spec, false).await
    }

    pub async fn open_live(
        connection: ResolvedConnection,
        spec: DeleteSpec,
    ) -> Result<Self, Error> {
        Self::open_mode(connection, spec, true).await
    }

    async fn open_mode(
        connection: ResolvedConnection,
        spec: DeleteSpec,
        live: bool,
    ) -> Result<Self, Error> {
        spec.plan.policy.config.validate()?;
        if spec.plan.safety.is_none()
            || spec.plan.delete
            || spec.plan.live_values() != live
            || (live && (spec.require_empty_children || !spec.plan.parent_selection.is_empty()))
        {
            return Err(Error::Archive(
                "deletion requires an original archive safety contract",
            ));
        }
        let session = PostgresSource::new(connection).connect_restore().await?;
        crate::outcomes::lock(&session.client, spec.plan.table_oid).await?;
        if session.identity != spec.source_identity {
            return Err(Error::Archive(
                "source endpoint or user changed; refusing deletion",
            ));
        }
        session.client.batch_execute("SET application_name='coldctl-delete'; SET search_path=pg_catalog; SET TIME ZONE 'UTC'; SET DateStyle='ISO, YMD'; SET extra_float_digits=3;").await.map_err(pg)?;
        if live {
            validate_live(&session.client, &spec.plan).await?;
            session
                .client
                .batch_execute("SET idle_in_transaction_session_timeout='10s'")
                .await
                .map_err(pg)?;
        }
        Ok(Self {
            session,
            spec,
            live,
        })
    }

    pub async fn batch(
        &mut self,
        rows: &[Vec<Option<String>>],
    ) -> Result<DeleteBatchResult, Error> {
        if self.live {
            return Err(Error::Archive("legacy operation on live deletion session"));
        }
        let result = self.batch_inner(rows).await?;
        Ok(DeleteBatchResult {
            deleted: result.deleted,
            absent: result.absent,
        })
    }

    pub async fn batch_live(
        &mut self,
        rows: &[Vec<Option<String>>],
    ) -> Result<LiveDeleteBatchResult, Error> {
        if !self.live {
            return Err(Error::Archive("live operation on legacy deletion session"));
        }
        tokio::time::timeout(std::time::Duration::from_secs(30), self.batch_inner(rows))
            .await
            .map_err(|_| Error::Connector(coldctl_connector_runtime::Error::Timeout))?
    }

    async fn batch_inner(
        &mut self,
        rows: &[Vec<Option<String>>],
    ) -> Result<LiveDeleteBatchResult, Error> {
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
        let mut expected_by_key = std::collections::BTreeMap::new();
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
            if expected_by_key.insert(key, row).is_some() {
                return Err(changed());
            }
        }
        let relation = format!(
            "{}.{}",
            quote(&plan.policy.config.schema),
            quote(&plan.policy.config.table)
        );
        let tx = self
            .session
            .client
            .build_transaction()
            .isolation_level(tokio_postgres::IsolationLevel::ReadCommitted)
            .start()
            .await
            .map_err(pg)?;
        // Protect the schema/trigger/FK checks while permitting ordinary writers.
        // This still conflicts with VACUUM, concurrent index work and other
        // schema guards, so keep it bounded to one batch and retain lock_timeout.
        tx.batch_execute(&format!(
            "LOCK TABLE {relation} IN SHARE UPDATE EXCLUSIVE MODE"
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
        let unsafe_table: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal) OR EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class=$1)", &[&oid]).await.map_err(pg)?.get(0);
        if unsafe_table {
            return Err(Error::Archive(
                "deletion refuses tables with user triggers, rules, or incoming foreign keys",
            ));
        }
        let mut keys: Vec<i64> = expected_by_key.keys().copied().collect();
        if self.spec.require_empty_children {
            crate::relationships::guard_empty(&tx, plan, &keys).await?;
        } else {
            let incoming:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE confrelid=$1 AND contype='f')", &[&oid]).await.map_err(pg)?.get(0);
            if incoming {
                return Err(Error::Archive(
                    "deletion refuses incoming foreign keys outside a coordinated group",
                ));
            }
        }
        let values = plan
            .columns
            .iter()
            .map(|c| format!("{}::text", quote(&c.name)))
            .collect::<Vec<_>>()
            .join(",");
        let json = format!("pg_catalog.array_to_json(ARRAY[{values}])::text");
        let mut deferred = 0;
        let mut absent = 0;
        if self.live {
            validate_live(&tx, plan).await?;
            let predicate = crate::postgres_archive::predicate(plan);
            let selected = tx.query(
                &format!("SELECT {key}::bigint, CASE WHEN pg_catalog.octet_length({json})<=65536 THEN {json} ELSE NULL END, ({predicate}) IS TRUE FROM ONLY {relation} WHERE {key}=ANY($3::bigint[]) ORDER BY {key} FOR UPDATE", key=quote(&key)),
                &[&plan.cutoff_utc, &plan.policy.config.equals_value, &keys],
            ).await.map_err(pg)?;
            let mut seen = std::collections::BTreeSet::new();
            keys.clear();
            for row in selected {
                let id: i64 = row.get(0);
                let expected = expected_by_key.get(&id).ok_or_else(changed)?;
                if !seen.insert(id) {
                    return Err(changed());
                }
                let encoded: Option<String> = row.get(1);
                let eligible: bool = row.get(2);
                let actual: Option<Vec<Option<String>>> = encoded
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(|_| changed())?;
                if eligible
                    && actual.as_ref().is_some_and(|actual| {
                        actual.len() == expected.len()
                            && plan
                                .columns
                                .iter()
                                .zip(expected.iter().zip(actual))
                                .all(|(c, (a, b))| same(&c.postgres_type, a, b))
                    })
                {
                    keys.push(id);
                } else {
                    deferred += 1;
                }
            }
            absent = (expected_by_key.len() - seen.len()) as u64;
        }
        // DELETE locks only affected rows and RETURNING captures the exact version
        // deleted, including a concurrently committed update. Compare all values
        // before COMMIT; any mismatch rolls back the entire batch. Do not use a
        // separate unlocked SELECT (TOCTOU), or SKIP LOCKED (not evidence of absence).
        // Triggers, rules, incoming FKs and RLS were rejected under the schema guard.
        let deleted = if keys.is_empty() {
            Vec::new()
        } else {
            tx
            .query(
                &format!("DELETE FROM {relation} WHERE {key}=ANY($1::bigint[]) RETURNING {key}::bigint, CASE WHEN pg_catalog.octet_length({json})<=65536 THEN {json} ELSE NULL END", key = quote(&key)),
                &[&keys],
            )
            .await
            .map_err(pg)?
        };
        let mut present = std::collections::BTreeSet::new();
        for row in deleted {
            let id: i64 = row.get(0);
            let expected = expected_by_key.get(&id).ok_or_else(changed)?;
            if !present.insert(id) {
                return Err(changed());
            }
            let json: Option<String> = row.get(1);
            let actual: Vec<Option<String>> =
                serde_json::from_str(&json.ok_or_else(changed)?).map_err(|_| changed())?;
            if actual.len() != expected.len()
                || !plan.columns.iter().zip(expected.iter().zip(&actual)).all(
                    |(column, (expected, actual))| same(&column.postgres_type, expected, actual),
                )
            {
                return Err(changed());
            }
        }
        if self.live && present.len() != keys.len() {
            return Err(changed());
        }
        let result = LiveDeleteBatchResult {
            deleted: present.len() as u64,
            absent: if self.live {
                absent
            } else {
                (keys.len() - present.len()) as u64
            },
            deferred,
        };
        tx.commit().await.map_err(pg)?;
        Ok(result)
    }
}

pub(crate) async fn validate_live(
    client: &impl tokio_postgres::GenericClient,
    plan: &coldctl_core::archive::planner::ArchivePlan,
) -> Result<(), Error> {
    if !plan.live_values() || !plan.parent_selection.is_empty() || plan.delete {
        return Err(Error::Archive(
            "live values require a single PostgreSQL table",
        ));
    }
    let (oid, key, columns) = inspect(client, &plan.policy).await?;
    validate_signature(client, plan).await?;
    if oid != plan.table_oid || key != plan.primary_key || columns != plan.columns {
        return Err(Error::Archive("live source schema changed"));
    }
    let permitted: bool = client
        .query_one(
            "SELECT has_table_privilege($1::oid,'DELETE') AND has_any_column_privilege($1::oid,'UPDATE')",
            &[&oid],
        )
        .await
        .map_err(pg)?
        .get(0);
    if !permitted {
        return Err(Error::Archive(
            "live values require DELETE and UPDATE privilege on at least one column",
        ));
    }
    let unsafe_table: bool = client.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid=$1 AND NOT tgisinternal) OR EXISTS(SELECT 1 FROM pg_catalog.pg_rewrite WHERE ev_class=$1) OR EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE confrelid=$1 AND contype='f')", &[&oid]).await.map_err(pg)?.get(0);
    if unsafe_table {
        return Err(Error::Archive(
            "live deletion refuses triggers, rules, or incoming foreign keys",
        ));
    }
    Ok(())
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
