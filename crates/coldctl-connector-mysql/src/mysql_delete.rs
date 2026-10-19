//! Bounded, row-locked InnoDB transactions with metadata protection.
use crate::{
    mysql::{self, bad, err, q, table},
    mysql_archive::{MysqlArchive, key_parameter, key_type},
};
use coldctl_core::{
    error::Error,
    source::{
        config::ResolvedConnection,
        connector::{DeleteBatchResult, DeleteSpec},
        mysql_types,
    },
};
use mysql_async::{Row, prelude::Queryable};

pub struct DeleteSession {
    archive: MysqlArchive,
    spec: DeleteSpec,
}

// Information_schema.triggers silently omits triggers without TRIGGER privilege.
// Require a direct grant at a recognized exact scope rather than trusting an
// empty metadata result. Accept unrelated database revokes, never target revokes.
fn can_inspect_triggers(grants: &[String], database: &str, name: &str) -> bool {
    for grant in grants.iter().filter(|s| s.starts_with("REVOKE ")) {
        let Some((_, scope)) = grant.split_once(" ON ") else {
            return false;
        };
        let Some((scope, _)) = scope.split_once(" FROM ") else {
            return false;
        };
        // SHOW GRANTS uses backticks or ANSI double quotes. Only accept a
        // simple, exact database scope; ambiguous wildcard syntax fails closed.
        let Some(db) = scope.strip_suffix(".*") else {
            return false;
        };
        let db = db
            .strip_prefix('`')
            .and_then(|s| s.strip_suffix('`'))
            .or_else(|| db.strip_prefix('"').and_then(|s| s.strip_suffix('"')));
        let Some(db) = db else {
            return false;
        };
        if mysql::name(db).is_err() || db.eq_ignore_ascii_case(database) {
            return false;
        }
    }
    let scopes = [
        "*.*".to_owned(),
        format!("{}.*", q(database)),
        table(database, name),
        format!("\"{database}\".*"),
        format!("\"{database}\".\"{name}\""),
    ];
    grants.iter().any(|grant| {
        let Some(body) = grant.strip_prefix("GRANT ") else {
            return false;
        };
        let Some((privileges, rest)) = body.split_once(" ON ") else {
            return false;
        };
        let Some((scope, _)) = rest.split_once(" TO ") else {
            return false;
        };
        scopes.iter().any(|s| s == scope)
            && (privileges == "ALL PRIVILEGES" || privileges.split(", ").any(|p| p == "TRIGGER"))
    })
}

impl DeleteSession {
    pub async fn open(connection: ResolvedConnection, spec: DeleteSpec) -> Result<Self, Error> {
        spec.plan.policy.config.validate()?;
        if spec.plan.safety.is_none() {
            return Err(bad());
        }
        let archive = MysqlArchive::connect_mode(connection, false).await?;
        if archive.identity() != spec.source_identity {
            return Err(Error::Archive(
                "MySQL source identity changed; refusing deletion",
            ));
        }
        Ok(Self { archive, spec })
    }
    pub async fn abort(&mut self) {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let _ = self.archive.conn.query_drop("ROLLBACK").await;
        })
        .await;
    }
    pub async fn batch(
        &mut self,
        rows: &[Vec<Option<String>>],
    ) -> Result<DeleteBatchResult, Error> {
        let result = self.batch_inner(rows).await;
        if result.is_err() {
            self.abort().await;
        }
        result
    }
    async fn batch_inner(
        &mut self,
        rows: &[Vec<Option<String>>],
    ) -> Result<DeleteBatchResult, Error> {
        let plan = &self.spec.plan;
        if rows.is_empty()
            || rows.len() > 400
            || rows.len() > plan.policy.config.batch_size as usize
        {
            return Err(bad());
        }
        let key_index = plan
            .columns
            .iter()
            .position(|c| c.name == plan.primary_key)
            .ok_or_else(bad)?;
        let kind = key_type(plan)?;
        let mut keys = std::collections::HashSet::new();
        let mut parameters = Vec::new();
        for row in rows {
            if row.len() != plan.columns.len()
                || row.iter().flatten().map(String::len).sum::<usize>() > 65536
            {
                return Err(bad());
            }
            let key = mysql_types::cursor(kind, row[key_index].as_deref().ok_or_else(bad)?)?;
            if !keys.insert(key) {
                return Err(bad());
            }
            parameters.push(key_parameter(kind, key)?);
        }
        let database = &plan.policy.config.schema;
        let name = &plan.policy.config.table;
        let target = table(database, name);
        self.archive
            .conn
            .query_drop("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .await
            .map_err(err)?;
        self.archive
            .conn
            .query_drop("START TRANSACTION")
            .await
            .map_err(err)?;
        // Open the table with a write-compatible metadata lock before inspecting
        // schema/triggers/FKs. Held through commit; unrelated DML remains allowed.
        self.archive
            .conn
            .query_drop(format!(
                "SELECT {} FROM {target} WHERE 0 FOR UPDATE",
                q(&plan.primary_key)
            ))
            .await
            .map_err(err)?;
        self.archive.validate_resume(plan).await?;
        let grants: Vec<String> = self.archive.conn.query("SHOW GRANTS").await.map_err(err)?;
        if !can_inspect_triggers(&grants, database, name) {
            return Err(Error::Archive(
                "MySQL deletion requires direct TRIGGER privilege to inspect all source-table triggers",
            ));
        }
        let trigger:Option<u8>=self.archive.conn.exec_first("SELECT 1 FROM information_schema.triggers WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=? LIMIT 1",(database,name)).await.map_err(err)?;
        // PROCESS-protected InnoDB metadata includes cross-database constraints,
        // even if the user cannot read the child table's rows or ordinary metadata.
        let incoming: Option<u8> = self
            .archive
            .conn
            .exec_first(
                "SELECT 1 FROM information_schema.innodb_foreign WHERE REF_NAME=? LIMIT 1",
                (format!("{database}/{name}"),),
            )
            .await
            .map_err(err)?;
        if trigger.is_some() || incoming.is_some() {
            return Err(Error::Archive(
                "MySQL deletion refuses triggers or incoming foreign keys",
            ));
        }
        let columns = plan
            .columns
            .iter()
            .map(|c| q(&c.name))
            .collect::<Vec<_>>()
            .join(",");
        let sizes = plan
            .columns
            .iter()
            .map(|c| format!("COALESCE(OCTET_LENGTH({}),0)", q(&c.name)))
            .collect::<Vec<_>>()
            .join("+");
        let placeholders = vec!["?"; parameters.len()].join(",");
        let predicate = format!("{} IN ({placeholders})", q(&plan.primary_key));
        // Lock the bounded key set before fetching potentially large values.
        // READ COMMITTED avoids retaining gaps for missing keys. Only the keys
        // actually read below are deleted, so a later insert cannot be swept up.
        let found_sizes: Vec<(mysql_async::Value, u64)> = self.archive.conn.exec(
            format!("SELECT {}, {sizes} FROM {target} FORCE INDEX (PRIMARY) WHERE {predicate} ORDER BY {} FOR UPDATE", q(&plan.primary_key), q(&plan.primary_key)),
            mysql_async::Params::Positional(parameters.clone())).await.map_err(err)?;
        if found_sizes.iter().any(|(_, size)| *size > 30000) {
            return Err(Error::Archive(
                "MySQL source row changed or exceeds archive byte limit",
            ));
        }
        let parameters: Vec<_> = found_sizes.into_iter().map(|(key, _)| key).collect();
        let predicate = format!(
            "{} IN ({})",
            q(&plan.primary_key),
            vec!["?"; parameters.len()].join(",")
        );
        let select_sql = format!(
            "SELECT {columns} FROM {target} FORCE INDEX (PRIMARY) WHERE {predicate} ORDER BY {} FOR UPDATE",
            q(&plan.primary_key)
        );
        let actual_rows: Vec<Row> = if parameters.is_empty() {
            Vec::new()
        } else {
            self.archive
                .conn
                .exec(select_sql, mysql_async::Params::Positional(parameters))
                .await
                .map_err(err)?
        };
        let expected = rows
            .iter()
            .map(|row| {
                Ok((
                    mysql_types::cursor(kind, row[key_index].as_deref().ok_or_else(bad)?)?,
                    row,
                ))
            })
            .collect::<Result<std::collections::HashMap<_, _>, Error>>()?;
        let mut present = Vec::new();
        for actual in actual_rows {
            let actual = mysql::encode(actual, &plan.columns)?;
            let key = mysql_types::cursor(kind, actual[key_index].as_deref().ok_or_else(bad)?)?;
            if expected.get(&key).copied() != Some(&actual) {
                return Err(Error::Archive(
                    "MySQL source row differs from archive; deletion batch rolled back",
                ));
            }
            present.push(key_parameter(kind, key)?);
        }
        let mut result = DeleteBatchResult {
            deleted: 0,
            absent: (rows.len() - present.len()) as u64,
        };
        if !present.is_empty() {
            let count = present.len() as u64;
            let placeholders = vec!["?"; present.len()].join(",");
            self.archive
                .conn
                .exec_drop(
                    format!(
                        "DELETE FROM {target} WHERE {} IN ({placeholders})",
                        q(&plan.primary_key)
                    ),
                    mysql_async::Params::Positional(present),
                )
                .await
                .map_err(err)?;
            if self.archive.conn.affected_rows() != count {
                return Err(bad());
            }
            result.deleted = count;
        }
        self.archive.conn.query_drop("COMMIT").await.map_err(err)?;
        Ok(result)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn trigger_visibility_requires_an_unambiguous_direct_grant() {
        for scope in ["*.*", "`db`.*", "`db`.`events`"] {
            assert!(can_inspect_triggers(
                &[format!(
                    "GRANT SELECT, DELETE, TRIGGER ON {scope} TO 'user'@'%'"
                )],
                "db",
                "events"
            ));
        }
        for grant in [
            "GRANT SELECT ON *.* TO 'u'@'%'",
            "GRANT TRIGGER ON `other`.* TO 'u'@'%'",
            "GRANT `admin_role` TO 'u'@'%'",
            "GRANT TRIGGER ON `db`.`other` TO 'u'@'%'",
        ] {
            assert!(!can_inspect_triggers(&[grant.into()], "db", "events"));
        }
        assert!(!can_inspect_triggers(
            &[
                "GRANT ALL PRIVILEGES ON *.* TO 'u'@'%'".into(),
                "REVOKE TRIGGER ON `db`.* FROM 'u'@'%'".into()
            ],
            "db",
            "events"
        ));
        for quote in ['`', '"'] {
            let grant = "GRANT SELECT, DELETE, TRIGGER ON *.* TO 'u'@'%'".to_owned();
            assert!(can_inspect_triggers(
                &[
                    grant.clone(),
                    format!("REVOKE TRIGGER ON {quote}mysql{quote}.* FROM 'u'@'%'")
                ],
                "db",
                "events"
            ));
            assert!(!can_inspect_triggers(
                &[
                    grant.clone(),
                    format!("REVOKE TRIGGER ON {quote}db{quote}.* FROM 'u'@'%'")
                ],
                "db",
                "events"
            ));
            assert!(!can_inspect_triggers(
                &[grant.clone(), "REVOKE TRIGGER ON *.* FROM 'u'@'%'".into()],
                "db",
                "events"
            ));
            assert!(can_inspect_triggers(
                &[format!(
                    "GRANT TRIGGER ON {quote}db{quote}.{quote}events{quote} TO 'u'@'%'"
                )],
                "db",
                "events"
            ));
        }
    }
}
