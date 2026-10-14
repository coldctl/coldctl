//! One explicitly locked InnoDB transaction per verified archive batch.
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
// empty metadata result. Role-only grants and partial revokes fail closed.
fn can_inspect_triggers(grants: &[String], database: &str, name: &str) -> bool {
    if grants.iter().any(|s| s.starts_with("REVOKE ")) {
        return false;
    }
    let scopes = [
        "*.*".to_owned(),
        format!("{}.*", q(database)),
        table(database, name),
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
        // UNLOCK TABLES can commit: always ROLLBACK first, and never unlock if
        // rollback itself fails. Process exit then closes the connection.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            if self.archive.conn.query_drop("ROLLBACK").await.is_ok() {
                let _ = self.archive.conn.query_drop("UNLOCK TABLES").await;
            }
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
            .query_drop("SET autocommit=0")
            .await
            .map_err(err)?;
        // START TRANSACTION after LOCK TABLES would release the explicit locks.
        self.archive
            .conn
            .query_drop(format!("LOCK TABLES {target} WRITE"))
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
        let size_sql = format!(
            "SELECT {sizes} FROM {target} WHERE {}=?",
            q(&plan.primary_key)
        );
        let select_sql = format!(
            "SELECT {columns} FROM {target} WHERE {}=?",
            q(&plan.primary_key)
        );
        let delete_sql = format!("DELETE FROM {target} WHERE {}=?", q(&plan.primary_key));
        let mut result = DeleteBatchResult {
            deleted: 0,
            absent: 0,
        };
        for (expected, key) in rows.iter().zip(parameters) {
            let size: Option<u64> = self
                .archive
                .conn
                .exec_first(&size_sql, (&key,))
                .await
                .map_err(err)?;
            let Some(size) = size else {
                result.absent += 1;
                continue;
            };
            if size > 30000 {
                return Err(Error::Archive(
                    "MySQL source row changed or exceeds archive byte limit",
                ));
            }
            let actual: Row = self
                .archive
                .conn
                .exec_first(&select_sql, (&key,))
                .await
                .map_err(err)?
                .ok_or_else(bad)?;
            if mysql::encode(actual, &plan.columns)? != *expected {
                return Err(Error::Archive(
                    "MySQL source row differs from archive; deletion batch rolled back",
                ));
            }
            self.archive
                .conn
                .exec_drop(&delete_sql, (&key,))
                .await
                .map_err(err)?;
            if self.archive.conn.affected_rows() != 1 {
                return Err(bad());
            }
            result.deleted += 1;
        }
        self.archive.conn.query_drop("COMMIT").await.map_err(err)?;
        self.archive
            .conn
            .query_drop("UNLOCK TABLES")
            .await
            .map_err(err)?;
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
    }
}
