//! Catalog discovery is advisory. Deletion rechecks under table and row locks.
use crate::postgres::postgres_error;
use coldctl_core::{
    error::Error,
    source::{ForeignKey, Relationships},
};
use tokio_postgres::GenericClient;
pub fn pg(e: tokio_postgres::Error) -> Error {
    postgres_error("inspect foreign keys", e)
}
pub async fn discover(client: &impl GenericClient) -> Result<Relationships, Error> {
    let rows=client.query("SELECT f.conname::text,cn.nspname::text,c.relname::text,pn.nspname::text,p.relname::text,ARRAY(SELECT a.attname::text FROM unnest(f.conkey) WITH ORDINALITY k(n,i) JOIN pg_catalog.pg_attribute a ON a.attrelid=f.conrelid AND a.attnum=k.n ORDER BY k.i),ARRAY(SELECT a.attname::text FROM unnest(f.confkey) WITH ORDINALITY k(n,i) JOIN pg_catalog.pg_attribute a ON a.attrelid=f.confrelid AND a.attnum=k.n ORDER BY k.i),f.confdeltype::text,f.confupdtype::text FROM pg_catalog.pg_constraint f JOIN pg_catalog.pg_class c ON c.oid=f.conrelid JOIN pg_catalog.pg_namespace cn ON cn.oid=c.relnamespace JOIN pg_catalog.pg_class p ON p.oid=f.confrelid JOIN pg_catalog.pg_namespace pn ON pn.oid=p.relnamespace WHERE f.contype='f' ORDER BY cn.nspname,c.relname,f.conname",&[]).await.map_err(pg)?;
    fn action(s: String) -> String {
        match s.as_str() {
            "a" => "NO ACTION",
            "r" => "RESTRICT",
            "c" => "CASCADE",
            "n" => "SET NULL",
            "d" => "SET DEFAULT",
            _ => "UNKNOWN",
        }
        .into()
    }
    Ok(Relationships{visibility:"PostgreSQL catalogs; deletion rechecks privileges, RLS, RI triggers and FK metadata under locks".into(),foreign_keys:rows.iter().map(|r|ForeignKey{name:r.get(0),child_schema:r.get(1),child_table:r.get(2),parent_schema:r.get(3),parent_table:r.get(4),child_columns:r.get(5),parent_columns:r.get(6),on_delete:action(r.get(7)),on_update:action(r.get(8))}).collect()})
}
/// Restrict selection/guards to identical built-in types/collations and built-in
/// equality. Casting different FK types can round values and over-select children.
pub async fn validate(client: &impl GenericClient, fk: &ForeignKey) -> Result<(), Error> {
    let ok:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint f JOIN pg_catalog.pg_class c ON c.oid=f.conrelid JOIN pg_catalog.pg_namespace cn ON cn.oid=c.relnamespace WHERE f.contype='f' AND f.conname=$1 AND cn.nspname=$2 AND c.relname=$3 AND NOT EXISTS(SELECT 1 FROM unnest(f.conkey,f.confkey,f.conpfeqop) k(ck,pk,op) JOIN pg_catalog.pg_attribute ca ON ca.attrelid=f.conrelid AND ca.attnum=k.ck JOIN pg_catalog.pg_attribute pa ON pa.attrelid=f.confrelid AND pa.attnum=k.pk JOIN pg_catalog.pg_operator o ON o.oid=k.op WHERE ca.atttypid<>pa.atttypid OR ca.atttypmod<>pa.atttypmod OR ca.attcollation<>pa.attcollation OR o.oprnamespace<>'pg_catalog'::regnamespace OR o.oprname<>'='))",&[&fk.name,&fk.child_schema,&fk.child_table]).await.map_err(pg)?.get(0);
    if !ok {
        return Err(Error::Archive(
            "foreign-key selection requires identical types and collations with built-in equality",
        ));
    }
    Ok(())
}

pub async fn guard_empty(
    tx: &tokio_postgres::Transaction<'_>,
    plan: &coldctl_core::archive::planner::ArchivePlan,
    keys: &[i64],
) -> Result<(), Error> {
    use crate::postgres_archive::{quote_identifier as q, relation};
    let role: String = tx
        .query_one("SELECT current_setting('session_replication_role')", &[])
        .await
        .map_err(pg)?
        .get(0);
    if role != "origin" {
        return Err(Error::Archive(
            "coordinated deletion requires origin replication role",
        ));
    }
    // FOR UPDATE conflicts with the key-share locks taken by FK writers. The next
    // READ COMMITTED statement sees any writer that committed while we waited.
    tx.query(
        &format!(
            "SELECT {} FROM {} WHERE {}=ANY($1::bigint[]) ORDER BY {} FOR UPDATE",
            q(&plan.primary_key),
            relation(&plan.policy),
            q(&plan.primary_key),
            q(&plan.primary_key)
        ),
        &[&keys],
    )
    .await
    .map_err(pg)?;
    for fk in discover(tx).await?.foreign_keys.into_iter().filter(|f| {
        f.parent_schema == plan.policy.config.schema && f.parent_table == plan.policy.config.table
    }) {
        let child = format!("{}.{}", q(&fk.child_schema), q(&fk.child_table));
        tx.batch_execute(&format!(
            "LOCK TABLE {child} IN SHARE UPDATE EXCLUSIVE MODE"
        ))
        .await
        .map_err(pg)?;
        let safe:bool=tx.query_one("SELECT c.relkind='r' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid=c.oid OR inhparent=c.oid) AND (SELECT count(*)=4 AND bool_and(t.tgenabled IN ('O','A')) FROM pg_catalog.pg_trigger t WHERE t.tgconstraint=f.oid) FROM pg_catalog.pg_constraint f JOIN pg_catalog.pg_class c ON c.oid=f.conrelid JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND f.conname=$3 AND f.contype='f'",&[&fk.child_schema,&fk.child_table,&fk.name]).await.map_err(pg)?.get(0);
        if !safe {
            return Err(Error::Archive(
                "incoming foreign keys require ordinary readable children without RLS and enabled RI triggers",
            ));
        }
        validate(tx, &fk).await?;
        let join = fk
            .child_columns
            .iter()
            .zip(&fk.parent_columns)
            .map(|(c, p)| format!("c.{}=p.{}", q(c), q(p)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let present:bool=tx.query_one(&format!("SELECT EXISTS(SELECT 1 FROM {child} c JOIN {} p ON {join} WHERE p.{}=ANY($1::bigint[]))",relation(&plan.policy),q(&plan.primary_key)),&[&keys]).await.map_err(pg)?.get(0);
        if present {
            return Err(Error::Archive(
                "parent deletion blocked by incoming foreign keys with remaining children",
            ));
        }
    }
    Ok(())
}
