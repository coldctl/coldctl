//! Explicit PostgreSQL/MySQL policy groups. All archives verify before child-first deletion.
//! Progress remains in ordinary durable archive/deletion journals; retries use IDs.
use super::{
    controls::{ExecutionControls, Shutdown},
    deletion, executor,
    planner::SourceStability,
    verifier,
};
use crate::{
    error::Error,
    paths::StatePaths,
    policy::model::Policy,
    source::{ConnectorSource, DataSource, ForeignKey},
    state::{archive as jobs, sources},
};
use std::collections::{BTreeSet, HashMap};

fn order(policies: &[Policy], fks: &[ForeignKey]) -> Result<Vec<usize>, Error> {
    if policies.len() < 2 || policies.len() > 32 {
        return Err(Error::Archive("groups require 2 to 32 distinct tables"));
    }
    let mut tables = HashMap::new();
    for (i, p) in policies.iter().enumerate() {
        if p.source != policies[0].source
            || tables
                .insert((p.config.schema.clone(), p.config.table.clone()), i)
                .is_some()
        {
            return Err(Error::Archive(
                "group members must be distinct tables on one source",
            ));
        }
    }
    let mut children = vec![BTreeSet::new(); policies.len()];
    for fk in fks {
        if let Some(&parent) = tables.get(&(fk.parent_schema.clone(), fk.parent_table.clone())) {
            let child = tables
                .get(&(fk.child_schema.clone(), fk.child_table.clone()))
                .ok_or(Error::Archive(
                    "group omits an incoming child table; add its explicit policy/job",
                ))?;
            children[parent].insert(*child);
        }
    }
    let mut done = BTreeSet::new();
    let mut result = Vec::new();
    while result.len() < policies.len() {
        let next = (0..policies.len())
            .find(|i| !done.contains(i) && children[*i].iter().all(|c| done.contains(c)))
            .ok_or(Error::Archive(
                "cyclic or self-referencing groups are unsupported; no deletion started",
            ))?;
        result.push(next);
        done.insert(next);
    }
    Ok(result)
}

pub(crate) async fn discover_order(
    paths: &StatePaths,
    policies: &[Policy],
) -> Result<(Vec<usize>, Vec<ForeignKey>), Error> {
    let first = policies
        .first()
        .ok_or(Error::Archive("empty policy group"))?;
    let source = sources::show(paths, &first.source)?;
    if !matches!(source.connection.engine(), "mysql" | "postgres") {
        return Err(Error::Archive(
            "coordinated groups require MySQL or PostgreSQL",
        ));
    }
    let discovered = ConnectorSource::in_state(source.connection, paths)
        .discover()
        .await?;
    let fks = discovered
        .relationships
        .ok_or(Error::Archive("relationship discovery unavailable"))?
        .foreign_keys;
    Ok((order(policies, &fks)?, fks))
}

pub async fn run(
    paths: &StatePaths,
    names: &[String],
    select_children: bool,
    shutdown: Shutdown,
) -> Result<Vec<jobs::Job>, Error> {
    let policies = names
        .iter()
        .map(|n| jobs::policy_show(paths, n))
        .collect::<Result<Vec<_>, _>>()?;
    let (deletion_order, relationships) = discover_order(paths, &policies).await?;
    let indices: Vec<usize> = if select_children {
        deletion_order.into_iter().rev().collect()
    } else {
        (0..names.len()).collect()
    };
    let mut result: Vec<jobs::Job> = Vec::new();
    for index in indices {
        let policy = &policies[index];
        let mut selection = Vec::new();
        if select_children {
            for fk in relationships.iter().filter(|fk| {
                fk.child_schema == policy.config.schema && fk.child_table == policy.config.table
            }) {
                if let Some(parent) = result.iter().find(|j| {
                    j.plan.policy.config.schema == fk.parent_schema
                        && j.plan.policy.config.table == fk.parent_table
                }) {
                    let manifest = super::checkpoint::load(paths, &parent.id)?
                        .manifest
                        .ok_or(Error::Archive("parent manifest missing"))?;
                    selection.push(super::planner::ParentSelection {
                        job_id: parent.id.clone(),
                        manifest_sha256: manifest.sha256,
                        rows: parent.rows_processed,
                        relationship: fk.clone(),
                    });
                }
            }
        }
        if shutdown.requested() {
            return Err(Error::Archive(
                "group interrupted; existing jobs retained; no deletion started",
            ));
        }
        let job = executor::run_selected(
            paths,
            &names[index],
            executor::RunOptions {
                source_stability: SourceStability::QuiescentCopy,
                allow_repeat: false,
                minimum_free_bytes: 1_073_741_824,
                execution: ExecutionControls::default(),
                shutdown: shutdown.clone(),
                progress: None,
            },
            |_| {},
            None,
            selection,
        )
        .await?;
        if job.status != jobs::JobStatus::Completed {
            return Err(Error::Archive(
                "group archive incomplete; resume its job; no deletion started",
            ));
        }
        verifier::verify(paths, &job.id)?;
        result.push(jobs::job_show(paths, &job.id)?);
    }
    Ok(result)
}

pub(crate) async fn verified_order(
    paths: &StatePaths,
    ids: &[String],
) -> Result<Vec<String>, Error> {
    let members = ids
        .iter()
        .map(|id| jobs::job_show(paths, id))
        .collect::<Result<Vec<_>, _>>()?;
    if members
        .iter()
        .any(|j| j.imported || j.status != jobs::JobStatus::Completed || j.plan.safety.is_none())
    {
        return Err(Error::Archive(
            "every group member must be a completed original archive",
        ));
    }
    let policies = members
        .iter()
        .map(|j| j.plan.policy.clone())
        .collect::<Vec<_>>();
    let (order, _) = discover_order(paths, &policies).await?;
    for member in &members {
        for dependency in &member.plan.parent_selection {
            if !ids.contains(&dependency.job_id)
                || super::checkpoint::load(paths, &dependency.job_id)?
                    .manifest
                    .is_none_or(|m| m.sha256 != dependency.manifest_sha256)
            {
                return Err(Error::Archive(
                    "group must include the exact parent archives used for child selection",
                ));
            }
        }
    }
    // Do not trust historical verified timestamps: verify every member now,
    // before the first source mutation, including on a partial-delete retry.
    for job in &members {
        verifier::verify(paths, &job.id)?;
    }
    Ok(order.into_iter().map(|i| members[i].id.clone()).collect())
}

pub async fn delete(
    paths: &StatePaths,
    ids: &[String],
    shutdown: Shutdown,
) -> Result<Vec<crate::state::deletion::Progress>, Error> {
    let order = verified_order(paths, ids).await?;
    let mut results = Vec::new();
    for id in order {
        if shutdown.requested() {
            return Err(Error::Archive("group interrupted; retry the same job IDs"));
        }
        let progress = deletion::run_mode(
            paths,
            &id,
            deletion::Options {
                confirm_delete: true,
                confirm_original_source: true,
                max_batches: None,
                shutdown: shutdown.clone(),
            },
            true,
        )
        .await?;
        if !progress.completed {
            return Err(Error::Archive(
                "child deletion incomplete; parent deletion not started; retry the same IDs",
            ));
        }
        results.push(progress);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn policy(table: &str) -> Policy {
        Policy {
            id: table.into(),
            name: table.into(),
            source: "db".into(),
            destination: "out".into(),
            created_at: "".into(),
            config: crate::policy::model::PolicyConfig {
                schema: "app".into(),
                table: table.into(),
                time_column: "created_at".into(),
                older_than_days: 365,
                batch_size: 10,
                equals_column: None,
                equals_value: None,
            },
        }
    }
    fn fk(child: &str, parent: &str) -> ForeignKey {
        ForeignKey {
            name: "fk".into(),
            child_schema: "app".into(),
            child_table: child.into(),
            child_columns: vec!["parent_id".into()],
            parent_schema: "app".into(),
            parent_table: parent.into(),
            parent_columns: vec!["id".into()],
            on_delete: "CASCADE".into(),
            on_update: "RESTRICT".into(),
        }
    }
    #[test]
    fn graph_orders_descendants_and_rejects_missing_cycles_duplicates() {
        let p = vec![policy("p"), policy("c"), policy("g")];
        assert_eq!(
            order(&p, &[fk("c", "p"), fk("g", "c")]).unwrap(),
            vec![2, 1, 0]
        );
        assert!(order(&p[..2], &[fk("g", "c")]).is_err());
        assert!(order(&p, &[fk("c", "p"), fk("p", "c")]).is_err());
        assert!(order(&p, &[fk("p", "p")]).is_err());
        assert!(order(&[policy("p"), policy("p")], &[]).is_err());
    }
}
