//! Explicit conversions between legacy PostgreSQL archives and generic contracts.
use crate::archive::{
    batch::{ArchiveColumn, DataBatch},
    planner::ArchivePlan,
};
use coldctl_connector_protocol::{ContractError, Result, Validate, model::*};
use std::collections::BTreeMap;

pub fn postgres_v2_cursor(key: i64) -> Cursor {
    Cursor {
        connector_id: "postgres".into(),
        version: 1,
        encoding: "legacy-i64-le".into(),
        token: key.to_le_bytes().to_vec(),
    }
}
pub fn postgres_v2_key(cursor: &Cursor) -> Result<i64> {
    cursor.validate()?;
    if cursor.connector_id != "postgres"
        || cursor.version != 1
        || cursor.encoding != "legacy-i64-le"
        || cursor.token.len() != 8
    {
        return Err(ContractError("not a legacy PostgreSQL cursor"));
    }
    Ok(i64::from_le_bytes(
        cursor.token.as_slice().try_into().unwrap(),
    ))
}
/// Keep native text intact, including JSON whitespace, NaN, BC dates and numeric scale.
pub fn postgres_v2_batch(
    columns: &[ArchiveColumn],
    batch: DataBatch,
    sequence: u64,
) -> Result<Batch> {
    let result = Batch {
        sequence,
        payload: BatchPayload::Rows {
            columns: columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    logical_type: LogicalType::NativeText,
                    native_type: c.postgres_type.clone(),
                    nullable: c.nullable,
                    parameters: BTreeMap::new(),
                })
                .collect(),
            rows: batch
                .rows
                .into_iter()
                .map(|r| {
                    r.into_iter()
                        .map(|v| v.map_or(Value::Null, Value::NativeText))
                        .collect()
                })
                .collect(),
        },
        next_cursor: postgres_v2_cursor(
            batch
                .last_key
                .integer()
                .map_err(|_| ContractError("legacy batch requires integer cursor"))?,
        ),
    };
    result.validate()?;
    Ok(result)
}
/// This does not strengthen old identity evidence or migrate a live job.
/// `upper == None` is the saved empty-scan bound, not an unbounded scan.
pub fn postgres_v2_plan(
    plan: &ArchivePlan,
    pin: ConnectorPin,
    upper: Option<i64>,
) -> Result<ScanPlan> {
    plan.policy
        .config
        .validate()
        .map_err(|_| ContractError("invalid legacy policy configuration"))?;
    if plan.delete {
        return Err(ContractError(
            "source deletion is not a connector v1 capability",
        ));
    }
    if pin.id != "postgres" {
        return Err(ContractError("legacy plan requires PostgreSQL connector"));
    }
    let mut predicates = vec![Predicate::Before {
        field: plan.policy.config.time_column.clone(),
        utc_cutoff: plan.cutoff_utc.clone(),
    }];
    if let (Some(field), Some(value)) = (
        &plan.policy.config.equals_column,
        &plan.policy.config.equals_value,
    ) {
        predicates.push(Predicate::Equal {
            field: field.clone(),
            value: Value::NativeText(value.clone()),
        });
    }
    let mut identity = BTreeMap::from([("table-oid".into(), plan.table_oid.to_string())]);
    let consistency = match &plan.safety {
        Some(safety) => {
            identity.insert(
                "database-oid".into(),
                safety.source_identity.database_oid.to_string(),
            );
            identity.insert(
                "database-name".into(),
                safety.source_identity.database_name.clone(),
            );
            identity.insert(
                "schema-signature".into(),
                safety.source_identity.schema_signature.clone(),
            );
            match safety.source_stability {
                crate::archive::planner::SourceStability::ImmutableRows => {
                    Consistency::ImmutableRows
                }
                crate::archive::planner::SourceStability::QuiescentCopy => {
                    Consistency::QuiescentCopy
                }
            }
        }
        None => {
            return Err(ContractError(
                "legacy plan lacks an explicit stability contract; operator migration required",
            ));
        }
    };
    let result = ScanPlan {
        version: 1,
        connector: pin,
        dataset: Dataset {
            kind: DatasetKind::Table,
            namespace: vec![plan.policy.config.schema.clone()],
            name: plan.policy.config.table.clone(),
        },
        consistency,
        predicates,
        key_fields: vec![plan.primary_key.clone()],
        value_encoding: "postgres-text-v2".into(),
        columns: plan
            .columns
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                logical_type: LogicalType::NativeText,
                native_type: c.postgres_type.clone(),
                nullable: c.nullable,
                parameters: BTreeMap::new(),
            })
            .collect(),
        source_identity: identity,
        upper_bound: upper.map(postgres_v2_cursor),
    };
    result.validate()?;
    Ok(result)
}
