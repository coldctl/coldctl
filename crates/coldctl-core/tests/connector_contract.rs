use coldctl_connector_protocol::{Validate, model::*};
use coldctl_core::{
    archive::batch::{ArchiveColumn, DataBatch},
    connector_contract::*,
};
#[derive(serde::Deserialize)]
struct Legacy {
    columns: Vec<ArchiveColumn>,
    rows: Vec<Vec<Option<String>>>,
    last_key: i64,
}
#[test]
fn legacy_postgres_native_values_and_nulls_are_not_reinterpreted() {
    let f: Legacy = serde_json::from_str(include_str!(
        "../../coldctl-connector-protocol/tests/fixtures/postgres_legacy.json"
    ))
    .unwrap();
    let before = f.rows.clone();
    let converted = postgres_v2_batch(
        &f.columns,
        DataBatch {
            rows: f.rows,
            last_key: f.last_key.into(),
        },
        1,
    )
    .unwrap();
    assert_eq!(postgres_v2_key(&converted.next_cursor).unwrap(), i64::MAX);
    let BatchPayload::Rows { columns, rows } = converted.payload else {
        panic!()
    };
    assert_eq!(columns[1].native_type, "numeric");
    for (old, new) in before.iter().flatten().zip(rows.iter().flatten()) {
        match (old, new) {
            (None, Value::Null) => {}
            (Some(a), Value::NativeText(b)) => assert_eq!(a, b),
            _ => panic!("legacy value changed"),
        }
    }
}
#[test]
fn legacy_cursor_round_trips_extremes_and_refuses_other_contracts() {
    for key in [i64::MIN, -1, 0, 1, i64::MAX] {
        assert_eq!(postgres_v2_key(&postgres_v2_cursor(key)).unwrap(), key);
    }
    let mut cursor = postgres_v2_cursor(1);
    cursor.connector_id = "mysql".into();
    assert!(postgres_v2_key(&cursor).is_err());
    cursor.connector_id = "postgres".into();
    cursor.version = 2;
    assert!(postgres_v2_key(&cursor).is_err());
}
#[test]
fn plan_conversion_preserves_bounds_identity_and_rejects_missing_stability() {
    let mut plan:coldctl_core::archive::planner::ArchivePlan=serde_json::from_value(serde_json::json!({
        "policy":{"id":"p","name":"p","source":"s","destination":"d","created_at":"2026-01-01T00:00:00Z","config":{"schema":"public","table":"events","time_column":"created_at","older_than_days":365,"batch_size":100,"equals_column":null,"equals_value":null}},
        "destination_path":"C:/archive","cutoff_utc":"2025-01-01T00:00:00Z","primary_key":"id","table_oid":42,
        "columns":[{"name":"id","postgres_type":"int8","nullable":false}],"estimated_rows":1,"delete":false,"warnings":[],
        "safety":{"source_stability":"quiescent_copy","source_identity":{"database_oid":20,"database_name":"fixture","schema_signature":"abc"},"available_bytes_at_preflight":100,"minimum_free_bytes":1}
    })).unwrap();
    let pin = ConnectorPin {
        id: "postgres".into(),
        version: "0.1.1".into(),
        sha256: "a".repeat(64),
    };
    let converted = postgres_v2_plan(&plan, pin.clone(), Some(i64::MAX)).unwrap();
    converted.validate().unwrap();
    assert_eq!(converted.key_fields, vec!["id"]);
    assert_eq!(converted.source_identity["table-oid"], "42");
    assert_eq!(
        postgres_v2_key(converted.upper_bound.as_ref().unwrap()).unwrap(),
        i64::MAX
    );
    assert!(
        postgres_v2_plan(&plan, pin.clone(), None)
            .unwrap()
            .upper_bound
            .is_none()
    );
    plan.policy.config.equals_column = Some("status".into());
    assert!(postgres_v2_plan(&plan, pin.clone(), Some(1)).is_err());
    plan.policy.config.equals_column = None;
    plan.delete = true;
    assert!(postgres_v2_plan(&plan, pin.clone(), Some(1)).is_err());
    plan.delete = false;
    plan.safety = None;
    assert!(postgres_v2_plan(&plan, pin, None).is_err());
}
