//! Explicit format-2 PostgreSQL compatibility API. The supervisor itself is engine-neutral.
use super::config::ResolvedConnection;
use crate::archive::key::ArchiveKey;
use crate::{
    archive::{controls::ExecutionControls, planner::ArchivePlan},
    error::Error,
    policy::model::Policy,
};
pub use coldctl_connector_runtime::Client;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
#[serde(
    tag = "operation",
    content = "arguments",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Request {
    Configure(ResolvedConnection),
    Test,
    Discover,
    Analyze,
    OpenScan,
    ParentSelectionBegin(ArchivePlan),
    ParentSelectionRows {
        index: usize,
        rows: Vec<Vec<Option<String>>>,
    },
    ParentSelectionFinish,
    ConfigureTimeouts(ExecutionControls),
    Plan {
        policy: Policy,
        destination: PathBuf,
    },
    SourceIdentity {
        table_oid: u32,
    },
    ValidateResume(ArchivePlan),
    UpperKey(ArchivePlan),
    ReadBatch {
        plan: ArchivePlan,
        last: Option<ArchiveKey>,
        upper: ArchiveKey,
        rows: i32,
        bytes: u32,
    },
    RestoreOpen(RestoreSpec),
    RestoreBegin {
        verify_count: bool,
    },
    RestoreValidate {
        rows: Vec<Vec<Option<String>>>,
    },
    RestoreApply {
        sequence: i64,
        rows: Vec<Vec<Option<String>>>,
    },
    RestoreFinish {
        completed: bool,
    },
    DeleteOpen(DeleteSpec),
    MeasurePostgres(DeleteSpec),
    ReclaimPostgres {
        spec: DeleteSpec,
        options: crate::archive::outcomes::ReclaimOptions,
    },
    ValidateLiveV1(ArchivePlan),
    DeleteOpenLiveV1(DeleteSpec),
    DeleteBatchLiveV1 {
        rows: Vec<Vec<Option<String>>>,
    },
    DeleteBatch {
        rows: Vec<Vec<Option<String>>>,
    },
    Close,
    Cancel,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteSpec {
    pub plan: ArchivePlan,
    pub source_identity: String,
    /// Coordinator-only opt-in: incoming references must be empty under parent locks.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub require_empty_children: bool,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteBatchResult {
    pub deleted: u64,
    pub absent: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LiveDeleteBatchResult {
    pub deleted: u64,
    pub absent: u64,
    pub deferred: u64,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreSpec {
    pub plan: ArchivePlan,
    pub objects_created: i64,
    pub rows_processed: i64,
    pub schema: String,
    pub table: String,
    pub manifest_hash: String,
    pub validate_only: bool,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreProgress {
    pub batches: i64,
    pub rows: i64,
    pub completed: bool,
}

pub async fn launch(connection: &super::SourceConnection) -> Result<Client, Error> {
    launch_pinned(connection, None).await
}
pub async fn launch_pinned(
    connection: &super::SourceConnection,
    expected: Option<&coldctl_connector_protocol::model::ConnectorPin>,
) -> Result<Client, Error> {
    let engine = connection.engine();
    let resolved = connection.resolve()?;
    let path = match std::env::var_os(connector_variable(engine)) {
        Some(path) => PathBuf::from(path),
        None => std::env::current_exe()
            .map_err(|_| Error::Archive("cannot locate connector"))?
            .parent()
            .ok_or(Error::Archive("cannot locate connector"))?
            .join(format!(
                "coldctl-connector-{engine}{}",
                std::env::consts::EXE_SUFFIX
            )),
    };
    if !path.is_absolute() || !path.is_file() {
        return Err(Error::Archive(
            "Database connector missing; install the selected engine, or configure its absolute development executable path",
        ));
    }
    if let Some(pin) = expected {
        if pin.id != engine || coldctl_connector_runtime::digest(&path)? != pin.sha256 {
            return Err(Error::Archive(
                "connector version or digest changed; restore the pinned executable before resume",
            ));
        }
    }
    let mut client = Client::launch(
        path,
        connection.engine(),
        uuid::Uuid::new_v4().to_string(),
        engine_features(connection.engine()),
    )
    .await?;
    if expected.is_some_and(|pin| pin != &client.pin) {
        return Err(Error::Archive(
            "connector version or digest changed; restore the pinned executable before resume",
        ));
    }
    require_ca_support(&client, engine, &resolved)?;
    let _: () = client
        .call(Request::Configure(resolved), 15_000, false)
        .await?;
    Ok(client)
}

/// CLI/stateful execution uses approved packages. Development escape hatch is explicit.
pub async fn launch_in_state(
    connection: &super::SourceConnection,
    paths: &crate::paths::StatePaths,
    expected: Option<&coldctl_connector_protocol::model::ConnectorPin>,
) -> Result<Client, Error> {
    if std::env::var("COLDCTL_ALLOW_UNVERIFIED_CONNECTOR").as_deref() == Ok("1")
        && std::env::var_os(connector_variable(connection.engine())).is_some()
    {
        return launch_pinned(connection, expected).await;
    }
    let selected = crate::connectors::select(paths, connection.engine(), expected)?;
    let mut client = Client::launch(
        selected.path,
        connection.engine(),
        uuid::Uuid::new_v4().to_string(),
        engine_features(connection.engine()),
    )
    .await?;
    // Verify the full signed capability declaration before releasing any credentials.
    if client.hello != selected.package.hello {
        return Err(Error::Archive(
            "connector handshake differs from its signed package manifest",
        ));
    }
    client.retain_package_lease(selected.lease);
    let resolved = connection.resolve()?;
    require_ca_support(&client, connection.engine(), &resolved)?;
    client
        .call::<_, ()>(Request::Configure(resolved), 15_000, false)
        .await?;
    Ok(client)
}

fn require_ca_support(
    client: &Client,
    engine: &str,
    resolved: &ResolvedConnection,
) -> Result<(), Error> {
    if engine == "postgres"
        && resolved.ca_pem.is_some()
        && !client.hello.features.contains("postgres-ca-v1")
    {
        return Err(Error::SourceConfiguration(
            "PostgreSQL connector does not support custom CA bundles; upgrade the connector before using --tls-ca-env",
        ));
    }
    Ok(())
}

pub fn host_features() -> std::collections::BTreeSet<String> {
    [
        "legacy-postgres-v2",
        "mysql-text-v1",
        "mongodb-bson-v1",
        "safe-diagnostics-v1",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

pub fn engine_features(engine: &str) -> std::collections::BTreeSet<String> {
    std::collections::BTreeSet::from([if engine == "mongodb" {
        "mongodb-bson-v1"
    } else if engine == "mysql" {
        "mysql-text-v1"
    } else {
        "legacy-postgres-v2"
    }
    .to_owned()])
}

fn connector_variable(engine: &str) -> &'static str {
    match engine {
        "mysql" => "COLDCTL_MYSQL_CONNECTOR",
        "mongodb" => "COLDCTL_MONGODB_CONNECTOR",
        _ => "COLDCTL_POSTGRES_CONNECTOR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_delete_wire_format_omits_disabled_parent_guard() {
        // Released connectors reject unknown DeleteSpec fields.
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct LegacyDeleteSpec {
            plan: ArchivePlan,
            source_identity: String,
        }
        let legacy = serde_json::json!({
            "plan": {
                "policy": {"id":"p", "name":"p", "source":"s", "destination":"d",
                    "created_at":"", "config": {"schema":"public", "table":"events",
                    "time_column":"created_at", "older_than_days":90, "batch_size":1}},
                "destination_path":"archive", "cutoff_utc":"2026-01-01",
                "primary_key":"id", "table_oid":1, "columns":[], "estimated_rows":null,
                "delete":false, "warnings":[]
            },
            "source_identity":"original"
        });
        let mut spec: DeleteSpec = serde_json::from_value(legacy).unwrap();
        assert!(!spec.require_empty_children);
        let wire = serde_json::to_value(&spec).unwrap();
        let old: LegacyDeleteSpec = serde_json::from_value(wire).unwrap();
        assert_eq!(old.source_identity, "original");
        assert_eq!(old.plan.table_oid, 1);
        spec.require_empty_children = true;
        let guarded = serde_json::to_value(&spec).unwrap();
        assert!(serde_json::from_value::<LegacyDeleteSpec>(guarded.clone()).is_err());
        assert!(
            serde_json::from_value::<DeleteSpec>(guarded)
                .unwrap()
                .require_empty_children
        );
    }
}
