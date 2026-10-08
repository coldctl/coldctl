use super::batch::ArchiveColumn;
use crate::{
    error::Error,
    paths::StatePaths,
    policy::model::Policy,
    source::ConnectorArchive,
    state::{archive as store, destinations, sources},
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStability {
    ImmutableRows,
    QuiescentCopy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SourceIdentity {
    pub database_oid: u32,
    pub database_name: String,
    pub schema_signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyContract {
    pub source_stability: SourceStability,
    pub source_identity: SourceIdentity,
    pub available_bytes_at_preflight: u64,
    pub minimum_free_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivePlan {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector_pin: Option<coldctl_connector_protocol::model::ConnectorPin>,
    pub policy: Policy,
    pub destination_path: PathBuf,
    pub cutoff_utc: String,
    pub primary_key: String,
    pub table_oid: u32,
    pub columns: Vec<ArchiveColumn>,
    pub estimated_rows: Option<i64>,
    pub delete: bool,
    pub warnings: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub safety: Option<SafetyContract>,
}

pub async fn plan(paths: &StatePaths, name: &str) -> Result<ArchivePlan, Error> {
    let policy = store::policy_show(paths, name)?;
    let source = sources::show(paths, &policy.source)?;
    let destination = destinations::show(paths, &policy.destination)?;
    let reader = ConnectorArchive::connect_in_state(source.connection, paths, None).await?;
    reader.plan(policy, destination.path).await
}
