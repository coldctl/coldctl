use super::planner::ArchivePlan;
use crate::archive::key::ArchiveKey;
use crate::destination::StoredObject;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ManifestObject {
    pub object: StoredObject,
    pub rows: i64,
    pub last_key: ArchiveKey,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub job_id: String,
    pub plan: ArchivePlan,
    pub upper_key: Option<ArchiveKey>,
    pub rows: i64,
    pub objects: Vec<ManifestObject>,
    pub source_deleted: bool,
}
