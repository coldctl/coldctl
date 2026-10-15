pub mod azure;
pub mod local;
mod object;
pub mod s3;
mod storage;
pub use storage::{DestinationConfig, for_job, open};

use crate::error::Error;
use serde::Serialize;
use std::path::PathBuf;

#[derive(Debug, Serialize)]
pub struct Destination {
    pub id: String,
    pub name: String,
    pub destination_type: String,
    pub path: PathBuf,
    pub created_at: String,
    pub config: Option<DestinationConfig>,
}

#[derive(Debug, Serialize)]
pub struct AccessCheck {
    pub path: PathBuf,
    pub writable: bool,
    pub readable: bool,
    pub probe_removed: bool,
}

/// Shared sink contract: relative keys, create-only publication and verified reads.
pub trait ArchiveDestination {
    fn exists(&self, key: &str) -> Result<bool, Error>;
    /// Return a seekable file containing exactly the checksum-verified object.
    /// Remote implementations use a bounded, automatically deleted temporary file.
    fn read_verified(&self, object: &StoredObject) -> Result<std::fs::File, Error>;
    fn test_access(&self) -> Result<AccessCheck, Error>;
    /// Create without overwrite, read-back verify, then return the content receipt.
    fn put_file(&self, key: &str, source: &std::path::Path) -> Result<StoredObject, Error>;
    fn verify(&self, object: &StoredObject) -> Result<(), Error>;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredObject {
    pub key: String,
    pub bytes: u64,
    pub sha256: String,
}

pub fn validate_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(Error::DestinationConfiguration(
            "name must contain 1–64 ASCII letters, digits, hyphens, or underscores",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod cloud_auth_tests;
