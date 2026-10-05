pub mod local;

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
}

#[derive(Debug, Serialize)]
pub struct AccessCheck {
    pub path: PathBuf,
    pub writable: bool,
    pub readable: bool,
    pub probe_removed: bool,
}

/// Storage behavior belongs here; object put/verify methods arrive with archive execution.
pub trait ArchiveDestination {
    fn test_access(&self) -> Result<AccessCheck, Error>;
    fn put_file(&self, key: &str, source: &std::path::Path) -> Result<StoredObject, Error>;
    fn verify(&self, object: &StoredObject) -> Result<(), Error>;
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
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
