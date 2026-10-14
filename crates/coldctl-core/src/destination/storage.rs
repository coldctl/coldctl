//! Provider selection is confined to this module. Executors deal only in object keys.
use super::{ArchiveDestination, local::LocalDestination, object::ObjectDestination, s3::S3Config};
use crate::{error::Error, state::archive::Job};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum DestinationConfig {
    S3(S3Config),
    Azure(super::azure::AzureConfig),
}
impl From<S3Config> for DestinationConfig {
    fn from(config: S3Config) -> Self {
        Self::S3(config)
    }
}
impl From<super::azure::AzureConfig> for DestinationConfig {
    fn from(config: super::azure::AzureConfig) -> Self {
        Self::Azure(config)
    }
}
impl DestinationConfig {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::S3(_) => "s3",
            Self::Azure(_) => "azure",
        }
    }
    pub(super) fn build(&self) -> Result<Box<dyn object_store::ObjectStore>, Error> {
        match self {
            Self::S3(config) => Ok(Box::new(config.build()?)),
            Self::Azure(config) => Ok(Box::new(config.build()?)),
        }
    }
    pub(super) fn key(&self, key: &str) -> Result<object_store::path::Path, Error> {
        match self {
            Self::S3(config) => config.key(key),
            Self::Azure(config) => config.key(key),
        }
    }

    pub fn location(&self) -> String {
        match self {
            Self::Azure(config) => format!(
                "az://{}/{}/{}",
                config.account, config.container, config.prefix
            ),
            Self::S3(config) => format!("s3://{}/{}", config.bucket, config.prefix),
        }
    }
    pub fn validate(&self) -> Result<(), Error> {
        match self {
            Self::S3(config) => config.validate(),
            Self::Azure(config) => config.validate(),
        }
    }
}
pub fn open(
    config: Option<&DestinationConfig>,
    local_root: &Path,
) -> Result<Box<dyn ArchiveDestination>, Error> {
    match config {
        None => Ok(Box::new(LocalDestination::new(local_root.to_owned())?)),
        Some(config) => Ok(Box::new(ObjectDestination::new(
            config.clone(),
            local_root.to_owned(),
        )?)),
    }
}
pub fn for_job(job: &Job) -> Result<Box<dyn ArchiveDestination>, Error> {
    // An explicitly imported local directory must never execute a manifest's remote configuration.
    open(
        if job.imported {
            None
        } else {
            job.plan.destination_config.as_ref()
        },
        &job.storage_root,
    )
}
