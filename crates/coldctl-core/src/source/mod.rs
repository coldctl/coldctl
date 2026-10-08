use crate::archive::key::ArchiveKey;
pub mod config;
pub mod connector;
pub mod mysql_types;
pub mod postgres;
pub mod postgres_archive;
pub mod postgres_restore;

use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::future::Future;

pub use config::SourceConnection;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Source {
    pub id: String,
    pub name: String,
    pub source_type: String,
    pub connection: SourceConnection,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub database: String,
    pub user: String,
    pub server_version: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Discovery {
    pub schemas: Vec<String>,
    pub tables: Vec<Table>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Table {
    pub schema: String,
    pub name: String,
    pub partitioned: bool,
    pub estimated_rows: Option<f64>,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    pub indexes: Vec<Index>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    pub archive_time_candidate: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub method: String,
    pub unique: bool,
    pub primary: bool,
    pub valid: bool,
    pub columns: Vec<String>,
    pub included_columns: Vec<String>,
    pub has_expressions: bool,
    pub partial: bool,
}

/// Database behavior stays behind this boundary; archive methods can be added when implemented.
pub trait DataSource {
    fn analyze(&self) -> impl Future<Output = Result<crate::analysis::Analysis, Error>> + Send;
    fn test_connection(&self) -> impl Future<Output = Result<ConnectionInfo, Error>> + Send;
    fn discover(&self) -> impl Future<Output = Result<Discovery, Error>> + Send;
}

pub trait ArchiveSource {
    fn upper_key(
        &mut self,
        plan: &crate::archive::planner::ArchivePlan,
    ) -> impl Future<Output = Result<Option<ArchiveKey>, Error>> + Send;
    fn read_batch(
        &mut self,
        plan: &crate::archive::planner::ArchivePlan,
        last: Option<ArchiveKey>,
        upper: ArchiveKey,
    ) -> impl Future<Output = Result<Option<crate::archive::batch::DataBatch>, Error>> + Send;
}

/// Engine-selected supervised source, archive and restore facades.
pub use postgres::PostgresSource as ConnectorSource;
pub use postgres_archive::PostgresArchive as ConnectorArchive;
pub use postgres_restore as restore;
