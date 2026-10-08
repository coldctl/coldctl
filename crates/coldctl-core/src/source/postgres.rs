//! Compatibility facade; all database work runs in the optional executable.
use super::{
    ConnectionInfo, DataSource, Discovery, SourceConnection,
    connector::{Request, launch},
};
use crate::{analysis::Analysis, error::Error};
pub struct PostgresSource {
    connection: SourceConnection,
    paths: Option<crate::paths::StatePaths>,
}
impl PostgresSource {
    pub fn new(connection: SourceConnection) -> Self {
        Self {
            connection,
            paths: None,
        }
    }
}
impl PostgresSource {
    pub fn in_state(connection: SourceConnection, paths: &crate::paths::StatePaths) -> Self {
        Self {
            connection,
            paths: Some(paths.clone()),
        }
    }
    async fn client(&self) -> Result<coldctl_connector_runtime::Client, Error> {
        match &self.paths {
            Some(paths) => super::connector::launch_in_state(&self.connection, paths, None).await,
            None => launch(&self.connection).await,
        }
    }
}
impl DataSource for PostgresSource {
    async fn test_connection(&self) -> Result<ConnectionInfo, Error> {
        let mut client = self.client().await?;
        Ok(client.call(Request::Test, 35_000, false).await?)
    }
    async fn discover(&self) -> Result<Discovery, Error> {
        let mut client = self.client().await?;
        Ok(client.call(Request::Discover, 35_000, false).await?)
    }
    async fn analyze(&self) -> Result<Analysis, Error> {
        let mut client = self.client().await?;
        Ok(client.call(Request::Analyze, 65_000, false).await?)
    }
}
