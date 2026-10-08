use super::{Column, ConnectionInfo, DataSource, Discovery, Index, Table, config::TlsMode};
use coldctl_core::error::Error;
use std::{collections::BTreeMap, time::Duration};
use tokio::time::timeout;
use tokio_postgres::{Client, Config, IsolationLevel, config::SslMode};

pub struct PostgresSource {
    connection: coldctl_core::source::config::ResolvedConnection,
}

pub(super) struct Session {
    pub(super) client: Client,
    pub(super) identity: String,
    driver: tokio::task::JoinHandle<()>,
}

impl Drop for Session {
    fn drop(&mut self) {
        // Close even when an operation times out or returns early.
        self.driver.abort();
    }
}

fn failure(operation: &'static str, reason: &'static str) -> Error {
    Error::Postgres { operation, reason }
}

/// Raw server/driver errors may contain credentials or sensitive SQL literals.
/// Expose an actionable category without retaining their text in the error chain.
pub(super) fn postgres_error(operation: &'static str, error: tokio_postgres::Error) -> Error {
    let reason = match error.code().map(|code| code.code()) {
        Some("28P01" | "28000") => {
            "authentication rejected; check the user and credential environment variable"
        }
        Some("42P01" | "42703" | "42804") => {
            "source schema is incompatible or changed; review table and column definitions"
        }
        Some("3D000") => "database does not exist; check the configured database",
        Some("42501") => {
            "permission denied; check CONNECT, schema USAGE, and table SELECT privileges"
        }
        Some("57014" | "55P03") => {
            "query timed out or was cancelled; retry when the database is less busy"
        }
        Some("53300" | "57P03") => "server cannot accept connections currently; retry later",
        Some(_) => {
            "server rejected the metadata request; check server compatibility and permissions"
        }
        None if error.is_closed() || transient_io(&error) => {
            "connection closed or unavailable; check network and server availability before retrying"
        }
        None => {
            "connection or TLS failure; check host, port, network access, and the server certificate trust/hostname"
        }
    };
    failure(operation, reason)
}

fn transient_io(error: &(dyn std::error::Error + 'static)) -> bool {
    if let Some(io) = error.downcast_ref::<std::io::Error>() {
        return matches!(
            io.kind(),
            std::io::ErrorKind::ConnectionRefused
                | std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::BrokenPipe
        );
    }
    error.source().is_some_and(transient_io)
}

impl PostgresSource {
    pub fn new(connection: coldctl_core::source::config::ResolvedConnection) -> Self {
        Self { connection }
    }

    pub(super) async fn connect(&self) -> Result<Session, Error> {
        self.connect_mode(false).await
    }

    pub(super) async fn connect_restore(&self) -> Result<Session, Error> {
        self.connect_mode(true).await
    }

    async fn connect_mode(&self, write: bool) -> Result<Session, Error> {
        let resolved = &self.connection;
        // Pin the resolved endpoint/user/TLS choice, excluding credential values.
        use sha2::{Digest, Sha256};
        let identity = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    &resolved.host,
                    resolved.port,
                    &resolved.database,
                    &resolved.user,
                    resolved.tls
                ))
                .map_err(|_| failure("connect", "cannot identify source"))?
            )
        );
        let mut config = Config::new();
        config.host(&resolved.host).port(resolved.port).user(&resolved.user).dbname(&resolved.database)
            .application_name(if write { "coldctl-restore" } else { "coldctl" })
            .connect_timeout(Duration::from_secs(10))
            .options(if write { "-c default_transaction_read_only=off -c statement_timeout=10000 -c lock_timeout=2000" } else { "-c default_transaction_read_only=on -c statement_timeout=10000 -c lock_timeout=2000" })
            .ssl_mode(match resolved.tls { TlsMode::Require => SslMode::Require, TlsMode::Disable => SslMode::Disable });
        if let Some(password) = &resolved.password {
            config.password(password);
        }
        // Native TLS verifies both the certificate chain and hostname using the OS trust store.
        let tls = native_tls::TlsConnector::new()
            .map_err(|_| failure("connect", "unable to initialize the system TLS provider"))?;
        let (client, connection) = timeout(
            Duration::from_secs(10),
            config.connect(postgres_native_tls::MakeTlsConnector::new(tls)),
        )
        .await
        .map_err(|_| {
            failure(
                "connect",
                "timed out after 10 seconds; check host, port, and network access",
            )
        })?
        .map_err(|e| postgres_error("connect", e))?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Session {
            client,
            driver,
            identity,
        })
    }

    async fn read_metadata(
        tx: &tokio_postgres::Transaction<'_>,
    ) -> Result<(Discovery, Vec<u32>), Error> {
        let schemas = tx
            .query(include_str!("sql/schemas.sql"), &[])
            .await
            .map_err(|e| postgres_error("discover schemas", e))?
            .iter()
            .map(|r| r.get(0))
            .collect();
        let mut tables = Vec::new();
        let mut positions = BTreeMap::new();
        for row in tx
            .query(include_str!("sql/tables.sql"), &[])
            .await
            .map_err(|e| postgres_error("discover tables", e))?
        {
            let oid: u32 = row.get(0);
            positions.insert(oid, tables.len());
            tables.push(Table {
                schema: row.get(1),
                name: row.get(2),
                partitioned: row.get(3),
                estimated_rows: row.get(4),
                columns: Vec::new(),
                primary_key: Vec::new(),
                indexes: Vec::new(),
            });
        }
        let ids: Vec<u32> = positions.keys().copied().collect();
        for row in tx
            .query(include_str!("sql/columns.sql"), &[&ids])
            .await
            .map_err(|e| postgres_error("discover columns", e))?
        {
            let oid: u32 = row.get(0);
            if let Some(&position) = positions.get(&oid) {
                tables[position].columns.push(Column {
                    name: row.get(1),
                    data_type: row.get(2),
                    nullable: row.get(3),
                    archive_time_candidate: row.get(4),
                });
            }
        }
        for row in tx
            .query(include_str!("sql/indexes.sql"), &[&ids])
            .await
            .map_err(|e| postgres_error("discover indexes", e))?
        {
            let oid: u32 = row.get(0);
            if let Some(&position) = positions.get(&oid) {
                let index = Index {
                    name: row.get(1),
                    method: row.get(2),
                    unique: row.get(3),
                    primary: row.get(4),
                    valid: row.get(5),
                    columns: row.get(6),
                    included_columns: row.get(7),
                    has_expressions: row.get(8),
                    partial: row.get(9),
                };
                if index.primary {
                    tables[position].primary_key = index.columns.clone();
                }
                tables[position].indexes.push(index);
            }
        }
        // Match IDs to the same order as tables so analysis can reuse this snapshot.
        let mut ordered_ids = vec![0; tables.len()];
        for (oid, position) in positions {
            ordered_ids[position] = oid;
        }
        Ok((Discovery { schemas, tables }, ordered_ids))
    }

    async fn analyze_inner(&self) -> Result<coldctl_core::analysis::Analysis, Error> {
        let mut session = self.connect().await?;
        let tx = session
            .client
            .build_transaction()
            .read_only(true)
            .isolation_level(IsolationLevel::RepeatableRead)
            .start()
            .await
            .map_err(|e| postgres_error("analyze", e))?;
        let (discovery, ids) = Self::read_metadata(&tx).await?;
        let names: BTreeMap<_, _> = ids
            .iter()
            .copied()
            .zip(
                discovery
                    .tables
                    .iter()
                    .map(|t| (t.schema.clone(), t.name.clone())),
            )
            .collect();
        let mut statistics = BTreeMap::new();
        for row in tx
            .query(include_str!("sql/analysis.sql"), &[&ids])
            .await
            .map_err(|e| postgres_error("analyze statistics", e))?
        {
            let oid: u32 = row.get(0);
            if let Some(name) = names.get(&oid) {
                statistics.insert(
                    name.clone(),
                    coldctl_core::analysis::TableStatistics {
                        total_bytes: row.get(1),
                        table_bytes: row.get(2),
                        index_bytes: row.get(3),
                        last_analyzed_at: row.get(4),
                        estimated_changes_since_analyze: row.get(5),
                    },
                );
            }
        }
        tx.commit()
            .await
            .map_err(|e| postgres_error("analyze", e))?;
        Ok(coldctl_core::analysis::assess(discovery, statistics))
    }
    async fn discover_inner(&self) -> Result<Discovery, Error> {
        let mut session = self.connect().await?;
        let tx = session
            .client
            .build_transaction()
            .read_only(true)
            .isolation_level(IsolationLevel::RepeatableRead)
            .start()
            .await
            .map_err(|e| postgres_error("discover", e))?;
        let (discovery, _) = Self::read_metadata(&tx).await?;
        tx.commit()
            .await
            .map_err(|e| postgres_error("discover", e))?;
        Ok(discovery)
    }
}

impl DataSource for PostgresSource {
    async fn analyze(&self) -> Result<coldctl_core::analysis::Analysis, Error> {
        timeout(Duration::from_secs(30), self.analyze_inner())
            .await
            .map_err(|_| {
                failure(
                    "analyze",
                    "analysis timed out after 30 seconds; retry when the database is less busy",
                )
            })?
    }
    async fn test_connection(&self) -> Result<ConnectionInfo, Error> {
        let session = self.connect().await?;
        let row = timeout(Duration::from_secs(10), session.client.query_one(
            "SELECT current_database()::text, current_user::text, current_setting('server_version')", &[]))
            .await.map_err(|_| failure("test", "query timed out after 10 seconds"))?
            .map_err(|e| postgres_error("test", e))?;
        Ok(ConnectionInfo {
            database: row.get(0),
            user: row.get(1),
            server_version: row.get(2),
        })
    }

    async fn discover(&self) -> Result<Discovery, Error> {
        timeout(Duration::from_secs(30), self.discover_inner())
            .await
            .map_err(|_| {
                failure(
                    "discover",
                    "discovery timed out after 30 seconds; retry when the database is less busy",
                )
            })?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires COLDCTL_TEST_POSTGRES_URL pointing to a disposable PostgreSQL database"]
    async fn live_sessions_are_read_only_and_auth_errors_are_redacted() {
        let source = PostgresSource::new(
            coldctl_core::source::SourceConnection::from_url_env(
                "COLDCTL_TEST_POSTGRES_URL".into(),
            )
            .unwrap()
            .resolve()
            .unwrap(),
        );
        let session = source.connect().await.unwrap();
        let row = session
            .client
            .query_one("SHOW transaction_read_only", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "on");
        let row = session
            .client
            .query_one("SHOW statement_timeout", &[])
            .await
            .unwrap();
        assert_eq!(row.get::<_, String>(0), "10s");

        let resolved = source.connection.clone();
        let mut config = Config::new();
        config
            .host(&resolved.host)
            .port(resolved.port)
            .user(&resolved.user)
            .dbname(&resolved.database)
            .password("COLDCTL_INTENTIONALLY_WRONG_PASSWORD")
            .ssl_mode(SslMode::Disable)
            .connect_timeout(Duration::from_secs(5));
        let error = match config.connect(tokio_postgres::NoTls).await {
            Ok(_) => panic!("integration server must require password authentication"),
            Err(error) => postgres_error("connect", error),
        };
        assert!(error.to_string().contains("authentication rejected"));
        assert!(!format!("{error:?}").contains("COLDCTL_INTENTIONALLY_WRONG_PASSWORD"));
    }
}
