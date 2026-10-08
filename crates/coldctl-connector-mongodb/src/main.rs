mod mongo;
mod restore;
use coldctl_connector_protocol::{capability::*, model::ConnectorPin};
use coldctl_connector_runtime::{self as runtime, Call, Reply, transport};
pub use coldctl_core::source::config;
pub use coldctl_core::source::{
    ArchiveSource, Column, ConnectionInfo, DataSource, Discovery, Index, Table,
};
use coldctl_core::{
    error::{Error, FailureCategory},
    source::{config::ResolvedConnection, connector::Request},
};
use serde::Serialize;
use std::collections::BTreeSet;

fn value<T: Serialize>(value: T) -> Result<serde_json::Value, Error> {
    serde_json::to_value(value).map_err(|_| Error::Archive("connector serialization failed"))
}
fn classify(error: &Error) -> runtime::Error {
    if let Error::Archive(message) = error {
        if message.contains("authentication") {
            return runtime::Error::Authentication;
        }
        if message.contains("permission") {
            return runtime::Error::Permission;
        }
        if message.contains("timeout") {
            return runtime::Error::Timeout;
        }
        if message.contains("connection or TLS") {
            return runtime::Error::Unavailable;
        }
        if message.contains("byte limit") {
            return runtime::Error::OversizedRow;
        }
        if message.contains("unsupported column") {
            return runtime::Error::UnsupportedColumn;
        }
        if message.contains("row-level security") {
            return runtime::Error::RowSecurity;
        }
    }

    match error.failure().category {
        FailureCategory::Authentication => runtime::Error::Authentication,
        FailureCategory::Permission => runtime::Error::Permission,
        FailureCategory::Timeout => runtime::Error::Timeout,
        FailureCategory::Schema => runtime::Error::Schema,
        FailureCategory::Connectivity => runtime::Error::Unavailable,
        FailureCategory::OversizedRow => runtime::Error::OversizedRow,
        _ => runtime::Error::Configuration,
    }
}
// Row credit is allowed to shrink for --max-rows without changing the frozen scan.
fn scan_contract(plan: &coldctl_core::archive::planner::ArchivePlan) -> serde_json::Result<String> {
    let mut contract = plan.clone();
    contract.policy.config.batch_size = 1;
    serde_json::to_string(&contract)
}
#[derive(Default)]
struct Server {
    connection: Option<ResolvedConnection>,
    scan: Option<mongo::Mongo>,
    restore: Option<restore::RestoreSession>,
    bound: Option<(String, Option<coldctl_core::archive::key::ArchiveKey>)>,
}
impl Server {
    fn connection(&self) -> Result<ResolvedConnection, Error> {
        self.connection
            .clone()
            .ok_or(Error::Archive("connector not configured"))
    }
    async fn dispatch(&mut self, request: Request) -> Result<serde_json::Value, Error> {
        match request {
            Request::Configure(connection) => {
                if self.connection.is_some()
                    || connection.port == 0
                    || [&connection.host, &connection.database, &connection.user]
                        .iter()
                        .any(|s| s.is_empty() || s.len() > 1024 || s.chars().any(char::is_control))
                    || connection
                        .password
                        .as_ref()
                        .is_some_and(|p| p.len() > 65536)
                {
                    return Err(Error::Archive("invalid connector configuration"));
                }
                self.connection = Some(connection);
                value(())
            }
            Request::Test => value(
                mongo::MongoSource::new(self.connection()?)
                    .test_connection()
                    .await?,
            ),
            Request::Discover => value(
                mongo::MongoSource::new(self.connection()?)
                    .discover()
                    .await?,
            ),
            Request::Analyze => value(
                mongo::MongoSource::new(self.connection()?)
                    .analyze()
                    .await?,
            ),
            Request::OpenScan => {
                if self.scan.is_some() || self.restore.is_some() {
                    return Err(Error::Archive("session already opened"));
                }
                let scan = mongo::Mongo::connect(self.connection()?).await?;
                let identity = scan.identity().to_owned();
                self.scan = Some(scan);
                value(identity)
            }
            Request::ConfigureTimeouts(controls) => {
                self.scan
                    .as_mut()
                    .ok_or(Error::Archive("scan not open"))?
                    .configure_timeouts(&controls)
                    .await?;
                value(())
            }
            Request::Plan {
                policy,
                destination,
            } => value(
                self.scan
                    .as_mut()
                    .ok_or(Error::Archive("scan not open"))?
                    .plan(policy, destination)
                    .await?,
            ),
            Request::SourceIdentity { table_oid } => value(
                self.scan
                    .as_mut()
                    .ok_or(Error::Archive("scan not open"))?
                    .source_identity(table_oid)
                    .await?,
            ),
            Request::ValidateResume(plan) => {
                plan.policy.config.validate()?;
                if plan.delete {
                    return Err(Error::Archive("delete unsupported"));
                }
                self.scan
                    .as_mut()
                    .ok_or(Error::Archive("scan not open"))?
                    .validate_resume(&plan)
                    .await?;
                value(())
            }
            Request::UpperKey(plan) => {
                plan.policy.config.validate()?;
                if plan.delete {
                    return Err(Error::Archive("delete unsupported"));
                }
                let upper = self
                    .scan
                    .as_mut()
                    .ok_or(Error::Archive("scan not open"))?
                    .upper_key(&plan)
                    .await?;
                self.bound = Some((
                    scan_contract(&plan).map_err(|_| Error::Archive("invalid plan"))?,
                    upper,
                ));
                value(upper)
            }
            Request::ReadBatch {
                plan,
                last,
                upper,
                rows,
                bytes,
            } => {
                plan.policy.config.validate()?;
                if plan.delete
                    || rows <= 0
                    || rows > 1000
                    || rows != plan.policy.config.batch_size
                    || bytes != 32 * 1024 * 1024
                    || last.is_some_and(|v| !v.within(upper))
                {
                    return Err(Error::Archive("invalid scan credit"));
                }
                let encoded = scan_contract(&plan).map_err(|_| Error::Archive("invalid plan"))?;
                if self
                    .bound
                    .as_ref()
                    .is_some_and(|(p, u)| p != &encoded || *u != Some(upper))
                {
                    return Err(Error::Archive("scan contract changed"));
                }
                self.bound = Some((encoded, Some(upper)));
                let batch = self
                    .scan
                    .as_mut()
                    .ok_or(Error::Archive("scan not open"))?
                    .read_batch(&plan, last, upper)
                    .await?;
                if batch.as_ref().is_some_and(|b| {
                    b.rows
                        .iter()
                        .flatten()
                        .flatten()
                        .map(String::len)
                        .sum::<usize>()
                        > bytes as usize
                }) {
                    return Err(Error::Archive("batch exceeds byte credit"));
                }
                value(batch)
            }
            Request::RestoreOpen(spec) => {
                if self.restore.is_some() || self.scan.is_some() {
                    return Err(Error::Archive("session already opened"));
                }
                self.restore = Some(restore::RestoreSession::open(self.connection()?, spec).await?);
                value(())
            }
            Request::RestoreBegin { verify_count } => value(
                self.restore
                    .as_mut()
                    .ok_or(Error::Archive("restore not open"))?
                    .begin(verify_count)
                    .await?,
            ),
            Request::RestoreValidate { rows } => {
                self.restore
                    .as_mut()
                    .ok_or(Error::Archive("restore not open"))?
                    .validate(&rows)
                    .await?;
                value(())
            }
            Request::RestoreApply { sequence, rows } => {
                self.restore
                    .as_mut()
                    .ok_or(Error::Archive("restore not open"))?
                    .apply(sequence, &rows)
                    .await?;
                value(())
            }
            Request::RestoreFinish { completed } => {
                self.restore
                    .as_mut()
                    .ok_or(Error::Archive("restore not open"))?
                    .finish(completed)
                    .await?;
                value(())
            }
            Request::Close | Request::Cancel => {
                if let Some(restore) = self.restore.as_mut() {
                    restore.abort().await;
                }
                self.scan = None;
                self.restore = None;
                self.connection = None;
                value(())
            }
        }
    }
}
#[tokio::main]
async fn main() {
    // stdout is protocol-only; even failures never print database/credential text.
    if serve().await.is_err() {
        std::process::exit(1)
    }
}
async fn serve() -> runtime::Result<()> {
    let pin = ConnectorPin {
        id: "mongodb".into(),
        version: env!("CARGO_PKG_VERSION").into(),
        sha256: runtime::digest(
            &std::env::current_exe().map_err(|_| runtime::Error::Unavailable)?,
        )?,
    };
    let features = BTreeSet::from(["mongodb-bson-v1".into()]);
    let hello = Hello {
        protocol: CURRENT,
        connector: pin,
        features: features.clone(),
        required_host_features: features,
        capabilities: Capabilities {
            source: Some(SourceCapabilities {
                analyze: true,
                encodings: BTreeSet::from(["mongodb-bson-hex-v1".into()]),
                cursor_versions: BTreeSet::from([1]),
            }),
            sink: None,
            restore: Some(RestoreCapabilities {
                transactional_checkpoint: true,
                value_validation: true,
            }),
        },
    };
    let mut input = tokio::io::stdin();
    let mut output = tokio::io::stdout();
    transport::send(&mut output, 0, &hello).await?;
    let mut server = Server::default();
    let mut session = None;
    let mut id = 1u64;
    loop {
        let request: Call<Request> = tokio::time::timeout(
            std::time::Duration::from_secs(300),
            transport::receive(&mut input, id),
        )
        .await
        .map_err(|_| runtime::Error::Timeout)??;
        if request.deadline_ms == 0
            || request.deadline_ms > 300_000
            || request.session.is_empty()
            || request.session.len() > 64
            || !request
                .session
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || session.as_ref().is_some_and(|s| s != &request.session)
        {
            return Err(runtime::Error::Protocol);
        }
        if id == 1 && !matches!(request.body, Request::Configure(_)) {
            return Err(runtime::Error::Protocol);
        }
        session = Some(request.session.clone());
        let close = matches!(request.body, Request::Close | Request::Cancel);
        let result = match tokio::time::timeout(
            std::time::Duration::from_millis(request.deadline_ms.into()),
            server.dispatch(request.body),
        )
        .await
        {
            Ok(result) => result.map_err(|e| classify(&e)),
            Err(_) => Err(runtime::Error::Timeout),
        };
        let failed = result.is_err();
        if failed {
            if let Some(restore) = server.restore.as_mut() {
                restore.abort().await;
            }
        }
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            transport::send(
                &mut output,
                id,
                &Reply {
                    session: request.session,
                    result,
                },
            ),
        )
        .await
        .map_err(|_| runtime::Error::Timeout)??;
        if failed || close {
            return Ok(());
        }
        id = id.checked_add(1).ok_or(runtime::Error::Protocol)?;
    }
}

#[cfg(test)]
mod tests;
