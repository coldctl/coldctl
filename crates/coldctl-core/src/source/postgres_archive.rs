//! Explicit legacy facade over the supervised source connector.
use super::{
    ArchiveSource, SourceConnection,
    connector::{Client, Request, launch_pinned},
};
use crate::archive::key::ArchiveKey;
use crate::{
    archive::{
        batch::DataBatch,
        controls::ExecutionControls,
        planner::{ArchivePlan, SourceIdentity},
    },
    error::Error,
    policy::model::Policy,
};
use std::path::PathBuf;
use tokio::sync::Mutex;
pub struct PostgresArchive {
    client: Mutex<Client>,
    identity: String,
    deadline: u32,
}
impl PostgresArchive {
    pub async fn connect(connection: SourceConnection) -> Result<Self, Error> {
        Self::connect_pinned(connection, None).await
    }
    pub(crate) async fn connect_pinned(
        connection: SourceConnection,
        pin: Option<&coldctl_connector_protocol::model::ConnectorPin>,
    ) -> Result<Self, Error> {
        let mut client = launch_pinned(&connection, pin).await?;
        let identity = client.call(Request::OpenScan, 15_000, false).await?;
        Ok(Self {
            client: Mutex::new(client),
            identity,
            deadline: 35_000,
        })
    }
    pub(crate) async fn connect_in_state(
        connection: SourceConnection,
        paths: &crate::paths::StatePaths,
        pin: Option<&coldctl_connector_protocol::model::ConnectorPin>,
    ) -> Result<Self, Error> {
        let mut client = super::connector::launch_in_state(&connection, paths, pin).await?;
        let identity = client.call(Request::OpenScan, 15_000, false).await?;
        Ok(Self {
            client: Mutex::new(client),
            identity,
            deadline: 35_000,
        })
    }
    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }
    pub(crate) async fn configure_timeouts(
        &mut self,
        controls: &ExecutionControls,
    ) -> Result<(), Error> {
        controls.validate()?;
        self.client
            .lock()
            .await
            .call::<_, ()>(Request::ConfigureTimeouts(controls.clone()), 15_000, false)
            .await?;
        self.deadline = controls.query_timeout_ms.saturating_add(5000).min(300_000);
        Ok(())
    }
    pub async fn plan(
        &self,
        policy: Policy,
        destination_path: PathBuf,
    ) -> Result<ArchivePlan, Error> {
        let mut client = self.client.lock().await;
        let mut plan: ArchivePlan = client
            .call(
                Request::Plan {
                    policy,
                    destination: destination_path,
                },
                self.deadline,
                false,
            )
            .await?;
        plan.connector_pin = Some(client.pin.clone());
        let valid_columns = match client.pin.id.as_str() {
            "postgres" => plan
                .columns
                .iter()
                .all(|column| supported(&column.postgres_type)),
            "mysql" => plan
                .columns
                .iter()
                .all(|column| crate::source::mysql_types::parts(&column.postgres_type).is_ok()),
            "mongodb" => {
                plan.columns == crate::format::bson::columns() && plan.primary_key == "_id"
            }
            _ => false,
        };
        if plan.columns.is_empty() || !valid_columns {
            return Err(Error::Archive(
                "connector returned incompatible archive columns",
            ));
        }
        Ok(plan)
    }
    pub(crate) async fn source_identity(&self, table_oid: u32) -> Result<SourceIdentity, Error> {
        Ok(self
            .client
            .lock()
            .await
            .call(Request::SourceIdentity { table_oid }, self.deadline, false)
            .await?)
    }
    pub(crate) async fn validate_resume(&mut self, plan: &ArchivePlan) -> Result<(), Error> {
        let mut client = self.client.lock().await;
        if plan
            .connector_pin
            .as_ref()
            .is_some_and(|pin| pin != &client.pin)
        {
            return Err(Error::Archive(
                "connector version or digest changed; restore the pinned executable before resume",
            ));
        }
        client
            .call::<_, ()>(Request::ValidateResume(plan.clone()), self.deadline, false)
            .await?;
        Ok(())
    }
}
impl ArchiveSource for PostgresArchive {
    async fn upper_key(&mut self, plan: &ArchivePlan) -> Result<Option<ArchiveKey>, Error> {
        let upper: Option<ArchiveKey> = self
            .client
            .lock()
            .await
            .call(Request::UpperKey(plan.clone()), self.deadline, false)
            .await?;
        if upper.is_some_and(|key| {
            !key.matches_engine(
                plan.connector_pin
                    .as_ref()
                    .map_or("postgres", |p| p.id.as_str()),
            )
        }) {
            return Err(Error::Archive(
                "connector returned an incompatible upper key",
            ));
        }
        Ok(upper)
    }
    async fn read_batch(
        &mut self,
        plan: &ArchivePlan,
        last: Option<ArchiveKey>,
        upper: ArchiveKey,
    ) -> Result<Option<DataBatch>, Error> {
        let batch: Option<DataBatch> = self
            .client
            .lock()
            .await
            .call(
                Request::ReadBatch {
                    plan: plan.clone(),
                    last,
                    upper,
                    rows: plan.policy.config.batch_size,
                    bytes: 32 * 1024 * 1024,
                },
                self.deadline,
                false,
            )
            .await?;
        if let Some(batch) = &batch {
            if batch.rows.is_empty()
                || batch.rows.len() > plan.policy.config.batch_size as usize
                || !batch.last_key.within(upper)
                || last.is_some_and(|last| !batch.last_key.follows(last))
            {
                return Err(Error::Archive("connector batch out of order or bounds"));
            }
            let key = plan
                .columns
                .iter()
                .position(|c| c.name == plan.primary_key)
                .ok_or(Error::Archive("connector key missing"))?;
            let mut previous = last;
            let mut bytes = 0usize;
            for row in &batch.rows {
                if row.len() != plan.columns.len() {
                    return Err(Error::Archive("connector row width mismatch"));
                }
                let value = row[key]
                    .as_deref()
                    .ok_or(Error::Archive("connector key missing"))?;
                let current = if plan.columns[key].postgres_type == crate::format::bson::ID_TYPE {
                    if plan.columns != crate::format::bson::columns() {
                        return Err(Error::Archive("invalid BSON columns"));
                    }
                    crate::format::bson::validate_eligibility(
                        row,
                        &plan.policy.config.time_column,
                        &plan.cutoff_utc,
                    )?;
                    crate::format::bson::key(value)?
                } else {
                    ArchiveKey::from(crate::source::mysql_types::cursor(
                        &plan.columns[key].postgres_type,
                        value,
                    )?)
                };
                if previous.is_some_and(|v| !current.follows(v)) || !current.within(upper) {
                    return Err(Error::Archive("connector batch out of order or bounds"));
                }
                previous = Some(current);
                let size = row.iter().flatten().map(String::len).sum::<usize>();
                bytes = bytes.saturating_add(size);
                if size > 65536
                    || bytes > 32 * 1024 * 1024
                    || row
                        .iter()
                        .zip(&plan.columns)
                        .any(|(v, c)| v.is_none() && !c.nullable)
                {
                    return Err(Error::Archive(
                        "connector batch exceeds credit or nullability contract",
                    ));
                }
            }
            if previous != Some(batch.last_key) {
                return Err(Error::Archive("connector cursor mismatch"));
            }
        }
        Ok(batch)
    }
}
pub fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
pub(crate) fn supported(kind: &str) -> bool {
    matches!(
        kind,
        "int2"
            | "int4"
            | "int8"
            | "bool"
            | "float4"
            | "float8"
            | "text"
            | "varchar"
            | "bpchar"
            | "numeric"
            | "date"
            | "timestamp"
            | "timestamptz"
            | "uuid"
            | "json"
            | "jsonb"
    )
}
