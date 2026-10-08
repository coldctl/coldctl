use crate::{Result, Validate, ensure, identifier, model::ConnectorPin};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}
pub const CURRENT: ProtocolVersion = ProtocolVersion { major: 1, minor: 0 };
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCapabilities {
    pub analyze: bool,
    pub encodings: BTreeSet<String>,
    pub cursor_versions: BTreeSet<u32>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SinkCapabilities {
    pub publish_if_absent: bool,
    pub reconcile: bool,
    pub range_read: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreCapabilities {
    pub transactional_checkpoint: bool,
    pub value_validation: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    pub source: Option<SourceCapabilities>,
    pub sink: Option<SinkCapabilities>,
    pub restore: Option<RestoreCapabilities>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub protocol: ProtocolVersion,
    pub connector: ConnectorPin,
    pub features: BTreeSet<String>,
    pub required_host_features: BTreeSet<String>,
    pub capabilities: Capabilities,
}
impl Validate for Hello {
    fn validate(&self) -> Result<()> {
        self.connector.validate()?;
        ensure(
            self.features.len() <= 64 && self.required_host_features.len() <= 64,
            "too many protocol features",
        )?;
        for f in self.features.iter().chain(&self.required_host_features) {
            identifier(f)?;
        }
        let c = &self.capabilities;
        ensure(
            c.source.is_some() || c.sink.is_some() || c.restore.is_some(),
            "connector has no capabilities",
        )?;
        if let Some(s) = &c.source {
            ensure(
                !s.encodings.is_empty()
                    && s.encodings.len() <= 16
                    && !s.cursor_versions.is_empty()
                    && s.cursor_versions.len() <= 16
                    && !s.cursor_versions.contains(&0),
                "invalid source capabilities",
            )?;
            for e in &s.encodings {
                identifier(e)?;
            }
        }
        Ok(())
    }
}
/// Both sides must advertise support for all features required by the other.
pub fn negotiate(
    hello: &Hello,
    host_features: &BTreeSet<String>,
    required_connector_features: &BTreeSet<String>,
) -> Result<ProtocolVersion> {
    hello.validate()?;
    ensure(
        hello.protocol.major == CURRENT.major,
        "incompatible protocol major",
    )?;
    ensure(
        hello.required_host_features.is_subset(host_features)
            && required_connector_features.is_subset(&hello.features),
        "required protocol feature is missing",
    )?;
    negotiate_versions(CURRENT, hello.protocol)
}

pub fn negotiate_versions(host: ProtocolVersion, peer: ProtocolVersion) -> Result<ProtocolVersion> {
    ensure(host.major == peer.major, "incompatible protocol major")?;
    Ok(ProtocolVersion {
        major: host.major,
        minor: host.minor.min(peer.minor),
    })
}
