use super::{Result, fail};
use coldctl_connector_protocol::{
    Validate,
    capability::{CURRENT, Hello, negotiate},
    model::ConnectorPin,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const MAX_FILE: u64 = 256 * 1024 * 1024;
pub const MAX_CATALOG: u64 = 4 * 1024 * 1024;
pub fn platform() -> &'static str {
    if cfg!(all(
        target_os = "windows",
        target_arch = "x86_64",
        target_env = "msvc"
    )) {
        "x86_64-pc-windows-msvc"
    } else if cfg!(all(
        target_os = "linux",
        target_arch = "x86_64",
        target_env = "gnu"
    )) {
        "x86_64-unknown-linux-gnu"
    } else {
        "unsupported"
    }
}
pub fn filename(s: &str) -> bool {
    let stem = s.split('.').next().unwrap_or("").to_ascii_uppercase();
    let reserved = [
        "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
        "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
    ];
    !reserved.contains(&stem.as_str())
        && !s.ends_with('.')
        && !s.is_empty()
        && s.len() <= 180
        && !s.starts_with('.')
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        && !s.contains("..")
}
pub fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileEntry {
    pub name: String,
    pub target: String,
    pub length: u64,
    pub sha256: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Package {
    pub hello: Hello,
    pub platform: String,
    pub agent: String,
    pub entrypoint: String,
    pub files: Vec<FileEntry>,
    pub requirements: Vec<String>,
}
impl Package {
    pub fn pin(&self) -> &ConnectorPin {
        &self.hello.connector
    }
    pub fn validate(&self) -> Result<()> {
        self.hello
            .validate()
            .map_err(|_| fail("invalid signed connector capabilities"))?;
        let version = semver::Version::parse(&self.pin().version)
            .map_err(|_| fail("invalid package version"))?;
        if !version.pre.is_empty()
            || !version.build.is_empty()
            || version.to_string() != self.pin().version
        {
            return Err(fail("packages require canonical stable semantic versions"));
        }
        if !["postgres", "mysql", "mongodb", "s3"].contains(&self.pin().id.as_str()) {
            return Err(fail("unsupported official connector ID"));
        }
        if !["x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu"].contains(&self.platform.as_str())
        {
            return Err(fail("unsupported package platform"));
        }
        semver::VersionReq::parse(&self.agent)
            .map_err(|_| fail("invalid agent compatibility requirement"))?;
        let executable = if self.platform.contains("windows") {
            "connector.exe"
        } else {
            "connector"
        };
        let expected = BTreeSet::from([
            executable,
            "LICENSE",
            "config-schema.json",
            "dependencies.json",
        ]);
        let names = self
            .files
            .iter()
            .map(|f| f.name.as_str())
            .collect::<BTreeSet<_>>();
        if self.entrypoint != executable || self.files.len() != 4 || names != expected {
            return Err(fail(
                "package must contain exactly the executable, license, configuration schema and dependency inventory",
            ));
        }
        let mut targets = BTreeSet::new();
        let mut total = 0u64;
        for file in &self.files {
            if !filename(&file.target)
                || !targets.insert(&file.target)
                || !digest(&file.sha256)
                || file.length == 0
                || file.length > MAX_FILE
                || (file.name != executable && file.length > MAX_CATALOG)
            {
                return Err(fail("invalid package target, digest or size"));
            }
            total = total
                .checked_add(file.length)
                .ok_or(fail("package too large"))?;
            if file.name == executable && file.sha256 != self.pin().sha256 {
                return Err(fail("executable digest differs from connector pin"));
            }
        }
        if total > MAX_FILE + 3 * MAX_CATALOG
            || self.requirements.len() > 16
            || self
                .requirements
                .iter()
                .any(|r| r.len() > 256 || r.chars().any(char::is_control))
        {
            return Err(fail("package limits exceeded"));
        }
        Ok(())
    }
    pub fn compatible(&self) -> Result<()> {
        self.validate()?;
        let features = crate::source::connector::host_features();
        if self.platform != platform()
            || self.hello.protocol.major != CURRENT.major
            || negotiate(&self.hello, &features, &BTreeSet::new()).is_err()
            || !semver::VersionReq::parse(&self.agent)
                .map_err(|_| fail("invalid agent requirement"))?
                .matches(
                    &semver::Version::parse(env!("CARGO_PKG_VERSION"))
                        .map_err(|_| fail("invalid agent version"))?,
                )
        {
            return Err(fail(
                "connector package is incompatible with this platform, agent or protocol",
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub format: u32,
    pub packages: Vec<Package>,
    pub revoked: Vec<String>,
}
impl Catalog {
    pub fn validate(&self) -> Result<()> {
        if self.format != 1
            || self.packages.len() > 256
            || self.revoked.len() > 4096
            || self.revoked.iter().any(|s| !digest(s))
        {
            return Err(fail("invalid signed connector catalog"));
        }
        let mut ids = BTreeSet::new();
        for p in &self.packages {
            p.validate()?;
            if !ids.insert((&p.pin().id, &p.pin().version, &p.platform)) {
                return Err(fail("duplicate package identity in signed catalog"));
            }
        }
        Ok(())
    }
}
