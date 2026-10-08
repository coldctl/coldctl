//! Connector contract v1. DTO validation is not a transport or a plugin sandbox.
pub mod capability;
pub mod model;
pub mod wire;

use serde::de::DeserializeOwned;
use std::fmt;

pub const MAX_CONTROL_BYTES: usize = 1024 * 1024;
pub const MAX_CURSOR_BYTES: usize = 64 * 1024;
pub const MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BATCH_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_ROWS: usize = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractError(pub &'static str);
impl fmt::Display for ContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for ContractError {}
pub type Result<T> = std::result::Result<T, ContractError>;
pub trait Validate {
    fn validate(&self) -> Result<()>;
}

/// Apply a byte bound BEFORE JSON decoding. Binary batches use separate frames in phase 2.
pub fn decode_control<T: DeserializeOwned + Validate>(bytes: &[u8]) -> Result<T> {
    ensure(
        bytes.len() <= MAX_CONTROL_BYTES,
        "control message exceeds byte limit",
    )?;
    let value: T =
        serde_json::from_slice(bytes).map_err(|_| ContractError("invalid control message"))?;
    value.validate()?;
    Ok(value)
}
pub(crate) fn ensure(ok: bool, message: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(ContractError(message))
    }
}
pub(crate) fn identifier(s: &str) -> Result<()> {
    ensure(
        !s.is_empty()
            && s.len() <= 64
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
        "invalid connector or feature identifier",
    )
}
pub(crate) fn name(s: &str) -> Result<()> {
    ensure(
        !s.is_empty() && s.len() <= 1024 && !s.chars().any(char::is_control),
        "invalid dataset or field name",
    )
}
