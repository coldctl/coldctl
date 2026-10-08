//! Control envelopes only. Process transport and binary chunk decoding land in phase 2.
use crate::{Result, Validate, ensure};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Test,
    Discover,
    Analyze,
    Plan,
    OpenScan,
    ReadBatch,
    ValidateResume,
    Close,
    Probe,
    Stage,
    Publish,
    Inspect,
    Read,
    Reconcile,
    Abort,
    RestorePreflight,
    CreateTarget,
    RestoreProgress,
    ApplyBatch,
    ValidateTarget,
    Finalize,
    Cancel,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub request_id: u64,
    pub session_id: String,
    pub operation: Operation,
    pub deadline_ms: u32,
    pub body: serde_json::Value,
}
impl Validate for Request {
    fn validate(&self) -> Result<()> {
        ensure(
            self.request_id > 0 && self.deadline_ms > 0 && self.deadline_ms <= 300_000,
            "invalid request ID or deadline",
        )?;
        ensure(
            !self.session_id.is_empty()
                && self.session_id.len() <= 64
                && self
                    .session_id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "invalid session ID",
        )?;
        ensure(self.body.is_object(), "request body must be an object")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    Configuration,
    Authentication,
    Permission,
    Unsupported,
    Timeout,
    Busy,
    Corruption,
    IdentityChanged,
    Protocol,
    OutcomeUnknown,
    Unavailable,
}
/// No free-form server message: the host renders safe text for the category.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Failure {
    pub code: ErrorCode,
    pub retryable: bool,
}

/// Fixed 24-byte header. Parsing performs no allocation and no IO.
pub const HEADER_BYTES: usize = 24;
pub const MAX_CHUNK_BYTES: u32 = 1024 * 1024;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Control,
    Data,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub kind: FrameKind,
    pub request_id: u64,
    pub payload_bytes: u32,
}
impl FrameHeader {
    pub fn encode(self) -> Result<[u8; HEADER_BYTES]> {
        ensure(
            self.payload_bytes > 0 && self.payload_bytes <= MAX_CHUNK_BYTES,
            "invalid frame length",
        )?;
        let mut bytes = [0u8; HEADER_BYTES];
        bytes[..4].copy_from_slice(b"CCTL");
        bytes[4..6].copy_from_slice(&1u16.to_be_bytes());
        bytes[8] = match self.kind {
            FrameKind::Control => 1,
            FrameKind::Data => 2,
        };
        bytes[12..20].copy_from_slice(&self.request_id.to_be_bytes());
        bytes[20..24].copy_from_slice(&self.payload_bytes.to_be_bytes());
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure(bytes.len() == HEADER_BYTES, "truncated frame header")?;
        ensure(
            &bytes[..4] == b"CCTL" && bytes[4..8] == [0, 1, 0, 0] && bytes[9..12] == [0, 0, 0],
            "unsupported frame header",
        )?;
        let kind = match bytes[8] {
            1 => FrameKind::Control,
            2 => FrameKind::Data,
            _ => return Err(crate::ContractError("unknown frame kind")),
        };
        let header = Self {
            kind,
            request_id: u64::from_be_bytes(bytes[12..20].try_into().unwrap()),
            payload_bytes: u32::from_be_bytes(bytes[20..24].try_into().unwrap()),
        };
        header.encode()?;
        Ok(header)
    }
}
