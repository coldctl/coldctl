use crate::{
    MAX_BATCH_BYTES, MAX_CURSOR_BYTES, MAX_DOCUMENT_BYTES, MAX_ROWS, Result, Validate, ensure,
    identifier, name,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DatasetKind {
    Table,
    Collection,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dataset {
    pub kind: DatasetKind,
    pub namespace: Vec<String>,
    pub name: String,
}
impl Validate for Dataset {
    fn validate(&self) -> Result<()> {
        ensure(self.namespace.len() <= 8, "too many namespace components")?;
        for part in &self.namespace {
            name(part)?;
        }
        name(&self.name)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    pub environment: String,
}
impl Validate for SecretRef {
    fn validate(&self) -> Result<()> {
        let s = &self.environment;
        ensure(
            !s.is_empty()
                && s.len() <= 128
                && s.bytes().enumerate().all(|(i, b)| {
                    b == b'_' || b.is_ascii_alphabetic() || (i > 0 && b.is_ascii_digit())
                }),
            "invalid credential environment reference",
        )
    }
}
// Deliberately not Debug: settings must still be checked by the connector's config schema.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorConfig {
    pub connector_id: String,
    pub config_version: u32,
    pub settings: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, SecretRef>,
}
impl Validate for ConnectorConfig {
    fn validate(&self) -> Result<()> {
        identifier(&self.connector_id)?;
        ensure(self.config_version > 0, "invalid configuration version")?;
        ensure(
            self.settings.len() <= 64 && self.secrets.len() <= 16,
            "too many configuration entries",
        )?;
        let mut bytes = 0usize;
        for (k, v) in &self.settings {
            name(k)?;
            bytes = bytes.saturating_add(k.len()).saturating_add(v.len());
        }
        ensure(bytes <= 64 * 1024, "configuration exceeds byte limit")?;
        for (k, v) in &self.secrets {
            name(k)?;
            v.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorPin {
    pub id: String,
    pub version: String,
    pub sha256: String,
}
impl Validate for ConnectorPin {
    fn validate(&self) -> Result<()> {
        identifier(&self.id)?;
        ensure(
            !self.version.is_empty()
                && self.version.len() <= 64
                && self
                    .version
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b".-+".contains(&b)),
            "invalid package version",
        )?;
        ensure(
            self.sha256.len() == 64
                && self
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid package digest",
        )
    }
}

/// Tokens are opaque and sensitive. They are never ordered or printed by the host.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cursor {
    pub connector_id: String,
    pub version: u32,
    pub encoding: String,
    pub token: Vec<u8>,
}
impl Validate for Cursor {
    fn validate(&self) -> Result<()> {
        identifier(&self.connector_id)?;
        identifier(&self.encoding)?;
        ensure(
            self.version > 0 && !self.token.is_empty() && self.token.len() <= MAX_CURSOR_BYTES,
            "invalid cursor version or size",
        )
    }
}

/// Integer strings never travel through a JSON floating-point number.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Key {
    Signed(String),
    Unsigned(String),
    ObjectId([u8; 12]),
}
impl Validate for Key {
    fn validate(&self) -> Result<()> {
        match self {
            Self::Signed(s) => ensure(
                s.parse::<i64>().is_ok_and(|v| v.to_string() == *s),
                "invalid signed key",
            ),
            Self::Unsigned(s) => ensure(
                s.parse::<u64>().is_ok_and(|v| v.to_string() == *s),
                "invalid unsigned key",
            ),
            Self::ObjectId(_) => Ok(()),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogicalType {
    Boolean,
    Signed,
    Unsigned,
    Float32Bits,
    Float64Bits,
    Decimal,
    Text,
    Binary,
    NativeText,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    pub logical_type: LogicalType,
    pub native_type: String,
    pub nullable: bool,
    pub parameters: BTreeMap<String, String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Value {
    Null,
    Boolean(bool),
    Signed(String),
    Unsigned(String),
    Float32Bits([u8; 4]),
    Float64Bits([u8; 8]),
    Decimal(String),
    Text(String),
    Binary(Vec<u8>),
    NativeText(String),
}
impl Value {
    fn size(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Boolean(_) => 1,
            Self::Float32Bits(_) => 4,
            Self::Float64Bits(_) => 8,
            Self::Signed(s)
            | Self::Unsigned(s)
            | Self::Decimal(s)
            | Self::Text(s)
            | Self::NativeText(s) => s.len(),
            Self::Binary(b) => b.len(),
        }
    }
    fn valid_for(&self, column: &Column) -> Result<()> {
        let matches = match self {
            Self::Null => column.nullable,
            Self::Boolean(_) => column.logical_type == LogicalType::Boolean,
            Self::Signed(s) => {
                Key::Signed(s.clone()).validate()?;
                column.logical_type == LogicalType::Signed
            }
            Self::Unsigned(s) => {
                Key::Unsigned(s.clone()).validate()?;
                column.logical_type == LogicalType::Unsigned
            }
            Self::Float32Bits(_) => column.logical_type == LogicalType::Float32Bits,
            Self::Float64Bits(_) => column.logical_type == LogicalType::Float64Bits,
            Self::Decimal(s) => {
                let digits = s.strip_prefix('-').unwrap_or(s);
                let parts: Vec<_> = digits.split('.').collect();
                ensure(
                    parts.len() <= 2
                        && parts.iter().all(|part| {
                            !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())
                        }),
                    "invalid decimal value",
                )?;
                column.logical_type == LogicalType::Decimal
            }
            Self::Text(_) => column.logical_type == LogicalType::Text,
            Self::Binary(_) => column.logical_type == LogicalType::Binary,
            Self::NativeText(_) => column.logical_type == LogicalType::NativeText,
        };
        ensure(matches, "value does not match column contract")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BatchPayload {
    Rows {
        columns: Vec<Column>,
        rows: Vec<Vec<Value>>,
    },
    /// Full BSON parsing and per-connector type/retention checks belong to the codec/connector.
    Bson { documents: Vec<Vec<u8>> },
}
impl BatchPayload {
    pub fn row_count(&self) -> usize {
        match self {
            Self::Rows { rows, .. } => rows.len(),
            Self::Bson { documents } => documents.len(),
        }
    }
}
impl Validate for BatchPayload {
    fn validate(&self) -> Result<()> {
        ensure(
            self.row_count() > 0 && self.row_count() <= MAX_ROWS,
            "invalid batch row count",
        )?;
        let mut bytes = 0usize;
        match self {
            Self::Rows { columns, rows } => {
                validate_columns(columns, true)?;
                for row in rows {
                    ensure(row.len() == columns.len(), "row width differs from schema")?;
                    let mut row_bytes = 0usize;
                    for (v, c) in row.iter().zip(columns) {
                        v.valid_for(c)?;
                        row_bytes = row_bytes.saturating_add(v.size());
                    }
                    ensure(row_bytes <= 64 * 1024, "SQL row exceeds value byte limit")?;
                    bytes = bytes.saturating_add(row_bytes);
                }
            }
            Self::Bson { documents } => {
                for doc in documents {
                    ensure(
                        doc.len() >= 5 && doc.len() <= MAX_DOCUMENT_BYTES,
                        "invalid BSON document size",
                    )?;
                    let size = i32::from_le_bytes(doc[..4].try_into().unwrap());
                    ensure(
                        size >= 5 && size as usize == doc.len() && doc.last() == Some(&0),
                        "invalid BSON envelope",
                    )?;
                    bytes = bytes.saturating_add(doc.len());
                }
            }
        }
        ensure(bytes <= MAX_BATCH_BYTES, "batch exceeds value byte limit")
    }
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub sequence: u64,
    pub payload: BatchPayload,
    pub next_cursor: Cursor,
}
impl Validate for Batch {
    fn validate(&self) -> Result<()> {
        ensure(self.sequence > 0, "invalid batch sequence")?;
        self.payload.validate()?;
        self.next_cursor.validate()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Consistency {
    ImmutableRows,
    QuiescentCopy,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Predicate {
    Before { field: String, utc_cutoff: String },
    Equal { field: String, value: Value },
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScanPlan {
    pub version: u32,
    pub connector: ConnectorPin,
    pub dataset: Dataset,
    pub consistency: Consistency,
    pub predicates: Vec<Predicate>,
    pub source_identity: BTreeMap<String, String>,
    pub key_fields: Vec<String>,
    pub value_encoding: String,
    pub columns: Vec<Column>,
    pub upper_bound: Option<Cursor>,
}
impl Validate for ScanPlan {
    fn validate(&self) -> Result<()> {
        ensure(self.version == 1, "unsupported scan plan version")?;
        self.connector.validate()?;
        self.dataset.validate()?;
        identifier(&self.value_encoding)?;
        ensure(
            !self.key_fields.is_empty() && self.key_fields.len() <= 16,
            "invalid key field count",
        )?;
        let mut keys = BTreeSet::new();
        for key in &self.key_fields {
            name(key)?;
            ensure(keys.insert(key), "duplicate key field")?;
        }
        validate_columns(&self.columns, self.dataset.kind == DatasetKind::Table)?;
        ensure(
            self.predicates.len() <= 16
                && !self.source_identity.is_empty()
                && self.source_identity.len() <= 32,
            "invalid plan metadata size",
        )?;
        for (k, v) in &self.source_identity {
            name(k)?;
            name(v)?;
        }
        for p in &self.predicates {
            match p {
                Predicate::Before { field, utc_cutoff } => {
                    name(field)?;
                    name(utc_cutoff)?;
                }
                Predicate::Equal { field, value } => {
                    name(field)?;
                    ensure(value.size() <= 4096, "predicate value exceeds byte limit")?;
                }
            }
        }
        if let Some(cursor) = &self.upper_bound {
            cursor.validate()?;
            ensure(
                cursor.connector_id == self.connector.id,
                "cursor belongs to another connector",
            )?;
        }
        Ok(())
    }
}

fn validate_columns(columns: &[Column], required: bool) -> Result<()> {
    ensure(
        (!required || !columns.is_empty()) && columns.len() <= 1024,
        "invalid column count",
    )?;
    let mut names = BTreeSet::new();
    let mut bytes = 0usize;
    for c in columns {
        name(&c.name)?;
        name(&c.native_type)?;
        ensure(names.insert(&c.name), "duplicate column name")?;
        ensure(c.parameters.len() <= 32, "too many type parameters")?;
        bytes = bytes
            .saturating_add(c.name.len())
            .saturating_add(c.native_type.len());
        for (k, v) in &c.parameters {
            name(k)?;
            name(v)?;
            bytes = bytes.saturating_add(k.len()).saturating_add(v.len());
        }
    }
    ensure(bytes <= 256 * 1024, "schema metadata exceeds byte limit")
}
