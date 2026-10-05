use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveColumn {
    pub name: String,
    pub postgres_type: String,
    pub nullable: bool,
}

/// At most 1,000 rows; each row's JSON text representation is capped server-side at 64 KiB.
/// Values are PostgreSQL text representations, with SQL NULL preserved separately.
pub struct DataBatch {
    pub rows: Vec<Vec<Option<String>>>,
    pub last_key: i64,
}
