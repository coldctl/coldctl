use crate::archive::key::ArchiveKey;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveColumn {
    pub name: String,
    pub postgres_type: String,
    pub nullable: bool,
}

/// At most 1,000 rows. Relational values retain native text/hex with SQL NULL
/// preserved separately; document envelopes retain raw BSON as canonical hex.
/// Each connector must enforce the 64 KiB row transport budget before fetching payloads.
#[derive(Serialize, Deserialize)]
pub struct DataBatch {
    pub rows: Vec<Vec<Option<String>>>,
    pub last_key: ArchiveKey,
}
