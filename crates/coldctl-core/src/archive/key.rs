//! Lossless durable scan keys. Legacy integer keys retain their JSON/SQLite encoding.
use crate::error::Error;
use rusqlite::types::{FromSql, FromSqlError, FromSqlResult, ToSql, ToSqlOutput, ValueRef};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ArchiveKey {
    Integer(i64),
    ObjectId(ObjectIdKey),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectIdKey {
    pub object_id: [u8; 12],
}

impl ArchiveKey {
    pub fn matches_engine(self, engine: &str) -> bool {
        matches!(
            (self, engine),
            (Self::Integer(_), "postgres" | "mysql") | (Self::ObjectId(_), "mongodb")
        )
    }
    pub fn object_id(bytes: [u8; 12]) -> Self {
        Self::ObjectId(ObjectIdKey { object_id: bytes })
    }

    pub fn integer(self) -> Result<i64, Error> {
        match self {
            Self::Integer(value) => Ok(value),
            _ => Err(Error::Archive(
                "integer connector received a non-integer cursor",
            )),
        }
    }

    /// A connector must never compare keys from different domains.
    pub fn follows(self, previous: Self) -> bool {
        self.partial_cmp(&previous) == Some(Ordering::Greater)
    }

    pub fn within(self, upper: Self) -> bool {
        matches!(
            self.partial_cmp(&upper),
            Some(Ordering::Less | Ordering::Equal)
        )
    }
}

impl From<i64> for ArchiveKey {
    fn from(value: i64) -> Self {
        Self::Integer(value)
    }
}

impl std::fmt::Display for ArchiveKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Integer(value) => write!(f, "{value}"),
            Self::ObjectId(value) => {
                for byte in value.object_id {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    }
}

impl PartialOrd for ArchiveKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.partial_cmp(b),
            (Self::ObjectId(a), Self::ObjectId(b)) => a.object_id.partial_cmp(&b.object_id),
            _ => None,
        }
    }
}

impl ToSql for ArchiveKey {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(match self {
            Self::Integer(value) => ToSqlOutput::from(*value),
            Self::ObjectId(value) => ToSqlOutput::Borrowed(ValueRef::Blob(&value.object_id)),
        })
    }
}

impl FromSql for ArchiveKey {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Integer(value) => Ok(Self::Integer(value)),
            ValueRef::Blob(bytes) => bytes
                .try_into()
                .map(Self::object_id)
                .map_err(|_| FromSqlError::InvalidType),
            _ => Err(FromSqlError::InvalidType),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_json_is_unchanged_and_domains_do_not_compare() {
        for value in [i64::MIN, -1, 0, i64::MAX] {
            let key = ArchiveKey::from(value);
            assert_eq!(serde_json::to_string(&key).unwrap(), value.to_string());
            assert_eq!(
                serde_json::from_str::<ArchiveKey>(&value.to_string()).unwrap(),
                key
            );
            let object = ArchiveKey::object_id([0; 12]);
            assert_eq!(key.partial_cmp(&object), None);
            assert!(!key.within(object));
            assert!(!object.follows(key));
        }
        assert!(serde_json::from_str::<ArchiveKey>("1.0").is_err());
        assert!(serde_json::from_str::<ArchiveKey>("18446744073709551615").is_err());
    }

    #[test]
    fn object_ids_keep_all_96_bits_across_json_and_sqlite() {
        let db = rusqlite::Connection::open_in_memory().unwrap();
        // Existing non-STRICT INTEGER-affinity columns preserve BLOBs verbatim.
        db.execute_batch("CREATE TABLE checkpoints (key INTEGER)")
            .unwrap();
        let keys = [
            ArchiveKey::from(i64::MIN),
            ArchiveKey::object_id([0; 12]),
            ArchiveKey::object_id([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
            ArchiveKey::object_id([255; 12]),
            ArchiveKey::from(i64::MAX),
        ];
        for key in keys {
            db.execute("INSERT INTO checkpoints VALUES (?1)", [key])
                .unwrap();
            let encoded = serde_json::to_vec(&key).unwrap();
            assert_eq!(serde_json::from_slice::<ArchiveKey>(&encoded).unwrap(), key);
        }
        let mut query = db
            .prepare("SELECT key FROM checkpoints ORDER BY rowid")
            .unwrap();
        let loaded = query
            .query_map([], |r| r.get::<_, ArchiveKey>(0))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(loaded, keys);
        assert!(keys[2].follows(keys[1]));
        assert!(keys[3].follows(keys[2]));
        for sql in ["SELECT x'00'", "SELECT '123'", "SELECT 1.0", "SELECT NULL"] {
            assert!(
                db.query_row(sql, [], |r| r.get::<_, ArchiveKey>(0))
                    .is_err()
            );
        }
    }
}
