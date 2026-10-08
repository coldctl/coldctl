//! A lossless BSON envelope: no document-to-JSON conversion or inferred columns.
use crate::{
    archive::{batch::ArchiveColumn, key::ArchiveKey},
    error::Error,
};
use bson::{RawBsonRef, RawDocument};
use std::collections::HashSet;

// Hex transport fits the existing 64 KiB row credit including the ObjectId.
pub const MAX_DOCUMENT_BYTES: usize = 30 * 1024;
pub const MAX_DEPTH: usize = 64;
pub const ID_TYPE: &str = "mongodb:objectid-v1";
pub const DOCUMENT_TYPE: &str = "mongodb:bson-hex-v1";

fn invalid() -> Error {
    Error::Archive("invalid BSON envelope, ObjectId, or top-level Date retention field")
}

pub fn columns() -> Vec<ArchiveColumn> {
    [("_id", ID_TYPE), ("_coldctl_bson", DOCUMENT_TYPE)]
        .into_iter()
        .map(|(name, kind)| ArchiveColumn {
            name: name.into(),
            postgres_type: kind.into(),
            nullable: false,
        })
        .collect()
}

pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}

pub fn unhex(value: &str, maximum: usize) -> Result<Vec<u8>, Error> {
    if value.len() % 2 != 0 || value.len() / 2 > maximum {
        return Err(invalid());
    }
    fn digit(value: u8) -> Result<u8, Error> {
        match value {
            b'0'..=b'9' => Ok(value - b'0'),
            b'a'..=b'f' => Ok(value - b'a' + 10),
            _ => Err(invalid()),
        }
    }
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok(digit(pair[0])? * 16 + digit(pair[1])?))
        .collect()
}

pub fn key(value: &str) -> Result<ArchiveKey, Error> {
    let bytes: [u8; 12] = unhex(value, 12)?.try_into().map_err(|_| invalid())?;
    Ok(ArchiveKey::object_id(bytes))
}

fn validate_document(document: &RawDocument, depth: usize) -> Result<(), Error> {
    if depth > MAX_DEPTH {
        return Err(invalid());
    }
    let mut names = HashSet::new();
    for entry in document {
        let (name, value) = entry.map_err(|_| invalid())?;
        if !names.insert(name) {
            return Err(invalid());
        }
        validate_value(value, depth)?;
    }
    Ok(())
}

fn validate_value(value: RawBsonRef<'_>, depth: usize) -> Result<(), Error> {
    if depth > MAX_DEPTH {
        return Err(invalid());
    }
    match value {
        RawBsonRef::Document(document) => validate_document(document, depth + 1)?,
        RawBsonRef::Array(array) => {
            for value in array {
                validate_value(value.map_err(|_| invalid())?, depth + 1)?;
            }
        }
        RawBsonRef::JavaScriptCodeWithScope(value) => validate_document(value.scope, depth + 1)?,
        _ => {}
    }
    Ok(())
}

/// Validate recursively while retaining the exact source bytes, including numeric bits.
pub fn inspect(bytes: &[u8], retention: &str) -> Result<(ArchiveKey, i64), Error> {
    if bytes.len() > MAX_DOCUMENT_BYTES {
        return Err(Error::Archive(
            "BSON document exceeds the 30 KiB archive byte limit",
        ));
    }
    if retention.is_empty() || retention == "_id" || retention.contains(['.', '$', '\0']) {
        return Err(invalid());
    }
    let document = RawDocument::from_bytes(bytes).map_err(|_| invalid())?;
    validate_document(document, 0)?;
    let id = document.get_object_id("_id").map_err(|_| invalid())?;
    let date = document.get_datetime(retention).map_err(|_| invalid())?;
    Ok((ArchiveKey::object_id(id.bytes()), date.timestamp_millis()))
}

pub fn encode(bytes: &[u8], retention: &str) -> Result<Vec<Option<String>>, Error> {
    let (id, _) = inspect(bytes, retention)?;
    Ok(vec![Some(id.to_string()), Some(hex(bytes))])
}

/// Cross-check the queryable envelope key against the key inside the original BSON.
pub fn decode(row: &[Option<String>], retention: &str) -> Result<Vec<u8>, Error> {
    if row.len() != 2 {
        return Err(invalid());
    }
    let id = key(row[0].as_deref().ok_or_else(invalid)?)?;
    let bytes = unhex(row[1].as_deref().ok_or_else(invalid)?, MAX_DOCUMENT_BYTES)?;
    if inspect(&bytes, retention)?.0 != id {
        return Err(invalid());
    }
    Ok(bytes)
}

pub fn validate_eligibility(
    row: &[Option<String>],
    retention: &str,
    cutoff: &str,
) -> Result<(), Error> {
    let bytes = decode(row, retention)?;
    let cutoff = bson::DateTime::parse_rfc3339_str(cutoff).map_err(|_| invalid())?;
    if inspect(&bytes, retention)?.1 >= cutoff.timestamp_millis() {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::{Binary, Bson, DateTime, Decimal128, doc, oid::ObjectId, spec::BinarySubtype};

    fn fixture() -> Vec<u8> {
        bson::to_vec(&doc! {
            "_id": ObjectId::from_bytes([255; 12]), "created_at": DateTime::from_millis(-1),
            "null": Bson::Null, "int32": 42i32, "int64": i64::MAX,
            "decimal": "123456789.123456789123456789".parse::<Decimal128>().unwrap(),
            "binary": Binary { subtype: BinarySubtype::UserDefined(128), bytes: vec![0, 255] },
            "nested": {"items": [Bson::Null, Bson::Int64(i64::MIN)]},
            "nan": f64::from_bits(0x7ff8000000000042), "negative_zero": -0.0f64,
            "regex": bson::Regex { pattern: "a.*".into(), options: "im".into() },
            "timestamp": bson::Timestamp { time: u32::MAX, increment: u32::MAX },
            "min": Bson::MinKey, "max": Bson::MaxKey,
        })
        .unwrap()
    }

    #[test]
    fn heterogeneous_bson_round_trips_byte_for_byte() {
        let original = fixture();
        let envelope = encode(&original, "created_at").unwrap();
        let restored = decode(&envelope, "created_at").unwrap();
        assert_eq!(original, restored);
        let document = RawDocument::from_bytes(&restored).unwrap();
        assert!(document.get("missing").unwrap().is_none());
        assert!(matches!(
            document.get("null").unwrap(),
            Some(RawBsonRef::Null)
        ));
        assert_eq!(
            document.get_f64("nan").unwrap().to_bits(),
            0x7ff8000000000042
        );
        assert_eq!(inspect(&original, "created_at").unwrap().1, -1);
        assert!(validate_eligibility(&envelope, "created_at", "1970-01-01T00:00:00Z").is_ok());
        assert!(validate_eligibility(&envelope, "created_at", "1969-12-31T23:59:59.999Z").is_err());
    }

    #[test]
    fn rejects_wrong_or_missing_retention_and_mismatched_envelopes() {
        for value in [
            None,
            Some(Bson::Null),
            Some(Bson::Int64(1)),
            Some(Bson::Array(vec![Bson::DateTime(DateTime::now())])),
        ] {
            let mut document = doc! {"_id": ObjectId::new()};
            if let Some(value) = value {
                document.insert("created_at", value);
            }
            assert!(encode(&bson::to_vec(&document).unwrap(), "created_at").is_err());
        }
        let mut envelope = encode(&fixture(), "created_at").unwrap();
        envelope[0] = Some("000000000000000000000000".into());
        assert!(decode(&envelope, "created_at").is_err());
        assert!(key("FFFFFFFFFFFFFFFFFFFFFFFF").is_err());
        assert!(key("0000").is_err());
        assert!(inspect(&fixture(), "nested.date").is_err());
    }

    #[test]
    fn rejects_oversized_malformed_deep_and_duplicate_documents() {
        assert!(inspect(&vec![0; MAX_DOCUMENT_BYTES + 1], "date").is_err());
        let mut malformed = fixture();
        malformed[4] = 0x42;
        assert!(inspect(&malformed, "created_at").is_err());
        let mut nested = doc! {"value": 1};
        for _ in 0..=MAX_DEPTH {
            nested = doc! {"nested": nested};
        }
        let deep = doc! {"_id": ObjectId::new(), "created_at": DateTime::now(), "deep": nested};
        assert!(inspect(&bson::to_vec(&deep).unwrap(), "created_at").is_err());
        let mut duplicate = bson::RawDocumentBuf::new();
        duplicate.append("_id", ObjectId::new());
        duplicate.append("_id", ObjectId::new());
        duplicate.append("created_at", DateTime::now());
        assert!(inspect(duplicate.as_bytes(), "created_at").is_err());
    }
}
