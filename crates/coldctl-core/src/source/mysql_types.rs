//! MySQL text/hex v1. Cursor tokens use an order-preserving, reversible u64 bias
//! in the existing SQLite i64 journal; archived key values retain their full decimal text.
use crate::error::Error;
pub fn bad() -> Error {
    Error::Archive("unsupported or invalid MySQL type/value; row values omitted")
}
pub fn parts(kind: &str) -> Result<(&str, &str), Error> {
    let (ty, collation) = kind
        .strip_prefix("mysql:")
        .and_then(|s| s.split_once('|'))
        .ok_or_else(bad)?;
    if ty.len() > 100
        || !ty
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b" (),".contains(&b))
        || !collation
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(bad());
    }
    let base = ty.split(['(', ' ']).next().unwrap_or("");
    if !matches!(
        base,
        "tinyint"
            | "smallint"
            | "mediumint"
            | "int"
            | "bigint"
            | "decimal"
            | "float"
            | "double"
            | "date"
            | "datetime"
            | "timestamp"
            | "time"
            | "varchar"
            | "tinytext"
            | "text"
            | "mediumtext"
            | "longtext"
            | "binary"
            | "varbinary"
            | "tinyblob"
            | "blob"
            | "mediumblob"
            | "longblob"
    ) {
        return Err(bad());
    }
    // Only known grammar reaches native DDL. Reject arbitrary token sequences.
    let bare = ty.strip_suffix(" unsigned").unwrap_or(ty);
    let suffix = &bare[base.len()..];
    if !(suffix.is_empty()
        || (suffix.starts_with('(')
            && suffix.ends_with(')')
            && suffix[1..suffix.len() - 1]
                .split(',')
                .all(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))))
    {
        return Err(bad());
    }
    if ty.ends_with(" unsigned")
        && !matches!(
            base,
            "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
        )
    {
        return Err(bad());
    }
    Ok((ty, collation))
}
pub fn integer(kind: &str) -> bool {
    parts(kind).is_ok_and(|(t, _)| {
        matches!(
            t.split(['(', ' ']).next().unwrap_or(""),
            "tinyint" | "smallint" | "mediumint" | "int" | "bigint"
        )
    })
}
pub fn hex_value(kind: &str) -> bool {
    parts(kind).is_ok_and(|(t, _)| {
        matches!(
            t.split('(').next().unwrap_or(""),
            "varchar"
                | "tinytext"
                | "text"
                | "mediumtext"
                | "longtext"
                | "binary"
                | "varbinary"
                | "tinyblob"
                | "blob"
                | "mediumblob"
                | "longblob"
        )
    })
}
pub fn cursor(kind: &str, value: &str) -> Result<i64, Error> {
    if kind.starts_with("mysql:") && !integer(kind) {
        return Err(bad());
    }
    if kind.starts_with("mysql:") && parts(kind)?.0.ends_with(" unsigned") {
        let n = value.parse::<u64>().map_err(|_| bad())?;
        if n.to_string() != value {
            return Err(bad());
        }
        Ok((n ^ (1u64 << 63)) as i64)
    } else {
        value.parse().map_err(|_| bad())
    }
}
pub fn key_value(kind: &str, token: i64) -> String {
    if parts(kind).is_ok_and(|(t, _)| t.ends_with(" unsigned")) {
        ((token as u64) ^ (1u64 << 63)).to_string()
    } else {
        token.to_string()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_unsigned_domain_orders_and_round_trips() {
        let values = [0, 1, i64::MAX as u64, 1u64 << 63, u64::MAX];
        let mut last = None;
        for value in values {
            let token = cursor("mysql:bigint unsigned|", &value.to_string()).unwrap();
            assert!(last.is_none_or(|v| v < token));
            assert_eq!(
                key_value("mysql:bigint unsigned|", token),
                value.to_string()
            );
            last = Some(token);
        }
        assert!(cursor("mysql:bigint unsigned|", "18446744073709551616").is_err());
        assert!(parts("mysql:int); DROP TABLE t|x").is_err());
    }
}
