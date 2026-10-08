//! Native MySQL 8.4/InnoDB implementation. No driver is linked into the base agent.
use coldctl_core::{
    analysis::{Analysis, TableStatistics},
    error::Error,
    source::{
        Column, ConnectionInfo, Discovery, Index, Table,
        config::{ResolvedConnection, TlsMode},
    },
};
use mysql_async::{Conn, OptsBuilder, Row, SslOpts, Value, prelude::Queryable};
use std::collections::BTreeMap;
pub fn bad() -> Error {
    Error::Archive(
        "MySQL validation failed; unsupported schema, identity or values; details omitted",
    )
}
pub fn err(e: mysql_async::Error) -> Error {
    match e {
        mysql_async::Error::Server(e) => match e.code {
            1045 => Error::Archive("MySQL authentication failed"),
            1044 | 1142 | 1227 => Error::Archive("MySQL permission denied"),
            1205 | 3024 => Error::Archive("MySQL query timeout"),
            _ => bad(),
        },
        _ => Error::Archive("MySQL connection or TLS failed"),
    }
}
pub fn q(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}
pub fn table(schema: &str, name: &str) -> String {
    format!("{}.{}", q(schema), q(name))
}
pub fn name(s: &str) -> Result<(), Error> {
    if s.is_empty() || s.len() > 64 || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        Err(bad())
    } else {
        Ok(())
    }
}
pub async fn connect(c: &ResolvedConnection, readonly: bool) -> Result<Conn, Error> {
    let opts = OptsBuilder::default()
        .ip_or_hostname(c.host.clone())
        .tcp_port(c.port)
        .user(Some(c.user.clone()))
        .pass(c.password.clone())
        .db_name(Some(c.database.clone()))
        .prefer_socket(false)
        .max_allowed_packet(Some(40 * 1024 * 1024))
        .ssl_opts(if c.tls == TlsMode::Require {
            Some(if let Some(pem) = &c.ca_pem {
                SslOpts::default().with_root_certs(vec![pem.as_bytes().to_vec().into()])
            } else {
                SslOpts::default()
            })
        } else {
            None
        });
    let mut conn = Conn::new(opts).await.map_err(err)?;
    let version: String = conn
        .query_first("SELECT VERSION()")
        .await
        .map_err(err)?
        .ok_or_else(bad)?;
    if !version.starts_with("8.4.") || version.to_ascii_lowercase().contains("mariadb") {
        return Err(Error::Archive(
            "MySQL connector requires qualified MySQL 8.4; MariaDB is unsupported",
        ));
    }
    conn.query_drop("SET SESSION time_zone='+00:00'")
        .await
        .map_err(err)?;
    conn.query_drop("SET NAMES utf8mb4 COLLATE utf8mb4_bin")
        .await
        .map_err(err)?;
    conn.query_drop("SET SESSION sql_mode='STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'").await.map_err(err)?;
    conn.query_drop("SET SESSION max_execution_time=30000")
        .await
        .map_err(err)?;
    conn.query_drop("SET SESSION lock_wait_timeout=5")
        .await
        .map_err(err)?;
    conn.query_drop("SET SESSION innodb_lock_wait_timeout=5")
        .await
        .map_err(err)?;
    if readonly {
        conn.query_drop("SET SESSION TRANSACTION READ ONLY")
            .await
            .map_err(err)?;
    }
    Ok(conn)
}
pub struct MysqlSource {
    connection: ResolvedConnection,
}
impl MysqlSource {
    pub fn new(connection: ResolvedConnection) -> Self {
        Self { connection }
    }
    pub async fn test_connection(&self) -> Result<ConnectionInfo, Error> {
        let mut conn = connect(&self.connection, true).await?;
        let (database, user, server_version): (String, String, String) = conn
            .query_first("SELECT DATABASE(),CURRENT_USER(),VERSION()")
            .await
            .map_err(err)?
            .ok_or_else(bad)?;
        Ok(ConnectionInfo {
            database,
            user,
            server_version,
        })
    }
    pub async fn discover(&self) -> Result<Discovery, Error> {
        let mut conn = connect(&self.connection, true).await?;
        discover(&mut conn, &self.connection.database).await
    }
    pub async fn analyze(&self) -> Result<Analysis, Error> {
        let mut conn = connect(&self.connection, true).await?;
        let discovery = discover(&mut conn, &self.connection.database).await?;
        let rows:Vec<(String,Option<i64>,Option<i64>)>=conn.exec("SELECT TABLE_NAME,DATA_LENGTH,INDEX_LENGTH FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE'",(&self.connection.database,)).await.map_err(err)?;
        let stats = rows
            .into_iter()
            .map(|(table, data, index)| {
                (
                    (self.connection.database.clone(), table),
                    TableStatistics {
                        total_bytes: data.zip(index).and_then(|(a, b)| a.checked_add(b)),
                        table_bytes: data,
                        index_bytes: index,
                        ..Default::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut result = coldctl_core::analysis::assess(discovery, stats);
        result.method = "MySQL InnoDB information_schema estimates; no source row scans".into();
        Ok(result)
    }
}
pub async fn discover(conn: &mut Conn, database: &str) -> Result<Discovery, Error> {
    let entries:Vec<(String,Option<u64>,String)>=conn.exec("SELECT TABLE_NAME,TABLE_ROWS,COALESCE(ENGINE,'') FROM information_schema.tables WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME LIMIT 1001",(database,)).await.map_err(err)?;
    if entries.len() > 1000 {
        return Err(Error::Archive("discovery exceeds 1000 table limit"));
    }
    let mut tables = Vec::new();
    for (name, estimated, engine) in entries {
        let columns:Vec<(String,String,String)>=conn.exec("SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE FROM information_schema.columns WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION LIMIT 1601",(database,&name)).await.map_err(err)?;
        if columns.len() > 1600 {
            return Err(bad());
        }
        let idx:Vec<(String,u8,String,u64,Option<String>)>=conn.exec("SELECT INDEX_NAME,NON_UNIQUE,INDEX_TYPE,SEQ_IN_INDEX,COLUMN_NAME FROM information_schema.statistics WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY INDEX_NAME,SEQ_IN_INDEX",(database,&name)).await.map_err(err)?;
        let mut indexes: BTreeMap<String, Index> = BTreeMap::new();
        for (index, nonunique, method, _, col) in idx {
            let entry = indexes.entry(index.clone()).or_insert_with(|| Index {
                name: index.clone(),
                method: method.to_ascii_lowercase(),
                unique: nonunique == 0,
                primary: index == "PRIMARY",
                valid: true,
                columns: vec![],
                included_columns: vec![],
                has_expressions: false,
                partial: false,
            });
            if let Some(col) = col {
                entry.columns.push(col)
            } else {
                entry.has_expressions = true;
            }
        }
        let primary_key = indexes
            .get("PRIMARY")
            .map(|i| i.columns.clone())
            .unwrap_or_default();
        let partitioned:Option<u8>=conn.exec_first("SELECT 1 FROM information_schema.partitions WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND PARTITION_NAME IS NOT NULL LIMIT 1",(database,&name)).await.map_err(err)?;
        tables.push(Table {
            schema: database.into(),
            name,
            partitioned: partitioned.is_some(),
            estimated_rows: if engine == "InnoDB" {
                estimated.map(|n| n as f64)
            } else {
                None
            },
            columns: columns
                .into_iter()
                .map(|(name, data_type, null)| Column {
                    archive_time_candidate: matches!(
                        data_type.split('(').next(),
                        Some("date" | "datetime" | "timestamp")
                    ),
                    name,
                    data_type,
                    nullable: null == "YES",
                })
                .collect(),
            primary_key,
            indexes: indexes.into_values().collect(),
        });
    }
    Ok(Discovery {
        schemas: vec![database.into()],
        tables,
    })
}
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02X}")).collect()
}
pub fn unhex(value: &str) -> Result<Vec<u8>, Error> {
    if value.len() % 2 != 0 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(bad());
    }
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).map_err(|_| bad()))
        .collect()
}
pub fn encode(
    row: Row,
    columns: &[coldctl_core::archive::batch::ArchiveColumn],
) -> Result<Vec<Option<String>>, Error> {
    row.unwrap()
        .into_iter()
        .zip(columns)
        .map(|(value, col)| {
            let value = match value {
                Value::NULL => return if col.nullable { Ok(None) } else { Err(bad()) },
                Value::Bytes(bytes) => {
                    if coldctl_core::source::mysql_types::hex_value(&col.postgres_type) {
                        hex(&bytes)
                    } else {
                        String::from_utf8(bytes).map_err(|_| bad())?
                    }
                }
                Value::Int(n) => n.to_string(),
                Value::UInt(n) => n.to_string(),
                Value::Float(n) => {
                    if !n.is_finite() {
                        return Err(bad());
                    }
                    n.to_string()
                }
                Value::Double(n) => {
                    if !n.is_finite() {
                        return Err(bad());
                    }
                    n.to_string()
                }
                Value::Date(y, m, d, h, min, s, micro) => {
                    if y == 0 || m == 0 || d == 0 {
                        return Err(bad());
                    }
                    if col.postgres_type == "mysql:date|" {
                        format!("{y:04}-{m:02}-{d:02}")
                    } else {
                        format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}.{micro:06}")
                    }
                }
                Value::Time(negative, days, h, m, s, micro) => format!(
                    "{}{:02}:{m:02}:{s:02}.{micro:06}",
                    if negative { "-" } else { "" },
                    days * 24 + u32::from(h)
                ),
            };
            Ok(Some(value))
        })
        .collect()
}
