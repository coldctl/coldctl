//! Metadata-based archival assessment. No retention rules or deletion eligibility are inferred.
use crate::source::{Discovery, Table};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Default, Serialize)]
pub struct TableStatistics {
    /// Physical storage includes TOAST and indexes, not predicted archive size.
    pub total_bytes: Option<i64>,
    pub table_bytes: Option<i64>,
    pub index_bytes: Option<i64>,
    pub last_analyzed_at: Option<String>,
    pub estimated_changes_since_analyze: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct TimeCandidate {
    pub column: String,
    pub data_type: String,
    pub nullable: bool,
    /// Valid, non-partial B-tree/BRIN indexes with this column first.
    pub range_indexes: Vec<String>,
    /// These require checking the index predicate against a future policy.
    pub partial_range_indexes: Vec<String>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Finding {
    MissingPrimaryKey,
    NoTimeColumn,
    UnknownRowEstimate,
    PartitionedParent,
}

#[derive(Debug, Serialize)]
pub struct AnalyzedTable {
    pub table: Table,
    pub statistics: TableStatistics,
    pub time_candidates: Vec<TimeCandidate>,
    pub findings: Vec<Finding>,
}

#[derive(Debug, Serialize)]
pub struct Analysis {
    pub schemas: Vec<String>,
    pub tables: Vec<AnalyzedTable>,
    pub method: &'static str,
    pub timestamp_ranges: &'static str,
}

pub(crate) fn assess(
    discovery: Discovery,
    mut statistics: BTreeMap<(String, String), TableStatistics>,
) -> Analysis {
    let mut tables: Vec<_> = discovery
        .tables
        .into_iter()
        .map(|table| {
            let statistics = statistics
                .remove(&(table.schema.clone(), table.name.clone()))
                .unwrap_or_default();
            let time_candidates: Vec<_> = table
                .columns
                .iter()
                .filter(|c| c.archive_time_candidate)
                .map(|column| {
                    let mut range_indexes = Vec::new();
                    let mut partial_range_indexes = Vec::new();
                    for index in &table.indexes {
                        if index.valid
                            && !index.has_expressions
                            && matches!(index.method.as_str(), "btree" | "brin")
                            && index.columns.first() == Some(&column.name)
                        {
                            if index.partial {
                                partial_range_indexes.push(index.name.clone());
                            } else {
                                range_indexes.push(index.name.clone());
                            }
                        }
                    }
                    TimeCandidate {
                        column: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        range_indexes,
                        partial_range_indexes,
                    }
                })
                .collect();
            let mut findings = Vec::new();
            if table.primary_key.is_empty() {
                findings.push(Finding::MissingPrimaryKey);
            }
            if time_candidates.is_empty() {
                findings.push(Finding::NoTimeColumn);
            }
            if table.estimated_rows.is_none() {
                findings.push(Finding::UnknownRowEstimate);
            }
            if table.partitioned {
                findings.push(Finding::PartitionedParent);
            }
            AnalyzedTable {
                table,
                statistics,
                time_candidates,
                findings,
            }
        })
        .collect();
    tables.sort_by(|a, b| {
        b.statistics
            .total_bytes
            .cmp(&a.statistics.total_bytes)
            .then_with(|| a.table.schema.cmp(&b.table.schema))
            .then_with(|| a.table.name.cmp(&b.table.name))
    });
    Analysis {
        schemas: discovery.schemas,
        tables,
        method: "metadata_only",
        timestamp_ranges: "not_measured",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::{Column, Index};

    fn table(name: &str) -> Table {
        Table {
            schema: "public".into(),
            name: name.into(),
            partitioned: false,
            estimated_rows: None,
            columns: vec![],
            primary_key: vec![],
            indexes: vec![],
        }
    }
    fn index(name: &str, method: &str, columns: &[&str]) -> Index {
        Index {
            name: name.into(),
            method: method.into(),
            unique: false,
            primary: false,
            valid: true,
            columns: columns.iter().map(|s| s.to_string()).collect(),
            included_columns: vec![],
            has_expressions: false,
            partial: false,
        }
    }

    #[test]
    fn sorts_known_sizes_first_and_distinguishes_zero_from_unknown() {
        let mut empty = table("empty");
        empty.estimated_rows = Some(0.0);
        let mut parent = table("parent");
        parent.partitioned = true;
        let discovery = Discovery {
            schemas: vec!["public".into()],
            tables: vec![parent, empty, table("big")],
        };
        let stats = BTreeMap::from([
            (
                ("public".into(), "empty".into()),
                TableStatistics {
                    total_bytes: Some(0),
                    ..Default::default()
                },
            ),
            (
                ("public".into(), "big".into()),
                TableStatistics {
                    total_bytes: Some(8192),
                    ..Default::default()
                },
            ),
        ]);
        let result = assess(discovery, stats);
        assert_eq!(
            result
                .tables
                .iter()
                .map(|t| t.table.name.as_str())
                .collect::<Vec<_>>(),
            ["big", "empty", "parent"]
        );
        assert!(
            !result.tables[1]
                .findings
                .contains(&Finding::UnknownRowEstimate)
        );
        assert!(
            result.tables[2]
                .findings
                .contains(&Finding::PartitionedParent)
        );
        assert!(
            result.tables[2]
                .findings
                .contains(&Finding::MissingPrimaryKey)
        );
        assert!(result.tables[2].findings.contains(&Finding::NoTimeColumn));
    }

    #[test]
    fn range_indexes_require_valid_leading_keys_and_partial_indexes_are_separate() {
        let mut t = table("orders");
        t.primary_key = vec!["id".into()];
        t.columns.push(Column {
            name: "created_at".into(),
            data_type: "timestamptz".into(),
            nullable: true,
            archive_time_candidate: true,
        });
        let mut partial = index("partial", "btree", &["created_at"]);
        partial.partial = true;
        let mut invalid = index("invalid", "btree", &["created_at"]);
        invalid.valid = false;
        let mut expression = index("expr", "btree", &["created_at"]);
        expression.has_expressions = true;
        let mut included = index("include", "btree", &["id"]);
        included.included_columns = vec!["created_at".into()];
        t.indexes = vec![
            index("time", "btree", &["created_at", "id"]),
            index("brin", "brin", &["created_at"]),
            index("nonleading", "btree", &["id", "created_at"]),
            index("hash", "hash", &["created_at"]),
            partial,
            invalid,
            expression,
            included,
        ];
        let result = assess(
            Discovery {
                schemas: vec![],
                tables: vec![t],
            },
            BTreeMap::new(),
        );
        let candidate = &result.tables[0].time_candidates[0];
        assert_eq!(candidate.range_indexes, ["time", "brin"]);
        assert_eq!(candidate.partial_range_indexes, ["partial"]);
        assert!(candidate.nullable);
        assert!(
            !result.tables[0]
                .findings
                .contains(&Finding::MissingPrimaryKey)
        );
    }

    #[test]
    fn empty_database_has_an_explicit_metadata_only_report() {
        let result = assess(
            Discovery {
                schemas: vec!["public".into()],
                tables: vec![],
            },
            BTreeMap::new(),
        );
        assert!(result.tables.is_empty());
        assert_eq!(result.method, "metadata_only");
        assert_eq!(result.timestamp_ranges, "not_measured");
    }
}
