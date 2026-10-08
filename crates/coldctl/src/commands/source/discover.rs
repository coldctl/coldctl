use super::{Output, text};
use coldctl_core::{
    paths::StatePaths,
    source::{ConnectorSource, DataSource},
    state::sources,
};

pub async fn run(
    paths: &StatePaths,
    name: &str,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let source = sources::show(paths, name)?;
    let discovery = ConnectorSource::in_state(source.connection, paths)
        .discover()
        .await?;
    match output {
        Output::Json => super::json(&serde_json::to_value(discovery)?)?,
        Output::Table => {
            println!(
                "Source: {name}\nSchemas: {}",
                discovery
                    .schemas
                    .iter()
                    .map(|s| text(s))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            println!(
                "Row counts are catalog estimates; unknown means statistics are unavailable or the table is partitioned."
            );
            for table in discovery.tables {
                println!(
                    "\n{}.{}  rows: {}{}",
                    text(&table.schema),
                    text(&table.name),
                    table
                        .estimated_rows
                        .map(|v| format!("~{v:.0}"))
                        .unwrap_or_else(|| "unknown".into()),
                    if table.partitioned {
                        " (partitioned)"
                    } else {
                        ""
                    }
                );
                println!(
                    "  Primary key: {}",
                    if table.primary_key.is_empty() {
                        "none".into()
                    } else {
                        table
                            .primary_key
                            .iter()
                            .map(|s| text(s))
                            .collect::<Vec<_>>()
                            .join(", ")
                    }
                );
                for column in table.columns {
                    println!(
                        "  {:<24} {:<24} {}{}",
                        text(&column.name),
                        text(&column.data_type),
                        if column.nullable {
                            "nullable"
                        } else {
                            "not null"
                        },
                        if column.archive_time_candidate {
                            "  [archive time candidate]"
                        } else {
                            ""
                        }
                    );
                }
                for index in table.indexes {
                    println!(
                        "  Index {} [{}] ({}) unique={} primary={} valid={} partial={} expressions={} include=({})",
                        text(&index.name),
                        text(&index.method),
                        index
                            .columns
                            .iter()
                            .map(|s| text(s))
                            .collect::<Vec<_>>()
                            .join(", "),
                        index.unique,
                        index.primary,
                        index.valid,
                        index.partial,
                        index.has_expressions,
                        index
                            .included_columns
                            .iter()
                            .map(|s| text(s))
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
        }
    }
    Ok(())
}
