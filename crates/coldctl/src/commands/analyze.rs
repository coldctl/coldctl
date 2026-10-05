use super::source::Output;
use crate::output::text;
use clap::Args as ClapArgs;
use coldctl_core::{
    analysis::{Analysis, Finding},
    paths::StatePaths,
    source::{DataSource, postgres::PostgresSource},
    state::sources,
};

#[derive(ClapArgs)]
pub struct Args {
    /// Configured source name.
    pub source: String,
    #[arg(long, value_enum, default_value_t = Output::Table)]
    pub output: Output,
}

pub async fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    let source = sources::show(paths, &args.source)?;
    let analysis = PostgresSource::new(source.connection).analyze().await?;
    match args.output {
        Output::Json => println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({ "source": source.name, "analysis": analysis })
            )?
        ),
        Output::Table => print_report(&source.name, &analysis),
    }
    Ok(())
}

fn bytes(value: Option<i64>) -> String {
    match value {
        None => "unknown".into(),
        Some(value) if value >= 1024 * 1024 * 1024 => {
            format!("{:.1} GiB", value as f64 / (1024.0 * 1024.0 * 1024.0))
        }
        Some(value) if value >= 1024 * 1024 => {
            format!("{:.1} MiB", value as f64 / (1024.0 * 1024.0))
        }
        Some(value) if value >= 1024 => format!("{:.1} KiB", value as f64 / 1024.0),
        Some(value) => format!("{value} B"),
    }
}

fn names(values: &[String]) -> String {
    if values.is_empty() {
        "none".into()
    } else {
        values
            .iter()
            .map(|s| text(s))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

fn print_report(source: &str, analysis: &Analysis) {
    println!("Coldctl analysis: {}", text(source));
    println!("Read-only metadata assessment; tables sorted by total storage, largest first.");
    println!(
        "Rows are estimates and may be stale. Storage includes TOAST/indexes, not predicted archive size."
    );
    println!(
        "Timestamp ranges and archive-eligible rows were not measured. No retention rule is inferred.\n"
    );
    if analysis.tables.is_empty() {
        println!(
            "No accessible tables found. Check the selected database and schema USAGE/table SELECT permissions."
        );
        return;
    }
    for entry in &analysis.tables {
        let table = &entry.table;
        let stats = &entry.statistics;
        println!("{}.{}", text(&table.schema), text(&table.name));
        println!(
            "  Rows: {} | Total: {} | Table/TOAST: {} | Indexes: {}",
            table
                .estimated_rows
                .map(|v| format!("~{v:.0}"))
                .unwrap_or_else(|| "unknown".into()),
            bytes(stats.total_bytes),
            bytes(stats.table_bytes),
            bytes(stats.index_bytes)
        );
        println!("  Primary key: {}", names(&table.primary_key));
        println!(
            "  Last statistics analysis: {} | Estimated changes since: {}",
            stats
                .last_analyzed_at
                .as_deref()
                .map(text)
                .unwrap_or_else(|| "unknown".into()),
            stats
                .estimated_changes_since_analyze
                .map(|n| n.to_string())
                .unwrap_or_else(|| "unknown".into())
        );
        for candidate in &entry.time_candidates {
            println!(
                "  Time candidate: {} ({}){}",
                text(&candidate.column),
                text(&candidate.data_type),
                if candidate.nullable {
                    " — nullable; a policy must account for NULLs"
                } else {
                    ""
                }
            );
            println!(
                "    Leading B-tree/BRIN indexes: {}",
                names(&candidate.range_indexes)
            );
            if !candidate.partial_range_indexes.is_empty() {
                println!(
                    "    Partial indexes (predicate must match policy): {}",
                    names(&candidate.partial_range_indexes)
                );
            }
            if candidate.range_indexes.is_empty() {
                println!("    ! No unconditional, valid leading time-range index identified.");
            }
        }
        for finding in &entry.findings {
            println!(
                "  ! {}",
                match finding {
                    Finding::MissingPrimaryKey =>
                        "No primary key; a stable archive/resume key needs review.",
                    Finding::NoTimeColumn => "No supported date/timestamp archive column found.",
                    Finding::UnknownRowEstimate =>
                        "Row estimate unavailable; this does not mean the table is empty.",
                    Finding::PartitionedParent =>
                        "Partitioned parent: sizes/row counts are not aggregated; inspect individual partitions.",
                }
            );
        }
        println!();
    }
    println!(
        "Choose a retention column and business rule before planning an archive. No data was modified."
    );
}
