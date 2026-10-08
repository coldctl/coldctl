use crate::output::{Output, text};
use clap::{Args as ClapArgs, Subcommand};
use coldctl_core::{
    archive::{
        executor,
        planner::{self, ArchivePlan},
        recovery, verifier,
    },
    paths::StatePaths,
};

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long,global=true,value_enum,default_value_t=Output::Table)]
    pub output: Output,
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    /// Validate and estimate without reading table rows or writing archive files.
    Plan { policy: String },
    /// Export eligible rows to a NEW local Parquet job directory. Never deletes source rows.
    Run {
        policy: String,
        /// Assert stable rows AND eligibility (including no backdated inserts).
        #[arg(long, value_enum)]
        source_stability: Stability,
        /// Acknowledge that a new run can duplicate previous exports.
        #[arg(long)]
        allow_repeat: bool,
        #[command(flatten)]
        execution: crate::execution::ExecutionArgs,
    },
    /// Continue an interrupted, failed, or cancelled job using its saved checkpoint.
    Resume {
        id: String,
        /// Confirm the original database and continued stability of eligible rows.
        #[arg(long)]
        confirm_original_source: bool,
        #[command(flatten)]
        execution: crate::execution::ExecutionArgs,
    },
    /// Verify a completed archive offline, without database credentials.
    Verify { id: String },
    /// Inspect a format-v2 manifest without the original state or database.
    Inspect { directory: std::path::PathBuf },
    /// Check manifest/file consistency offline; this does not prove authenticity.
    VerifyDirectory { directory: std::path::PathBuf },
    /// Verify and import a completed archive; never overwrites an existing job ID.
    Import {
        directory: std::path::PathBuf,
        /// Acknowledge that supplied files and manifest do not prove authenticity.
        #[arg(long, required = true)]
        accept_untrusted_manifest: bool,
    },
    /// List unreferenced staging candidates only; never deletes files.
    StagingList { id: String },
    /// Restore into a new dedicated target table in the same database engine; rerun to resume.
    Restore {
        id: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        schema: String,
        #[arg(long)]
        table: String,
        #[arg(long, required = true)]
        confirm_separate_target: bool,
        #[arg(long)]
        max_batches: Option<u64>,
    },
    /// Compare every committed restored value with the verified archive.
    ValidateRestore {
        id: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        schema: String,
        #[arg(long)]
        table: String,
    },
}
pub async fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Restore {
            id,
            target,
            schema,
            table,
            confirm_separate_target,
            max_batches,
        } => {
            let signals = crate::execution::Signals::install()?;
            eprintln!(
                "Verifying archive before restore. Restore checkpoints are committed in the target database."
            );
            let report = coldctl_core::source::restore::run(
                paths,
                &id,
                coldctl_core::source::restore::Options {
                    target,
                    schema,
                    table,
                    confirm_separate_target,
                    max_batches,
                    shutdown: signals.shutdown.clone(),
                    validate_only: false,
                    progress: Some(restore_progress()),
                },
            )
            .await?;
            print_restore(&report, args.output)?;
        }
        Command::ValidateRestore {
            id,
            target,
            schema,
            table,
        } => {
            let signals = crate::execution::Signals::install()?;
            let report = coldctl_core::source::restore::run(
                paths,
                &id,
                coldctl_core::source::restore::Options {
                    target,
                    schema,
                    table,
                    confirm_separate_target: false,
                    max_batches: None,
                    shutdown: signals.shutdown.clone(),
                    validate_only: true,
                    progress: Some(restore_progress()),
                },
            )
            .await?;
            print_restore(&report, args.output)?;
        }
        Command::Inspect { directory } => {
            let report = recovery::inspect(&directory)?;
            print_recovery(&report, args.output)?;
        }
        Command::VerifyDirectory { directory } => {
            let progress = crate::progress::Display::start();
            let report = recovery::verify_directory(&directory, progress.observer())?;
            drop(progress);
            print_recovery(&report, args.output)?;
        }
        Command::Import {
            directory,
            accept_untrusted_manifest: _,
        } => {
            let progress = crate::progress::Display::start();
            let job = recovery::import(paths, &directory, progress.observer())?;
            drop(progress);
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&job)?);
            } else {
                println!(
                    "Imported completed job {}: {} rows, {} files. Integrity checked against the supplied manifest; authenticity is not established.",
                    job.id, job.rows_processed, job.objects_created
                );
            }
        }
        Command::StagingList { id } => {
            let report = recovery::staging_candidates(paths, &id)?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "Dry run: {} unreferenced staging candidates. Nothing deleted. Stop all writers before any manual cleanup; this lock covers only this state directory.",
                    report.candidates.len()
                );
                for path in report.candidates {
                    println!("{}", text(&path.to_string_lossy()));
                }
            }
        }
        Command::Plan { policy } => print_plan(&planner::plan(paths, &policy).await?, args.output)?,
        Command::Verify { id } => {
            let progress = crate::progress::Display::start();
            let report = verifier::verify_with_progress(paths, &id, progress.observer())?;
            drop(progress);
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                crate::output::success(&format!(
                    "Verified {}: {} rows in {} files.",
                    report.job_id, report.rows, report.objects
                ));
                if report.integrity_basis == "supplied_manifest_not_authenticated" {
                    println!(
                        "Integrity checked against the imported manifest; authenticity is not established."
                    );
                }
            }
        }
        Command::Resume {
            id,
            confirm_original_source,
            execution,
        } => {
            let controls =
                execution.apply(coldctl_core::state::archive::job_show(paths, &id)?.execution)?;
            let signals = crate::execution::Signals::install()?;
            let progress = crate::progress::Display::start();
            let job = executor::resume_with_progress(
                paths,
                &id,
                confirm_original_source,
                controls,
                signals.shutdown.clone(),
                progress.observer(),
            )
            .await?;
            drop(progress);
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&job)?);
            } else {
                println!(
                    "Job {}: {:?}, {} rows, {} files.",
                    job.id, job.status, job.rows_processed, job.objects_created
                );
            }
        }
        Command::Run {
            policy,
            source_stability,
            allow_repeat,
            execution,
        } => {
            let controls = execution.apply(Default::default())?;
            let signals = crate::execution::Signals::install()?;
            let progress = crate::progress::Display::start();
            let job = executor::run_with_preflight_report(
                paths,
                &policy,
                executor::RunOptions {
                    source_stability: source_stability.into(),
                    allow_repeat,
                    minimum_free_bytes: controls.minimum_free_bytes,
                    execution: controls,
                    shutdown: signals.shutdown.clone(),
                    progress: progress.observer(),
                },
                |plan| {
                    for warning in &plan.warnings { eprintln!("! {}",text(warning)); }
                    if let Some(safety)=&plan.safety {
                        eprintln!("Preflight: {:?}; {} bytes available, {} bytes minimum. Source stability is an operator assertion.",safety.source_stability,safety.available_bytes_at_preflight,safety.minimum_free_bytes);
                    }
                },
            )
            .await?;
            drop(progress);
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&job)?);
            } else {
                crate::output::success(&format!("Archive job {}: {:?}.", job.id, job.status));
                println!(
                    "Rows: {} | Parquet files: {} | Bytes: {}",
                    job.rows_processed, job.objects_created, job.bytes_written
                );
                println!(
                    "Directory: {}",
                    text(&job.plan.destination_path.join(&job.id).to_string_lossy())
                );
                println!("Source rows were not deleted.");
            }
        }
    }
    Ok(())
}
pub(super) fn print_plan(plan: &ArchivePlan, output: Output) -> Result<(), serde_json::Error> {
    if matches!(output, Output::Json) {
        println!("{}", serde_json::to_string_pretty(plan)?);
        return Ok(());
    }
    println!(
        "Coldctl archive plan\n\nPolicy       {}\nSource       {}\nTable        {}.{}\nTime column  {}\nCutoff (UTC) {}\nPrimary key  {}\nDestination  {}\nBatch rows   {}\nEstimated rows {}\nDelete       NO",
        text(&plan.policy.name),
        text(&plan.policy.source),
        text(&plan.policy.config.schema),
        text(&plan.policy.config.table),
        text(&plan.policy.config.time_column),
        text(&plan.cutoff_utc),
        text(&plan.primary_key),
        text(&plan.destination_path.to_string_lossy()),
        plan.policy.config.batch_size,
        plan.estimated_rows
            .map(|n| format!("~{n} (planner estimate)"))
            .unwrap_or_else(|| "unknown".into())
    );
    if let (Some(column), Some(value)) = (
        &plan.policy.config.equals_column,
        &plan.policy.config.equals_value,
    ) {
        println!("Equality     {} = {}", text(column), text(value));
    }
    for warning in &plan.warnings {
        println!("! {}", text(warning));
    }
    println!("No data or archive files have been modified. Storage access is checked during run.");
    Ok(())
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum Stability {
    ImmutableRows,
    QuiescentCopy,
}
impl From<Stability> for coldctl_core::archive::planner::SourceStability {
    fn from(value: Stability) -> Self {
        match value {
            Stability::ImmutableRows => Self::ImmutableRows,
            Stability::QuiescentCopy => Self::QuiescentCopy,
        }
    }
}

fn print_recovery(report: &recovery::Inspection, output: Output) -> Result<(), serde_json::Error> {
    if matches!(output, Output::Json) {
        println!("{}", serde_json::to_string_pretty(report)?);
    } else {
        println!(
            "Archive {} | format {} | {} rows | {} files | {} bytes | verified: {}",
            report.job_id,
            report.format_version,
            report.rows,
            report.objects,
            report.bytes,
            report.verified
        );
        println!("Directory: {}", text(&report.directory.to_string_lossy()));
        println!(
            "Supplied manifest: consistency checks cannot prove authenticity. Inspect alone does not verify batch files."
        );
    }
    Ok(())
}

fn print_restore(
    report: &coldctl_core::source::restore::Report,
    output: Output,
) -> Result<(), serde_json::Error> {
    if matches!(output, Output::Json) {
        println!("{}", serde_json::to_string_pretty(report)?);
    } else {
        println!(
            "Restore {}: {} rows, {} batches; values validated: {}. Integrity basis: {}",
            report.status,
            report.rows,
            report.batches,
            report.values_validated,
            report.integrity_basis
        );
    }
    Ok(())
}

fn restore_progress() -> coldctl_core::source::restore::Observer {
    let started = std::time::Instant::now();
    std::sync::Arc::new(move |stage, rows, batches| {
        eprintln!(
            "Restore {stage}: {rows} rows, {batches} batches | {:.1}s elapsed",
            started.elapsed().as_secs_f64()
        );
    })
}
