use crate::output::{Output, text};
use clap::{Args as ClapArgs, Subcommand};
use coldctl_core::{paths::StatePaths, state::archive as store};
#[derive(ClapArgs)]
pub struct Args {
    #[arg(long,global=true,value_enum,default_value_t=Output::Table)]
    pub output: Output,
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    List,
    Show {
        id: String,
    },
    /// Request a safe stop after the current batch; published files are preserved.
    Cancel {
        id: String,
    },
    /// Retry a failed or interrupted job in place (same behavior as archive resume).
    Retry {
        id: String,
        /// Confirm the original database and continued stability of eligible rows.
        #[arg(long)]
        confirm_original_source: bool,
        #[command(flatten)]
        execution: crate::execution::ExecutionArgs,
    },
}
pub async fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Cancel { id } => {
            let job = coldctl_core::archive::checkpoint::cancel(paths, &id)?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&job)?);
            } else {
                println!(
                    "Job {}: {:?}; cancellation requested: {}",
                    job.id, job.status, job.cancel_requested
                );
            }
        }
        Command::Retry {
            id,
            confirm_original_source,
            execution,
        } => {
            let controls = execution.apply(store::job_show(paths, &id)?.execution)?;
            let signals = crate::execution::Signals::install()?;
            let progress = crate::progress::Display::start();
            let job = coldctl_core::archive::executor::resume_with_progress(
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
        Command::List => {
            let jobs = store::job_list(paths)?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&jobs)?);
            } else if jobs.is_empty() {
                println!("No archive jobs.");
            } else {
                for j in jobs {
                    println!(
                        "{}  {}  {:?}  {} rows  {} files",
                        j.id,
                        text(&j.policy_name),
                        j.status,
                        j.rows_processed,
                        j.objects_created
                    );
                }
                println!("Running can include interrupted processes; use archive resume <job-id>.");
            }
        }
        Command::Show { id } => {
            let j = store::job_show(paths, &id)?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&j)?);
            } else {
                println!(
                    "Job          {}\nPolicy       {}\nStatus       {:?}\nStarted      {}\nFinished     {}\nRows         {}\nBytes        {}\nParquet files {}\nLast key     {}\nDirectory    {}",
                    text(&j.id),
                    text(&j.policy_name),
                    j.status,
                    text(&j.started_at),
                    j.completed_at
                        .as_deref()
                        .map(text)
                        .unwrap_or_else(|| "pending".into()),
                    j.rows_processed,
                    j.bytes_written,
                    j.objects_created,
                    j.last_key
                        .map(|k| k.to_string())
                        .unwrap_or_else(|| "none".into()),
                    text(&j.storage_root.join(&j.id).to_string_lossy())
                );
                if let Some(error) = j.error {
                    println!("Reason       {}", text(&error));
                }
                if let Some(progress) = &j.progress {
                    println!("Last progress {}", serde_json::to_string(progress)?);
                    println!(
                        "Progress is a saved observation, not a liveness guarantee. Elapsed time is for the last invocation; rates describe its latest committed batch interval."
                    );
                }
                if let Some(failure) = &j.failure {
                    println!(
                        "Failure      {:?} | retryable: {}\nNext step    {}",
                        failure.category,
                        failure.retryable,
                        text(&failure.next_step)
                    );
                }
                println!("Imported     {}", j.imported);
                println!("Execution    {}", serde_json::to_string(&j.execution)?);
                if j.status == store::JobStatus::Running {
                    println!(
                        "Running can indicate an interrupted process; use archive resume <job-id>."
                    );
                }
                println!(
                    "Cancellation requested: {} | Last verified: {}",
                    j.cancel_requested,
                    j.verified_at
                        .as_deref()
                        .map(text)
                        .unwrap_or_else(|| "never".into())
                );
            }
        }
    }
    Ok(())
}
