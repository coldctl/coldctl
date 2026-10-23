use clap::{Args, Subcommand};
use coldctl_core::{
    archive::controls::ExecutionControls,
    paths::StatePaths,
    scheduler::Worker,
    state::{archive, scheduler as store},
};

#[derive(Args)]
pub struct SchedulerArgs {
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    /// Recurring coordinated PostgreSQL/MySQL parent-child archival and verified deletion.
    Group {
        #[command(subcommand)]
        command: GroupCommand,
    },
    /// Continuously archive and delete eligible records to S3 or Azure Blob at a fixed delay.
    Recurring {
        #[command(subcommand)]
        command: RecurringCommand,
    },
    /// Queue one archive; automatically resume checkpoints until it completes.
    Enqueue {
        policy: String,
        /// Adopt this incomplete job instead of creating a new archive.
        #[arg(long)]
        job: Option<String>,
        #[arg(long, value_enum)]
        source_stability: super::archive::Stability,
        /// Authorize automatic resumes against the original, continuously stable source.
        #[arg(long, required = true)]
        confirm_original_source: bool,
        #[arg(long)]
        allow_repeat: bool,
        /// Delete unchanged source records after archive verification (irreversible).
        #[arg(long)]
        delete_after_archive: bool,
        /// Delay initial execution by this many seconds (maximum one year).
        #[arg(long, default_value_t = 0)]
        delay_seconds: u64,
        /// Consecutive transient retries, with exponential backoff (maximum 100).
        #[arg(long, default_value_t = 5)]
        max_retries: u32,
        #[command(flatten)]
        execution: crate::execution::ExecutionArgs,
    },
    /// Run the foreground worker. Use a service manager for unattended operation.
    Run {
        /// Execute at most one due slice, then exit.
        #[arg(long)]
        once: bool,
    },
    /// List persistent queue entries as JSON.
    List,
    /// Inspect one queue entry as JSON.
    Show { id: String },
    /// Cancel a queue entry and request its active job to stop at a batch boundary.
    Cancel { id: String },
    /// Requeue a failed entry after fixing its cause, keeping the same archive job.
    Retry {
        id: String,
        #[arg(long, required = true)]
        confirm_original_source: bool,
    },
}

#[derive(Subcommand)]
pub enum RecurringCommand {
    /// Authorize repeated archive/verify/delete cycles until stopped.
    Create {
        /// Existing retention policy using PostgreSQL/MySQL/MongoDB and an S3 or Azure Blob destination.
        policy: String,
        /// Delay after completed deletion before the next cycle (60..31536000 seconds).
        #[arg(long)]
        every_seconds: u64,
        /// Assert source stability during every cycle, including downtime and retries.
        #[arg(long, value_enum)]
        source_stability: super::archive::Stability,
        /// Authorize automatic resumes against the original source for every cycle.
        #[arg(long, required = true)]
        confirm_original_source: bool,
        /// Explicitly authorize repeated verified source deletion (irreversible).
        #[arg(long, required = true)]
        delete_after_archive: bool,
        /// Consecutive transient retries before holding the schedule for recovery.
        #[arg(long, default_value_t = 5)]
        max_retries: u32,
        #[command(flatten)]
        execution: crate::execution::ExecutionArgs,
    },
    List,
    Show {
        /// Recurring schedule ID returned by create or list.
        id: String,
    },
    /// Permanently stop new cycles and cancel the active cycle at a safe boundary.
    Stop {
        /// Recurring schedule ID returned by create or list.
        id: String,
    },
    /// Retry the same failed cycle; never skip an incomplete deletion.
    Retry {
        /// Failed recurring schedule ID.
        id: String,
        /// Confirm continued source identity and stability before retrying.
        #[arg(long, required = true)]
        confirm_original_source: bool,
    },
}

#[derive(Subcommand)]
pub enum GroupCommand {
    Create {
        #[arg(long, num_args=2..=32)]
        policies: Vec<String>,
        #[arg(long)]
        every_seconds: u64,
        #[arg(long, required = true)]
        confirm_quiescent_source: bool,
        #[arg(long, required = true)]
        confirm_original_source: bool,
        #[arg(long, required = true)]
        delete_after_archive: bool,
        #[arg(long, default_value_t = 5)]
        max_retries: u32,
        #[command(flatten)]
        execution: crate::execution::ExecutionArgs,
    },
    List,
    Show {
        id: String,
    },
    Stop {
        id: String,
    },
    Retry {
        id: String,
        #[arg(long, required = true)]
        confirm_original_source: bool,
    },
}

pub async fn run(
    args: SchedulerArgs,
    paths: &StatePaths,
) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Group { command } => {
            use coldctl_core::state::recurring_groups as groups;
            let value = match command {
                GroupCommand::Create {
                    policies,
                    every_seconds,
                    confirm_quiescent_source,
                    confirm_original_source,
                    delete_after_archive,
                    max_retries,
                    execution,
                } => {
                    if !confirm_quiescent_source {
                        return Err("quiescent-source confirmation required".into());
                    }
                    serde_json::to_value(
                        groups::create(
                            paths,
                            &policies,
                            every_seconds,
                            store::Options {
                                delete_after_archive,
                                confirm_original_source,
                                allow_repeat: false,
                                source_stability:
                                    coldctl_core::archive::planner::SourceStability::QuiescentCopy,
                                execution: execution.apply(ExecutionControls {
                                    max_duration_seconds: Some(300),
                                    ..Default::default()
                                })?,
                                max_retries,
                            },
                        )
                        .await?,
                    )?
                }
                GroupCommand::List => serde_json::to_value(groups::list(paths)?)?,
                GroupCommand::Show { id } => serde_json::to_value(groups::show(paths, &id)?)?,
                GroupCommand::Stop { id } => serde_json::to_value(groups::stop(paths, &id)?)?,
                GroupCommand::Retry {
                    id,
                    confirm_original_source,
                } => {
                    if !confirm_original_source {
                        return Err("original-source confirmation required".into());
                    }
                    serde_json::to_value(groups::retry(paths, &id)?)?
                }
            };
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Command::Recurring { command } => {
            use coldctl_core::state::recurring;
            let value = match command {
                RecurringCommand::Create {
                    policy,
                    every_seconds,
                    source_stability,
                    confirm_original_source,
                    delete_after_archive,
                    max_retries,
                    execution,
                } => {
                    let controls = ExecutionControls {
                        max_duration_seconds: Some(300),
                        ..Default::default()
                    };
                    serde_json::to_value(recurring::create(
                        paths,
                        &policy,
                        every_seconds,
                        store::Options {
                            delete_after_archive,
                            source_stability: source_stability.into(),
                            allow_repeat: false,
                            confirm_original_source,
                            execution: execution.apply(controls)?,
                            max_retries,
                        },
                    )?)?
                }
                RecurringCommand::List => serde_json::to_value(recurring::list(paths)?)?,
                RecurringCommand::Show { id } => {
                    serde_json::to_value(recurring::show(paths, &id)?)?
                }
                RecurringCommand::Stop { id } => {
                    serde_json::to_value(recurring::stop(paths, &id)?)?
                }
                RecurringCommand::Retry {
                    id,
                    confirm_original_source,
                } => {
                    if !confirm_original_source {
                        return Err("original-source confirmation required".into());
                    }
                    serde_json::to_value(recurring::retry(paths, &id)?)?
                }
            };
            println!("{}", serde_json::to_string_pretty(&value)?);
        }
        Command::Enqueue {
            policy,
            job,
            source_stability,
            confirm_original_source,
            allow_repeat,
            delete_after_archive,
            delay_seconds,
            max_retries,
            execution,
        } => {
            let mut controls = if let Some(id) = &job {
                archive::job_show(paths, id)?.execution
            } else {
                ExecutionControls::default()
            };
            controls.max_duration_seconds = Some(60);
            let entry = store::enqueue(
                paths,
                &policy,
                job.as_deref(),
                store::Options {
                    delete_after_archive,
                    source_stability: source_stability.into(),
                    confirm_original_source,
                    allow_repeat,
                    execution: execution.apply(controls)?,
                    max_retries,
                },
                delay_seconds,
            )?;
            println!("{}", serde_json::to_string_pretty(&entry)?);
        }
        Command::Run { once } => {
            let signals = crate::execution::Signals::install()?;
            let worker = Worker::acquire(paths)?;
            loop {
                if signals.shutdown.requested() {
                    break;
                }
                let entry = worker.tick(signals.shutdown.clone()).await?;
                if let Some(entry) = &entry {
                    println!("{}", serde_json::to_string(entry)?);
                }
                if once {
                    break;
                }
                if entry.is_none() {
                    coldctl_core::scheduler::idle(&signals.shutdown).await;
                }
            }
        }
        Command::List => println!("{}", serde_json::to_string_pretty(&store::list(paths)?)?),
        Command::Show { id } => println!(
            "{}",
            serde_json::to_string_pretty(&store::show(paths, &id)?)?
        ),
        Command::Cancel { id } => {
            store::cancel(paths, &id)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&store::show(paths, &id)?)?
            );
        }
        Command::Retry {
            id,
            confirm_original_source,
        } => println!(
            "{}",
            serde_json::to_string_pretty(&store::retry(paths, &id, confirm_original_source)?)?
        ),
    }
    Ok(())
}
