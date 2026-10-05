mod cli;
mod commands;
mod output;
mod progress;

use clap::Parser;
use cli::Cli;
use coldctl_core::paths::StatePaths;
use commands::Commands;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            if let Some(error) = error.downcast_ref::<coldctl_core::error::Error>() {
                let failure = error.failure();
                output::error(&crate::output::text(&failure.message));
                eprintln!(
                    "Category: {:?}; retryable: {}. {}",
                    failure.category, failure.retryable, failure.next_step
                );
                if let coldctl_core::error::Error::JobFailed { job_id, .. } = error {
                    eprintln!("Inspect: coldctl jobs show {}", crate::output::text(job_id));
                }
            } else {
                output::error(&crate::output::text(&error.to_string()));
            }
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<(), Box<dyn std::error::Error>> {
    match cli.command {
        Some(Commands::Init) => {
            commands::init::run(&StatePaths::resolve(cli.data_dir.as_deref())?)?
        }
        Some(Commands::Status) => {
            commands::status::run(&StatePaths::resolve(cli.data_dir.as_deref())?)?
        }
        Some(Commands::State(args)) => {
            commands::state::run(args, &StatePaths::resolve(cli.data_dir.as_deref())?)?
        }
        Some(Commands::Source(args)) => {
            commands::source::run(args, &StatePaths::resolve(cli.data_dir.as_deref())?).await?
        }
        Some(Commands::Analyze(args)) => {
            commands::analyze::run(args, &StatePaths::resolve(cli.data_dir.as_deref())?).await?
        }
        Some(Commands::Destination(args)) => {
            commands::destination::run(args, &StatePaths::resolve(cli.data_dir.as_deref())?)?
        }
        Some(Commands::Policy(args)) => {
            commands::policy::run(args, &StatePaths::resolve(cli.data_dir.as_deref())?).await?
        }
        Some(Commands::Archive(args)) => {
            commands::archive::run(args, &StatePaths::resolve(cli.data_dir.as_deref())?).await?
        }
        Some(Commands::Jobs(args)) => {
            commands::jobs::run(args, &StatePaths::resolve(cli.data_dir.as_deref())?).await?
        }
        Some(Commands::Agent(args)) => {
            commands::agent::run(args, &StatePaths::resolve(cli.data_dir.as_deref())?).await?;
        }
        None => {
            println!("coldctl - cold data lifecycle tooling");
            println!("Run `coldctl --help` for usage.");
        }
    }
    Ok(())
}
mod execution;
