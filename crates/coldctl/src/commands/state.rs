use crate::output::{Output, text};
use clap::{Args as ClapArgs, Subcommand};
use coldctl_core::{paths::StatePaths, state::backup};
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long, global=true, value_enum, default_value_t=Output::Table)]
    output: Output,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Consistent SQLite snapshot, including WAL; archive files are not copied.
    Backup {
        /// New file outside the state directory; its parent must already exist.
        #[arg(long)]
        to: PathBuf,
    },
}
pub fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Backup { to } => {
            let report = backup::create(paths, &to)?;
            if matches!(args.output, Output::Json) {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "State backup: {}\nBytes: {}\nSHA-256: {}",
                    text(&report.path.to_string_lossy()),
                    report.bytes,
                    report.sha256
                );
                println!(
                    "Archives are not included. Stop all workers before creating a matched state-and-files recovery set."
                );
            }
        }
    }
    Ok(())
}
