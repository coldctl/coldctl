use clap::{Args, Subcommand};
use coldctl_core::{paths::StatePaths, protection};
#[derive(Args)]
pub struct Arguments {
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand)]
pub enum Command {
    /// Replace source-scoped policy; clears old monitoring sample. JSON file contains no secrets.
    Set {
        /// Registered source name.
        source: String,
        #[arg(long)]
        /// Path to the protection policy JSON file.
        config: std::path::PathBuf,
    },
    /// Submit a fresh source-ID-bound sample from your trusted monitoring collector.
    Sample {
        /// Registered source name.
        source: String,
        #[arg(long)]
        /// Path to a fresh monitoring sample JSON file.
        file: std::path::PathBuf,
    },
    /// Show policy, telemetry and last admission decision.
    Status {
        /// Registered source name.
        source: String,
    },
    /// Disable source protection explicitly.
    Clear {
        /// Registered source name.
        source: String,
        #[arg(long, required = true)]
        /// Confirm disabling all protection for this source.
        confirm: bool,
    },
}
fn read<T: serde::de::DeserializeOwned>(
    path: &std::path::Path,
) -> Result<T, Box<dyn std::error::Error>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(path)?
        .take(16_385)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 16_384 {
        return Err("configuration exceeds 16 KiB".into());
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid protection JSON".into())
}
pub fn run(args: Arguments, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Set { source, config } => {
            protection::set(paths, &source, read(&config)?)?;
            println!("Protection saved; publish fresh telemetry before work can proceed.");
        }
        Command::Sample { source, file } => {
            protection::sample(paths, &source, read(&file)?)?;
            println!("Monitoring sample accepted.");
        }
        Command::Status { source } => {
            protection::check(paths, &source)?;
            println!(
                "{}",
                serde_json::to_string_pretty(&protection::status(paths, &source)?)?
            );
        }
        Command::Clear { source, confirm: _ } => {
            protection::clear(paths, &source)?;
            println!("Protection disabled.");
        }
    }
    Ok(())
}
