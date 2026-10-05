use crate::output::{Output, text};
use clap::Args as ClapArgs;
use coldctl_core::{paths::StatePaths, state::destinations};
use std::path::PathBuf;

#[derive(ClapArgs)]
pub struct Args {
    /// Unique local name (ASCII letters, digits, hyphens, underscores).
    #[arg(long)]
    name: String,
    /// Archive directory. Saved as an absolute path; created by destination test if missing.
    #[arg(long)]
    path: PathBuf,
}

pub fn run(
    args: Args,
    paths: &StatePaths,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let destination = destinations::add_local(paths, &args.name, &args.path)?;
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&destination)?),
        Output::Table => {
            crate::output::success(&format!("Destination '{}' configured.", destination.name));
            println!("  Path: {}", text(&destination.path.to_string_lossy()));
            println!(
                "  Run `coldctl destination test {}` to check access.",
                destination.name
            );
        }
    }
    Ok(())
}
