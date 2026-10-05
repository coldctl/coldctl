use clap::Parser;

use crate::commands;

#[derive(Parser)]
#[command(
    name = "coldctl",
    version,
    about = "Analyze, archive, verify and restore PostgreSQL data"
)]
pub struct Cli {
    /// Override the OS application data directory.
    #[arg(long, global = true, value_name = "PATH")]
    pub data_dir: Option<std::path::PathBuf>,
    #[command(subcommand)]
    pub command: Option<commands::Commands>,
}
