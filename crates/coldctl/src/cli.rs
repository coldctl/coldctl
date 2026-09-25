use clap::Parser;

use crate::commands;

#[derive(Parser)]
#[command(
    name = "coldctl",
    version,
    about = "Archive and query cold database data"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<commands::Commands>,
}
