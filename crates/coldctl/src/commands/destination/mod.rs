use crate::output::Output;
use clap::{Args as ClapArgs, Subcommand};
use coldctl_core::paths::StatePaths;

mod add;
mod list;
mod remove;
mod show;
mod test;

#[derive(ClapArgs)]
pub struct Args {
    #[arg(long, global = true, value_enum, default_value_t = Output::Table)]
    pub output: Output,
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Save a destination configuration without touching its directory.
    Add {
        #[command(subcommand)]
        destination: AddCommand,
    },
    /// List saved destinations without checking storage access.
    List,
    /// Show saved destination metadata and its absolute path.
    Show { name: String },
    /// Create the directory if needed, then write, sync, read, and delete a temporary probe.
    Test { name: String },
    /// Remove configuration only; archive files and directories are preserved.
    Remove { name: String },
}

#[derive(Subcommand)]
pub enum AddCommand {
    /// Configure a local directory; relative paths resolve against the current directory.
    Local(add::Args),
}

pub fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Add {
            destination: AddCommand::Local(options),
        } => add::run(options, paths, args.output),
        Command::List => list::run(paths, args.output),
        Command::Show { name } => show::run(paths, &name, args.output),
        Command::Test { name } => test::run(paths, &name, args.output),
        Command::Remove { name } => remove::run(paths, &name, args.output),
    }
}
