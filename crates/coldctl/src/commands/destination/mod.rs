use crate::output::Output;
use clap::{Args as ClapArgs, Subcommand};
use coldctl_core::paths::StatePaths;

mod add;
mod azure;
mod list;
mod remove;
mod s3;
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
    /// Save a destination configuration without contacting storage.
    Add {
        #[command(subcommand)]
        destination: AddCommand,
    },
    /// List saved destinations without checking storage access.
    List,
    /// Show saved storage configuration and local staging path.
    Show { name: String },
    /// Check storage access with a unique write, verified read and probe cleanup.
    Test { name: String },
    /// Remove configuration only; archive files and directories are preserved.
    Remove { name: String },
}

#[derive(Subcommand)]
pub enum AddCommand {
    /// Configure a local directory; relative paths resolve against the current directory.
    Local(add::Args),
    /// Configure customer-owned S3 or a loopback S3 emulator.
    S3(Box<s3::Args>),
    /// Configure customer-owned Azure Blob Storage or Azurite.
    Azure(Box<azure::Args>),
}

pub fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Add {
            destination: AddCommand::Local(options),
        } => add::run(options, paths, args.output),
        Command::Add {
            destination: AddCommand::S3(options),
        } => s3::run(*options, paths, args.output),
        Command::Add {
            destination: AddCommand::Azure(options),
        } => azure::run(*options, paths, args.output),
        Command::List => list::run(paths, args.output),
        Command::Show { name } => show::run(paths, &name, args.output),
        Command::Test { name } => test::run(paths, &name, args.output),
        Command::Remove { name } => remove::run(paths, &name, args.output),
    }
}
