use clap::{Args as ClapArgs, Subcommand};
use coldctl_core::paths::StatePaths;

mod add;
mod discover;
mod list;
mod remove;
mod show;
mod test;

#[derive(ClapArgs)]
pub struct Args {
    /// Output format; JSON contains no resolved credentials.
    #[arg(long, global = true, value_enum, default_value_t = Output::Table)]
    pub output: Output,
    #[command(subcommand)]
    pub command: Command,
}

pub use crate::output::Output;

#[derive(Subcommand)]
pub enum Command {
    /// Save source configuration locally without connecting.
    Add {
        #[command(subcommand)]
        source: AddCommand,
    },
    /// List configured sources without connectivity checks.
    List,
    /// Show metadata and credential references, never credential values.
    Show { name: String },
    /// Connect and verify database authentication and database access.
    Test { name: String },
    /// Read schemas, tables, columns, keys, indexes, and row estimates.
    Discover { name: String },
    /// Remove local configuration only; leaves the database and archives untouched.
    Remove { name: String },
}

#[derive(Subcommand)]
pub enum AddCommand {
    /// Configure one PostgreSQL TCP endpoint. TLS is required by default.
    Postgres(add::Args),
    /// Configure one MySQL TCP endpoint. TLS is required by default.
    Mysql(add::Args),
    /// Configure a MongoDB primary endpoint. TLS is required by default.
    Mongodb(add::Args),
}

pub async fn run(args: Args, paths: &StatePaths) -> Result<(), Box<dyn std::error::Error>> {
    match args.command {
        Command::Add {
            source: AddCommand::Postgres(options),
        } => add::run(options, paths, args.output, "postgres")?,
        Command::Add {
            source: AddCommand::Mysql(options),
        } => add::run(options, paths, args.output, "mysql")?,
        Command::Add {
            source: AddCommand::Mongodb(options),
        } => add::run(options, paths, args.output, "mongodb")?,
        Command::List => list::run(paths, args.output)?,
        Command::Show { name } => show::run(paths, &name, args.output)?,
        Command::Remove { name } => remove::run(paths, &name, args.output)?,
        Command::Test { name } => test::run(paths, &name, args.output).await?,
        Command::Discover { name } => discover::run(paths, &name, args.output).await?,
    }
    Ok(())
}

fn json(value: &serde_json::Value) -> Result<(), serde_json::Error> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}

use crate::output::text;
