use clap::Subcommand;

pub mod agent;
pub mod analyze;
pub mod archive;
pub mod connector;
pub mod destination;
pub mod init;
pub mod jobs;
pub mod policy;
pub mod source;
pub mod state;
pub mod status;

#[derive(Subcommand)]
pub enum Commands {
    /// Install and manage independently signed connector packages.
    Connector(connector::Args),
    /// Initialize local state (safe to run repeatedly).
    Init,
    /// Show local installation status without network access.
    Status,
    /// Safely back up the local SQLite state.
    State(state::Args),
    /// Configure, test, and discover operational database sources.
    Source(source::Args),
    /// Assess archival candidates from metadata and existing statistics, without scanning rows.
    Analyze(analyze::Args),
    /// Configure and test archive storage destinations.
    Destination(destination::Args),
    Policy(policy::Args),
    Archive(archive::Args),
    Jobs(jobs::Args),
    #[command(subcommand)]
    Agent(agent::Command),
}
