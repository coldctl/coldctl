use clap::Subcommand;

pub mod agent;
pub mod archive;
pub mod destination;
pub mod jobs;
pub mod policy;
pub mod source;

#[derive(Subcommand)]
pub enum Commands {
    Init,
    Login,
    Status,
    #[command(subcommand)]
    Source(source::Command),
    #[command(subcommand)]
    Destination(destination::Command),
    #[command(subcommand)]
    Policy(policy::Command),
    #[command(subcommand)]
    Archive(archive::Command),
    #[command(subcommand)]
    Jobs(jobs::Command),
    #[command(subcommand)]
    Agent(agent::Command),
}
