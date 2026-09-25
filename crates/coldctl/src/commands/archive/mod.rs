use clap::Subcommand;

mod plan;
mod run;

#[derive(Subcommand)]
pub enum Command {
    Plan,
    Run,
}
