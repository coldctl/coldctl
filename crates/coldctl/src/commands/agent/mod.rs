use clap::Subcommand;

mod register;
mod run;
mod status;

#[derive(Subcommand)]
pub enum Command {
    Register,
    Status,
    Run,
}
