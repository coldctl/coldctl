use clap::Subcommand;

mod create;
mod list;
mod show;

#[derive(Subcommand)]
pub enum Command {
    Create,
    List,
    Show,
}
