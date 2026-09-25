use clap::Subcommand;

mod list;
mod show;

#[derive(Subcommand)]
pub enum Command {
    List,
    Show,
}
