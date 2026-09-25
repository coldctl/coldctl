use clap::Subcommand;

mod add;
mod discover;
mod list;
mod test;

#[derive(Subcommand)]
pub enum Command {
    Add,
    List,
    Test,
    Discover,
}
