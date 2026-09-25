use clap::Subcommand;

mod add;
mod list;
mod test;

#[derive(Subcommand)]
pub enum Command {
    Add,
    List,
    Test,
}
