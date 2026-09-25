mod cli;
mod commands;

use clap::Parser;
use cli::Cli;
use commands::Commands;

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Init)
        | Some(Commands::Login)
        | Some(Commands::Status)
        | Some(Commands::Source(_))
        | Some(Commands::Destination(_))
        | Some(Commands::Policy(_))
        | Some(Commands::Archive(_))
        | Some(Commands::Jobs(_))
        | Some(Commands::Agent(_)) => {}
        None => {
            println!("coldctl - cold data lifecycle tooling");
            println!("Run `coldctl --help` for usage.");
        }
    }
}
