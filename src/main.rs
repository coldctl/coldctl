use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "coldctl",
    version,
    about = "Archive and query cold database data"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    /// Analyze a database for cold data
    Analyze,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Analyze) => {
            println!("coldctl analyze is coming soon.");
        }
        None => {
            println!("coldctl - cold data lifecycle tooling");
            println!("Run `coldctl --help` for usage.");
        }
    }
}
