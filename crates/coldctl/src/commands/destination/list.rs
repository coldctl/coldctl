use crate::output::{Output, text};
use coldctl_core::{paths::StatePaths, state::destinations};

pub fn run(paths: &StatePaths, output: Output) -> Result<(), Box<dyn std::error::Error>> {
    let destinations = destinations::list(paths)?;
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&destinations)?),
        Output::Table => {
            if destinations.is_empty() {
                println!("No destinations configured. Use `coldctl destination add local --help`.");
            } else {
                println!("{:<24} {:<10} PATH", "NAME", "TYPE");
                for destination in destinations {
                    println!(
                        "{:<24} {:<10} {}",
                        destination.name,
                        destination.destination_type,
                        text(&destination.path.to_string_lossy())
                    );
                }
            }
        }
    }
    Ok(())
}
