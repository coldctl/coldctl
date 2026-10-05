use super::Output;
use coldctl_core::{paths::StatePaths, state::sources};

pub fn run(paths: &StatePaths, output: Output) -> Result<(), Box<dyn std::error::Error>> {
    let sources = sources::list(paths)?;
    match output {
        Output::Json => super::json(&serde_json::to_value(sources)?)?,
        Output::Table => {
            if sources.is_empty() {
                println!("No sources configured. Use `coldctl source add postgres --help`.");
            } else {
                println!("{:<24} {:<12} STATUS", "NAME", "TYPE");
                for source in sources {
                    println!("{:<24} {:<12} configured", source.name, source.source_type);
                }
            }
        }
    }
    Ok(())
}
