use crate::output::{Output, text};
use coldctl_core::{paths::StatePaths, state::destinations};

pub fn run(
    paths: &StatePaths,
    name: &str,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let destination = destinations::show(paths, name)?;
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&destination)?),
        Output::Table => println!(
            "Name        {}\nID          {}\nType        {}\nPath        {}\nCreated     {}",
            destination.name,
            text(&destination.id),
            destination.destination_type,
            text(&destination.path.to_string_lossy()),
            text(&destination.created_at)
        ),
    }
    Ok(())
}
