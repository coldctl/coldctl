use crate::output::{Output, text};
use coldctl_core::{destination, paths::StatePaths, state::destinations};

pub fn run(
    paths: &StatePaths,
    name: &str,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let destination = destinations::show(paths, name)?;
    let checked =
        destination::open(destination.config.as_ref(), &destination.path)?.test_access()?;
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&checked)?),
        Output::Table => {
            crate::output::success(&format!(
                "Destination '{name}' passed write, read-back integrity, and probe cleanup checks."
            ));
            println!("  Path: {}", text(&checked.path.to_string_lossy()));
        }
    }
    Ok(())
}
