use crate::output::{Output, text};
use coldctl_core::{
    destination::{ArchiveDestination, local::LocalDestination},
    paths::StatePaths,
    state::destinations,
};

pub fn run(
    paths: &StatePaths,
    name: &str,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    let destination = destinations::show(paths, name)?;
    let checked = LocalDestination::new(destination.path)?.test_access()?;
    match output {
        Output::Json => println!("{}", serde_json::to_string_pretty(&checked)?),
        Output::Table => {
            crate::output::success(&format!(
                "Destination '{name}' passed write, sync, read-back, and cleanup checks."
            ));
            println!("  Path: {}", text(&checked.path.to_string_lossy()));
        }
    }
    Ok(())
}
