use crate::output::Output;
use coldctl_core::{paths::StatePaths, state::destinations};

pub fn run(
    paths: &StatePaths,
    name: &str,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    destinations::remove(paths, name)?;
    match output {
        Output::Json => println!("{}", serde_json::json!({ "removed": name })),
        Output::Table => crate::output::success(&format!(
            "Destination '{name}' removed from local configuration. Archive files were preserved."
        )),
    }
    Ok(())
}
