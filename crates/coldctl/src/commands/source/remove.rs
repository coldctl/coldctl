use super::Output;
use coldctl_core::{paths::StatePaths, state::sources};

pub fn run(
    paths: &StatePaths,
    name: &str,
    output: Output,
) -> Result<(), Box<dyn std::error::Error>> {
    sources::remove(paths, name)?;
    match output {
        Output::Json => super::json(&serde_json::json!({ "removed": name }))?,
        Output::Table => crate::output::success(&format!(
            "Source '{name}' removed from local configuration."
        )),
    }
    Ok(())
}
