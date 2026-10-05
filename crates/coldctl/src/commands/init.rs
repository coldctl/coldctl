use coldctl_core::{error::Error, paths::StatePaths, state};

pub fn run(paths: &StatePaths) -> Result<(), Error> {
    let outcome = state::initialize(paths, env!("CARGO_PKG_VERSION"))?;
    crate::output::success(if outcome.created {
        "Coldctl initialized successfully."
    } else {
        "Coldctl is already initialized."
    });
    println!("  Data directory: {}", paths.data_dir.display());
    Ok(())
}
