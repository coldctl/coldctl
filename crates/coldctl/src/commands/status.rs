use coldctl_core::{error::Error, paths::StatePaths, state};

pub fn run(paths: &StatePaths) -> Result<(), Error> {
    let installation = state::status(paths)?;
    println!("Coldctl\n");
    println!("Version          {}", env!("CARGO_PKG_VERSION"));
    println!("Data directory   {}", paths.data_dir.display());
    match installation {
        Some(installation) => {
            println!("Installation     initialized");
            println!("Installation ID  {}", installation.id);
            println!("Initialized at   {}", installation.initialized_at);
        }
        None => println!("Installation     not initialized (run `coldctl init`)"),
    }
    println!("Cloud            not connected");
    Ok(())
}
