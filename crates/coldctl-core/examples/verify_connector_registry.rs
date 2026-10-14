//! Verify all previous release targets before carrying them into another signed release.
use futures_util::StreamExt;
use std::{io::Write, path::PathBuf};
use tough::{RepositoryLoader, TargetName};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 3 {
        return Err(
            "usage: verify_connector_registry TRUSTED_ROOT PREVIOUS_REPOSITORY NEW_TARGETS_DIR"
                .into(),
        );
    }
    let previous = std::fs::canonicalize(&args[1])?;
    let root = std::fs::read(&args[0])?;
    let repository = RepositoryLoader::new(
        &root,
        url::Url::from_directory_path(previous.join("metadata")).map_err(|_| "invalid path")?,
        url::Url::from_directory_path(previous.join("targets")).map_err(|_| "invalid path")?,
    )
    .load()
    .await?;
    if repository
        .targets()
        .signed
        .delegations
        .as_ref()
        .is_some_and(|delegations| !delegations.roles.is_empty())
    {
        return Err("delegated registry unsupported".into());
    }
    let output = PathBuf::from(&args[2]);
    std::fs::create_dir(&output)?;
    let mut total = 0u64;
    for (name, target) in &repository.targets().signed.targets {
        let name = name.raw();
        if !coldctl_core::connectors::model::filename(name) || target.length > 256 * 1024 * 1024 {
            return Err("invalid previous target".into());
        }
        total = total.checked_add(target.length).ok_or("size overflow")?;
        if total > 1500 * 1024 * 1024 {
            return Err("registry exceeds release size budget".into());
        }
        let mut stream = repository
            .read_target(&TargetName::new(name)?)
            .await?
            .ok_or("missing target")?;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(output.join(name))?;
        while let Some(chunk) = stream.next().await {
            file.write_all(&chunk?)?;
        }
    }
    let catalog: coldctl_core::connectors::model::Catalog =
        serde_json::from_slice(&std::fs::read(output.join("catalog.json"))?)?;
    catalog.validate()?;
    println!("Previous registry signatures, expiry and target hashes verified.");
    Ok(())
}
