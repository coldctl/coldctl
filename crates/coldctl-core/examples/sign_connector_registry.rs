//! Offline publisher utility. Uses existing authorized TUF keys; never creates or uploads keys.
use std::{num::NonZeroU64, path::PathBuf};
use tough::{
    editor::RepositoryEditor,
    key_source::{KeySource, LocalKeySource},
};
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() < 6 {
        return Err("usage: sign_connector_registry ROOT_JSON TARGETS_DIR NEW_OUT_DIR METADATA_VERSION EXPIRES_RFC3339 KEY_PEM [KEY_PEM ...]".into());
    }
    let source = PathBuf::from(&args[1]);
    let output = PathBuf::from(&args[2]);
    if output.exists() {
        return Err("output directory must not exist".into());
    }
    let version: NonZeroU64 = args[3].parse()?;
    let expires = args[4].parse()?;
    let catalog: coldctl_core::connectors::model::Catalog =
        serde_json::from_slice(&std::fs::read(source.join("catalog.json"))?)?;
    catalog.validate()?;
    let mut editor = RepositoryEditor::new(&args[0]).await?;
    editor
        .targets_version(version)?
        .targets_expires(expires)?
        .snapshot_version(version)
        .snapshot_expires(expires)
        .timestamp_version(version)
        .timestamp_expires(expires);
    for file in std::fs::read_dir(&source)? {
        let file = file?;
        if !file.file_type()?.is_file()
            || !coldctl_core::connectors::model::filename(&file.file_name().to_string_lossy())
        {
            return Err("only flat, regular targets are supported".into());
        }
        editor.add_target_path(file.path()).await?;
    }
    let keys: Vec<Box<dyn KeySource>> = args[5..]
        .iter()
        .map(|path| {
            Box::new(LocalKeySource {
                path: PathBuf::from(path),
            }) as Box<dyn KeySource>
        })
        .collect();
    let signed = editor.sign(&keys).await?;
    signed.write(output.join("metadata")).await?;
    signed
        .copy_targets(
            &source,
            output.join("targets"),
            tough::editor::signed::PathExists::Fail,
        )
        .await?;
    println!(
        "Signed registry prepared locally. Review expiry, metadata versions and revocations before publishing."
    );
    Ok(())
}
