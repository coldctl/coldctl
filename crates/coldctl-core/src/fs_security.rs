//! Defense in depth for operator-owned storage, not protection against a racing writer.
use crate::error::Error;
use std::{io, path::Path};

fn failure(path: &Path, source: io::Error) -> Error {
    Error::Filesystem {
        path: path.to_owned(),
        source,
    }
}

/// Check every existing component, including ancestors and dangling links.
pub(crate) fn check(path: &Path) -> Result<(), Error> {
    let absolute = std::path::absolute(path).map_err(|e| failure(path, e))?;
    for component in absolute.ancestors() {
        match std::fs::symlink_metadata(component) {
            Ok(metadata) => {
                #[cfg(windows)]
                let linked = {
                    use std::os::windows::fs::MetadataExt;
                    metadata.file_attributes() & 0x400 != 0
                };
                #[cfg(not(windows))]
                let linked = metadata.file_type().is_symlink();
                if linked || (!metadata.is_file() && !metadata.is_dir()) {
                    return Err(failure(
                        component,
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "storage paths must not contain links, reparse points, or special files",
                        ),
                    ));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(failure(component, e)),
        }
    }
    Ok(())
}

/// Newly created Unix directories are owner-only. Existing ACLs/modes are not changed.
pub(crate) fn create_dir(path: &Path) -> Result<(), Error> {
    check(path)?;
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path).map_err(|e| failure(path, e))?;
    check(path)
}

/// Pre-create SQLite files privately; SQLite may subsequently create sibling journals.
pub(crate) fn sqlite(path: &Path) -> Result<(), Error> {
    check_sqlite(path)?;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(failure(path, e)),
    }
}

pub(crate) fn check_sqlite(path: &Path) -> Result<(), Error> {
    check(path)?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        check(Path::new(&name))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn private_state_creation_and_existing_contents() {
        let temp = tempfile::tempdir().unwrap();
        let dir = temp.path().join("private/nested");
        create_dir(&dir).unwrap();
        let file = dir.join("state.db");
        sqlite(&file).unwrap();
        std::fs::write(&file, b"existing").unwrap();
        sqlite(&file).unwrap();
        assert_eq!(std::fs::read(&file).unwrap(), b"existing");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }
    #[cfg(windows)]
    #[test]
    fn rejects_windows_junction_ancestors() {
        let temp = tempfile::tempdir().unwrap();
        let real = temp.path().join("real");
        create_dir(&real).unwrap();
        let link = temp.path().join("junction");
        let output = std::process::Command::new("cmd")
            .args(["/C", "mklink", "/J"])
            .arg(&link)
            .arg(&real)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(create_dir(&link.join("child")).is_err());
        assert!(sqlite(&link.join("state.db")).is_err());
        assert!(!real.join("state.db").exists());
        std::fs::remove_dir(&link).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn rejects_linked_parent_and_sqlite_sidecar() {
        use std::os::unix::fs::symlink;
        let temp = tempfile::tempdir().unwrap();
        let real = temp.path().join("real");
        create_dir(&real).unwrap();
        let link = temp.path().join("link");
        symlink(&real, &link).unwrap();
        assert!(create_dir(&link.join("child")).is_err());
        let file = real.join("state.db");
        symlink(real.join("missing"), real.join("state.db-wal")).unwrap();
        assert!(sqlite(&file).is_err());
        assert!(!file.exists());
    }
}
