use super::{AccessCheck, ArchiveDestination};
use crate::error::Error;
use std::{
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

pub struct LocalDestination {
    root: PathBuf,
}

fn access_error(operation: &'static str, path: &Path, source: std::io::Error) -> Error {
    Error::DestinationAccess {
        operation,
        path: path.to_owned(),
        source,
    }
}

/// Resolve relative paths once, when configured; do not create or touch the destination.
pub fn resolve_path(path: &Path) -> Result<PathBuf, Error> {
    let text = path.to_str().ok_or(Error::DestinationConfiguration(
        "path must be valid Unicode",
    ))?;
    if text.is_empty() || text.chars().any(char::is_control) {
        return Err(Error::DestinationConfiguration(
            "path must be nonempty and contain no control characters",
        ));
    }
    std::path::absolute(path).map_err(|e| access_error("resolve path", path, e))
}

impl LocalDestination {
    pub(crate) fn has_capacity(&self, reserve: u64, additional: u64) -> Result<bool, Error> {
        let mut path = self.root.as_path();
        while !path
            .try_exists()
            .map_err(|e| access_error("inspect capacity path", path, e))?
        {
            path = path
                .parent()
                .ok_or(Error::Archive("cannot find destination filesystem"))?;
        }
        let available = fs2::available_space(path)
            .map_err(|e| access_error("inspect available space", path, e))?;
        Ok(reserve
            .checked_add(additional)
            .is_some_and(|required| available >= required))
    }
    /// A capacity floor, not an estimate or reservation for the entire archive.
    pub fn preflight(&self, minimum_free_bytes: u64) -> Result<u64, Error> {
        self.test_access()?;
        let available = fs2::available_space(&self.root)
            .map_err(|e| access_error("inspect available space", &self.root, e))?;
        if available < minimum_free_bytes {
            return Err(Error::Archive(
                "destination has insufficient available space for the preflight minimum",
            ));
        }
        Ok(available)
    }
    pub fn new(root: PathBuf) -> Result<Self, Error> {
        if !root.is_absolute() {
            return Err(Error::DestinationConfiguration(
                "stored destination path must be absolute",
            ));
        }
        resolve_path(&root)?;
        crate::fs_security::check(&root)?;
        Ok(Self { root })
    }
}

impl ArchiveDestination for LocalDestination {
    fn put_file(&self, key: &str, source: &Path) -> Result<super::StoredObject, Error> {
        let target = self.object_path(key)?;
        let parent = target
            .parent()
            .ok_or(Error::DestinationConfiguration("object parent missing"))?;
        crate::fs_security::create_dir(parent)?;
        let (bytes, sha256) = fingerprint(source)?;
        let mut staged = tempfile::Builder::new()
            .prefix(".coldctl-partial-")
            .tempfile_in(parent)
            .map_err(|e| access_error("stage object", parent, e))?;
        std::io::copy(
            &mut File::open(source).map_err(|e| access_error("open encoded object", source, e))?,
            &mut staged,
        )
        .map_err(|e| access_error("copy object", &target, e))?;
        staged
            .as_file()
            .sync_all()
            .map_err(|e| access_error("sync object", &target, e))?;
        staged
            .persist_noclobber(&target)
            .map_err(|e| access_error("publish object without overwrite", &target, e.error))?;
        let object = super::StoredObject {
            key: key.into(),
            bytes,
            sha256,
        };
        self.verify(&object)?;
        Ok(object)
    }
    fn verify(&self, object: &super::StoredObject) -> Result<(), Error> {
        let (bytes, sha256) = fingerprint(&self.object_path(&object.key)?)?;
        if bytes != object.bytes || sha256 != object.sha256 {
            return Err(Error::Archive(
                "archive object size/checksum verification failed",
            ));
        }
        Ok(())
    }
    /// A test writes only its unique create-new probe; existing files are never overwritten.
    fn test_access(&self) -> Result<AccessCheck, Error> {
        crate::fs_security::create_dir(&self.root).map_err(|error| match error {
            Error::Filesystem { source, .. } => {
                access_error("create directory", &self.root, source)
            }
            other => other,
        })?;
        let mut probe = tempfile::Builder::new()
            .prefix(".coldctl-probe-")
            .tempfile_in(&self.root)
            .map_err(|e| access_error("create probe", &self.root, e))?;
        let probe_path = probe.path().to_owned();
        let checked = (|| {
            let payload = b"coldctl destination access probe\n";
            probe
                .write_all(payload)
                .map_err(|e| access_error("write probe", &probe_path, e))?;
            probe
                .as_file()
                .sync_all()
                .map_err(|e| access_error("sync probe", &probe_path, e))?;
            let mut content = Vec::new();
            File::open(&probe_path)
                .and_then(|file| {
                    file.take(payload.len() as u64 + 1)
                        .read_to_end(&mut content)
                })
                .map_err(|e| access_error("read probe", &probe_path, e))?;
            if content != payload {
                return Err(access_error(
                    "verify probe",
                    &probe_path,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "probe contents did not match",
                    ),
                ));
            }
            Ok(())
        })();
        // Explicit cleanup on both success and failure; report its path if deletion fails.
        probe
            .close()
            .map_err(|e| access_error("remove probe", &probe_path, e))?;
        checked?;
        Ok(AccessCheck {
            path: self.root.clone(),
            writable: true,
            readable: true,
            probe_removed: true,
        })
    }
}

pub(crate) fn fingerprint(path: &Path) -> Result<(u64, String), Error> {
    use sha2::{Digest, Sha256};
    crate::fs_security::check(path)?;
    let mut file = File::open(path).map_err(|e| access_error("open object", path, e))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 32768];
    let mut bytes = 0u64;
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|e| access_error("read object", path, e))?;
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        bytes += count as u64;
    }
    Ok((bytes, format!("{:x}", hasher.finalize())))
}
impl LocalDestination {
    pub(crate) fn object_path(&self, key: &str) -> Result<PathBuf, Error> {
        if key.is_empty()
            || key
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.'))
        {
            return Err(Error::DestinationConfiguration(
                "invalid archive object key",
            ));
        }
        let path = self.root.join(key);
        crate::fs_security::check(&path)?;
        Ok(path)
    }
}
