//! Provider-independent create-only writes, bounded verified reads and access probes.
use super::{
    AccessCheck, ArchiveDestination, DestinationConfig, StoredObject,
    local::{LocalDestination, fingerprint},
};
use crate::error::Error;
use futures_util::StreamExt;
use object_store::PutMode;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};
const MAX_OBJECT: u64 = 128 * 1024 * 1024;
fn bad() -> Error {
    Error::DestinationConfiguration("invalid object key or size")
}
pub(super) fn network(_: impl std::fmt::Debug) -> Error {
    Error::Archive(
        "object storage request failed; check endpoint, credentials and permissions (remote diagnostics redacted)",
    )
}
fn remote(error: object_store::Error) -> Error {
    use object_store::Error::*;
    let (reason, retryable) = match error {
        NotFound { .. } => ("object missing", false),
        PermissionDenied { .. } => ("permission denied", false),
        Unauthenticated { .. } => ("authentication rejected", false),
        AlreadyExists { .. } | Precondition { .. } => (
            "create-only precondition failed; existing object preserved",
            false,
        ),
        NotSupported { .. } | NotImplemented => ("required storage operation unsupported", false),
        Generic { .. } | JoinError { .. } => (
            "request failed after bounded retries; remote details redacted",
            true,
        ),
        _ => ("request rejected; remote details redacted", false),
    };
    Error::ObjectStorage { reason, retryable }
}
pub(super) fn key_valid(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 900
        && key
            .split('/')
            .all(|s| !s.is_empty() && s != "." && s != "..")
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/-_.".contains(&b))
}
pub struct ObjectDestination {
    config: DestinationConfig,
    staging: PathBuf,
}
impl ObjectDestination {
    pub fn new(config: impl Into<DestinationConfig>, staging: PathBuf) -> Result<Self, Error> {
        let config = config.into();
        config.validate()?;
        LocalDestination::new(staging.clone())?;
        Ok(Self { config, staging })
    }
    // Existing CLI/verifier contracts are synchronous. Isolate the SDK runtime so these
    // calls also work from the archive executor's Tokio runtime, without nested block_on.
    fn request<T: Send>(
        &self,
        f: impl FnOnce(DestinationConfig) -> Result<T, Error> + Send,
    ) -> Result<T, Error> {
        std::thread::scope(|scope| {
            scope
                .spawn(|| f(self.config.clone()))
                .join()
                .map_err(|_| network("worker"))?
        })
    }
}
pub(super) fn runtime() -> Result<tokio::runtime::Runtime, Error> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(network)
}
impl ArchiveDestination for ObjectDestination {
    fn exists(&self, key: &str) -> Result<bool, Error> {
        self.request(|config| {
            runtime()?.block_on(async {
                match config.build()?.head(&config.key(key)?).await {
                    Ok(_) => Ok(true),
                    Err(object_store::Error::NotFound { .. }) => Ok(false),
                    Err(e) => Err(remote(e)),
                }
            })
        })
    }
    fn put_file(&self, key: &str, source: &Path) -> Result<StoredObject, Error> {
        let (bytes, sha256) = fingerprint(source)?;
        if bytes > MAX_OBJECT {
            return Err(Error::Archive("sink object exceeds 128 MiB batch limit"));
        }
        let mut payload = Vec::new();
        File::open(source)
            .map_err(network)?
            .take(MAX_OBJECT + 1)
            .read_to_end(&mut payload)
            .map_err(network)?;
        if payload.len() as u64 != bytes || format!("{:x}", Sha256::digest(&payload)) != sha256 {
            return Err(Error::Archive("staged object changed"));
        }
        self.request(move |config| {
            runtime()?.block_on(async {
                // Create-only: never use head-then-unconditional-PUT or trust ETags as checksums.
                config
                    .build()?
                    .put_opts(
                        &config.key(key)?,
                        bytes::Bytes::from(payload).into(),
                        PutMode::Create.into(),
                    )
                    .await
                    .map_err(remote)?;
                Ok(())
            })
        })?;
        let object = StoredObject {
            key: key.to_owned(),
            bytes,
            sha256,
        };
        self.verify(&object)?;
        Ok(object)
    }
    fn read_verified(&self, object: &StoredObject) -> Result<File, Error> {
        if object.bytes > MAX_OBJECT {
            return Err(Error::Archive("sink object exceeds 128 MiB batch limit"));
        }
        crate::fs_security::create_dir(&self.staging)?;
        if !LocalDestination::new(self.staging.clone())?.has_capacity(1024 * 1024, object.bytes)? {
            return Err(Error::Archive(
                "insufficient free space for remote read-back",
            ));
        }
        let mut file = tempfile::tempfile_in(&self.staging).map_err(network)?;
        self.request(move |config| {
            runtime()?.block_on(async {
                let response = config
                    .build()?
                    .get(&config.key(&object.key)?)
                    .await
                    .map_err(remote)?;
                if response.meta.size != object.bytes {
                    return Err(Error::Archive("remote object size mismatch"));
                }
                let mut stream = response.into_stream();
                let mut hash = Sha256::new();
                let mut count = 0u64;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(remote)?;
                    count = count.checked_add(chunk.len() as u64).ok_or_else(bad)?;
                    if count > object.bytes {
                        return Err(Error::Archive("remote object exceeds expected size"));
                    }
                    hash.update(&chunk);
                    file.write_all(&chunk).map_err(network)?;
                }
                if count != object.bytes || format!("{:x}", hash.finalize()) != object.sha256 {
                    return Err(Error::Archive("remote object checksum mismatch"));
                }
                file.seek(SeekFrom::Start(0)).map_err(network)?;
                Ok(file)
            })
        })
    }
    fn verify(&self, object: &StoredObject) -> Result<(), Error> {
        self.read_verified(object).map(drop)
    }
    fn test_access(&self) -> Result<AccessCheck, Error> {
        let checked = LocalDestination::new(self.staging.clone())?.test_access()?;
        let key = format!(".coldctl-probe-{}", uuid::Uuid::new_v4());
        let mut source = tempfile::NamedTempFile::new_in(&self.staging).map_err(network)?;
        source
            .write_all(b"coldctl storage probe")
            .map_err(network)?;
        let result = (|| {
            let original = self.put_file(&key, source.path())?;
            // Qualify create-only semantics against this endpoint, using only a
            // disposable probe. Never accept a provider that ignores If-None-Match.
            self.request(|config| {
                runtime()?.block_on(async {
                    match config
                        .build()?
                        .put_opts(
                            &config.key(&key)?,
                            bytes::Bytes::from_static(b"must not replace probe").into(),
                            PutMode::Create.into(),
                        )
                        .await
                    {
                        Err(
                            object_store::Error::AlreadyExists { .. }
                            | object_store::Error::Precondition { .. },
                        ) => Ok(()),
                        Err(error) => Err(remote(error)),
                        Ok(_) => Err(Error::DestinationConfiguration(
                            "storage endpoint does not enforce create-only writes",
                        )),
                    }
                })
            })?;
            self.verify(&original)
        })();
        // Only this unique probe may be deleted; archive deletion is not a sink capability.
        let cleanup = self.request(|config| {
            runtime()?.block_on(async {
                config
                    .build()?
                    .delete(&config.key(&key)?)
                    .await
                    .map_err(remote)
            })
        });
        result?;
        cleanup?;
        Ok(checked)
    }
}
