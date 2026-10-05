//! Recovery uses only the explicitly supplied directory, never paths from a manifest.
use super::{checkpoint, manifest::Manifest, verifier};
use crate::{
    destination::StoredObject,
    error::Error,
    paths::StatePaths,
    state::{self, archive as store},
};
use serde::Serialize;
use std::{
    collections::HashSet,
    io::Read,
    path::{Path, PathBuf},
};

const MAX_MANIFEST_BYTES: u64 = 64 * 1024 * 1024;
const MAX_OBJECTS: usize = 100_000;

#[derive(Debug, Serialize)]
pub struct Inspection {
    pub job_id: String,
    pub format_version: u32,
    pub directory: PathBuf,
    pub rows: i64,
    pub objects: usize,
    pub bytes: i64,
    pub verified: bool,
    pub integrity_basis: &'static str,
}
struct Loaded {
    manifest: Manifest,
    root: PathBuf,
    object: StoredObject,
    bytes: i64,
}

/// Reject symlinks/reparse points within an archive. The caller must keep files
/// immutable while checking/importing; this is not a hostile-writer sandbox.
pub(crate) fn regular(path: &Path, directory: bool) -> Result<(), Error> {
    let metadata = path
        .symlink_metadata()
        .map_err(|_| Error::Archive("archive file or directory is missing or inaccessible"))?;
    #[cfg(windows)]
    let reparse = {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    };
    #[cfg(not(windows))]
    let reparse = metadata.file_type().is_symlink();
    if reparse
        || (if directory {
            !metadata.is_dir()
        } else {
            !metadata.is_file()
        })
    {
        return Err(Error::Archive(
            "archive paths must be regular files/directories without symlinks or reparse points",
        ));
    }
    Ok(())
}

fn load(directory: &Path) -> Result<Loaded, Error> {
    crate::fs_security::check(directory)?;
    regular(directory, true)?;
    let directory = directory
        .canonicalize()
        .map_err(|_| Error::Archive("cannot resolve archive directory"))?;
    let manifest_path = directory.join("manifest.json");
    regular(&manifest_path, false)?;
    let mut bytes = Vec::new();
    std::fs::File::open(&manifest_path)
        .map_err(|_| Error::Archive("cannot read manifest"))?
        .take(MAX_MANIFEST_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::Archive("cannot read manifest"))?;
    if bytes.len() as u64 > MAX_MANIFEST_BYTES {
        return Err(Error::Archive("manifest exceeds the 64 MiB recovery limit"));
    }
    // Read the version before decoding the version-specific schema.
    #[derive(serde::Deserialize)]
    struct Version {
        format_version: u32,
    }
    let version: Version =
        serde_json::from_slice(&bytes).map_err(|_| Error::Archive("invalid archive manifest"))?;
    if version.format_version != 2 {
        return Err(Error::Archive(
            "unsupported manifest version; standalone recovery requires format version 2",
        ));
    }
    let manifest: Manifest =
        serde_json::from_slice(&bytes).map_err(|_| Error::Archive("invalid archive manifest"))?;
    let id = uuid::Uuid::parse_str(&manifest.job_id)
        .map_err(|_| Error::Archive("invalid manifest job ID"))?;
    if id.to_string() != manifest.job_id
        || directory.file_name().and_then(|n| n.to_str()) != Some(manifest.job_id.as_str())
    {
        return Err(Error::Archive(
            "archive directory must retain its canonical UUID job ID name",
        ));
    }
    if manifest.source_deleted
        || manifest.plan.delete
        || manifest.rows < 0
        || manifest.objects.len() > MAX_OBJECTS
    {
        return Err(Error::Archive(
            "invalid archive manifest or more than 100000 objects",
        ));
    }
    manifest.plan.policy.config.validate()?;
    let columns = &manifest.plan.columns;
    let mut names = HashSet::new();
    if columns.is_empty()
        || columns.len() > 1600
        || columns.iter().any(|c| {
            !names.insert(&c.name) || !crate::source::postgres_archive::supported(&c.postgres_type)
        })
    {
        return Err(Error::Archive("invalid manifest columns"));
    }
    let key = columns
        .iter()
        .find(|c| c.name == manifest.plan.primary_key)
        .ok_or(Error::Archive("missing primary key column"))?;
    if key.nullable || !matches!(key.postgres_type.as_str(), "int2" | "int4" | "int8") {
        return Err(Error::Archive("unsupported manifest primary key"));
    }
    let mut total_rows = 0i64;
    let mut total_bytes = 0i64;
    let mut last = None;
    for (index, entry) in manifest.objects.iter().enumerate() {
        if entry.object.key != format!("{}/batch-{:08}.parquet", manifest.job_id, index + 1)
            || entry.rows <= 0
            || entry.rows > i64::from(manifest.plan.policy.config.batch_size)
            || entry.object.bytes == 0
            || entry.object.sha256.len() != 64
            || !entry
                .object
                .sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || last.is_some_and(|n| entry.last_key <= n)
            || manifest.upper_key.is_none_or(|n| entry.last_key > n)
        {
            return Err(Error::Archive("invalid manifest batch checkpoint"));
        }
        total_rows = total_rows
            .checked_add(entry.rows)
            .ok_or(Error::Archive("manifest row count overflow"))?;
        total_bytes = total_bytes
            .checked_add(
                i64::try_from(entry.object.bytes)
                    .map_err(|_| Error::Archive("manifest byte count overflow"))?,
            )
            .ok_or(Error::Archive("manifest byte count overflow"))?;
        last = Some(entry.last_key);
    }
    if total_rows != manifest.rows {
        return Err(Error::Archive("manifest row totals do not match"));
    }
    let root = directory
        .parent()
        .ok_or(Error::Archive("archive directory has no parent"))?
        .to_path_buf();
    use sha2::{Digest, Sha256};
    let object = StoredObject {
        key: format!("{}/manifest.json", manifest.job_id),
        bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(&bytes)),
    };
    Ok(Loaded {
        manifest,
        root,
        object,
        bytes: total_bytes,
    })
}
fn report(loaded: &Loaded, verified: bool) -> Inspection {
    Inspection {
        job_id: loaded.manifest.job_id.clone(),
        format_version: 2,
        directory: loaded.root.join(&loaded.manifest.job_id),
        rows: loaded.manifest.rows,
        objects: loaded.manifest.objects.len(),
        bytes: loaded.bytes,
        verified,
        integrity_basis: "supplied_manifest_not_authenticated",
    }
}

/// Metadata inspection is intentionally separate from reading/decoding all batches.
pub fn inspect(directory: &Path) -> Result<Inspection, Error> {
    Ok(report(&load(directory)?, false))
}

fn verify_loaded(
    loaded: &Loaded,
    observer: Option<super::progress::Observer>,
) -> Result<(), Error> {
    let scratch =
        tempfile::tempdir().map_err(|_| Error::Archive("cannot stage recovery verification"))?;
    let paths = StatePaths::resolve(Some(scratch.path()))?;
    state::initialize(&paths, env!("CARGO_PKG_VERSION"))?;
    insert(&paths, loaded, false)?;
    verifier::verify_with_progress(&paths, &loaded.manifest.job_id, observer)?;
    Ok(())
}
pub fn verify_directory(
    directory: &Path,
    observer: Option<super::progress::Observer>,
) -> Result<Inspection, Error> {
    let loaded = load(directory)?;
    verify_loaded(&loaded, observer)?;
    Ok(report(&loaded, true))
}
pub fn import(
    paths: &StatePaths,
    directory: &Path,
    observer: Option<super::progress::Observer>,
) -> Result<store::Job, Error> {
    // Validate target initialization before the potentially expensive verification.
    let conn = store::open(paths, false)?;
    let loaded = load(directory)?;
    let exists: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=?1)",
            [&loaded.manifest.job_id],
            |r| r.get(0),
        )
        .map_err(|_| Error::Archive("cannot inspect import target"))?;
    if exists {
        return Err(Error::Archive(
            "job ID already exists; import never replaces existing jobs",
        ));
    }
    drop(conn);
    verify_loaded(&loaded, observer)?;
    insert(paths, &loaded, true)?;
    store::job_show(paths, &loaded.manifest.job_id)
}
fn insert(paths: &StatePaths, loaded: &Loaded, verified: bool) -> Result<(), Error> {
    let invalid =
        |_| Error::Archive("cannot import archive metadata; target state has been preserved");
    let mut conn = store::open(paths, true)?;
    let tx = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .map_err(invalid)?;
    let manifest = &loaded.manifest;
    if tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM jobs WHERE id=?1)",
            [&manifest.job_id],
            |r| r.get::<_, bool>(0),
        )
        .map_err(invalid)?
    {
        return Err(Error::Archive(
            "job ID already exists; import never replaces existing jobs",
        ));
    }
    let encode = |_| Error::Archive("cannot encode recovered metadata");
    let plan = serde_json::to_string(&manifest.plan).map_err(encode)?;
    let root = loaded
        .root
        .to_str()
        .ok_or(Error::Archive("archive path must be Unicode"))?;
    tx.execute("INSERT INTO jobs (id,policy_name,plan_json,status,started_at,completed_at,rows_processed,bytes_written,objects_created,last_key,imported_root,verified_at) VALUES (?1,?2,?3,'completed',strftime('%Y-%m-%dT%H:%M:%fZ','now'),strftime('%Y-%m-%dT%H:%M:%fZ','now'),?4,?5,?6,?7,?8,CASE WHEN ?9 THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') ELSE NULL END)", rusqlite::params![manifest.job_id,manifest.plan.policy.name,plan,manifest.rows,loaded.bytes,manifest.objects.len() as i64,manifest.objects.last().map(|b| b.last_key),root,verified]).map_err(invalid)?;
    tx.execute("INSERT INTO archive_checkpoints (job_id,source_id,source_identity,upper_key,manifest_json) VALUES (?1,'imported-completed-archive','not-reconstructed',?2,?3)", rusqlite::params![manifest.job_id,manifest.upper_key,serde_json::to_string(&loaded.object).map_err(encode)?]).map_err(invalid)?;
    for (index, batch) in manifest.objects.iter().enumerate() {
        let entry = serde_json::json!({"batch":batch,"staged_key":""});
        tx.execute("INSERT INTO archive_objects (job_id,sequence,metadata_json,committed) VALUES (?1,?2,?3,1)", rusqlite::params![manifest.job_id,index as i64+1,entry.to_string()]).map_err(invalid)?;
    }
    tx.commit().map_err(invalid)
}

#[derive(Serialize)]
pub struct StagingReport {
    pub job_id: String,
    pub dry_run: bool,
    pub candidates: Vec<PathBuf>,
}
/// Never removes files. A job lock prevents racing workers using this state.
pub fn staging_candidates(paths: &StatePaths, id: &str) -> Result<StagingReport, Error> {
    let _guard = checkpoint::lock(paths, id)?;
    let job = store::job_show(paths, id)?;
    let conn = store::open(paths, false)?;
    let mut statement = conn
        .prepare("SELECT metadata_json FROM archive_objects WHERE job_id=?1")
        .map_err(|_| Error::Archive("cannot inspect staging references"))?;
    let mut referenced = HashSet::new();
    for json in statement
        .query_map([id], |r| r.get::<_, String>(0))
        .map_err(|_| Error::Archive("cannot inspect staging references"))?
    {
        let entry: checkpoint::Entry = serde_json::from_str(
            &json.map_err(|_| Error::Archive("cannot inspect staging references"))?,
        )
        .map_err(|_| Error::Archive("invalid batch checkpoint"))?;
        referenced.insert(entry.staged_key);
    }
    let directory = job.storage_root.join(id);
    regular(&directory, true)?;
    let mut candidates = Vec::new();
    for entry in std::fs::read_dir(&directory)
        .map_err(|_| Error::Archive("cannot inspect staging directory"))?
    {
        let entry = entry.map_err(|_| Error::Archive("cannot inspect staging file"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(value) = name
            .strip_prefix(".staged-")
            .and_then(|s| s.strip_suffix(".parquet"))
        else {
            continue;
        };
        if uuid::Uuid::parse_str(value)
            .map(|id| id.to_string() != value)
            .unwrap_or(true)
            || referenced.contains(&format!("{id}/{name}"))
        {
            continue;
        }
        regular(&entry.path(), false)?;
        candidates.push(entry.path());
    }
    candidates.sort();
    Ok(StagingReport {
        job_id: id.into(),
        dry_run: true,
        candidates,
    })
}
