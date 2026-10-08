//! Managed connector packages. TUF authenticates updates; approved installations run offline.
pub mod model;
mod transport;
use crate::{error::Error, fs_security, paths::StatePaths};
use coldctl_connector_protocol::model::ConnectorPin;
use futures_util::StreamExt;
use model::{Catalog, MAX_CATALOG, Package};
use rusqlite::{Connection, OptionalExtension};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};
use tough::{Repository, RepositoryLoader, TargetName};
use url::Url;

type Result<T> = std::result::Result<T, Error>;
fn fail(message: &'static str) -> Error {
    Error::Archive(message)
}
fn io(_: std::io::Error) -> Error {
    fail("connector filesystem operation failed; check permissions and free space")
}
fn sql(_: rusqlite::Error) -> Error {
    fail("connector catalog unavailable or inconsistent")
}
fn encode<T: Serialize>(v: &T) -> Result<String> {
    serde_json::to_string(v).map_err(|_| fail("cannot encode connector metadata"))
}
fn decode<T: serde::de::DeserializeOwned>(s: &str) -> Result<T> {
    serde_json::from_str(s).map_err(|_| fail("invalid connector metadata"))
}
pub fn home(paths: &StatePaths) -> PathBuf {
    paths.data_dir.join("connectors")
}
fn private_file(path: &Path) -> Result<File> {
    fs_security::check(path)?;
    let mut options = OpenOptions::new();
    options.create(true).truncate(false).read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(io)
}
struct Store {
    root: PathBuf,
    db: Connection,
    _lock: File,
}
impl Store {
    fn open(paths: &StatePaths) -> Result<Self> {
        if crate::state::status(paths)?.is_none() {
            return Err(Error::NotInitialized);
        }
        let root = home(paths);
        fs_security::create_dir(&root)?;
        let lock = private_file(&root.join("manager.lock"))?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .map_err(|_| fail("connector manager is busy; retry after the current operation"))?;
        let path = root.join("catalog.db");
        fs_security::sqlite(&path)?;
        let db = Connection::open(path).map_err(sql)?;
        db.busy_timeout(Duration::from_secs(5)).map_err(sql)?;
        db.execute_batch("PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS format(version INTEGER NOT NULL); INSERT INTO format SELECT 1 WHERE NOT EXISTS(SELECT 1 FROM format);").map_err(sql)?;
        let versions: Vec<i64> = db
            .prepare("SELECT version FROM format")
            .map_err(sql)?
            .query_map([], |r| r.get(0))
            .map_err(sql)?
            .collect::<std::result::Result<_, _>>()
            .map_err(sql)?;
        if versions != [1] {
            return Err(fail("unsupported connector catalog version"));
        }
        db.execute_batch("CREATE TABLE IF NOT EXISTS registry(singleton INTEGER PRIMARY KEY CHECK(singleton=1),base TEXT NOT NULL,root BLOB NOT NULL,bootstrap_sha256 TEXT NOT NULL,catalog TEXT); CREATE TABLE IF NOT EXISTS packages(id TEXT NOT NULL,version TEXT NOT NULL,platform TEXT NOT NULL,digest TEXT NOT NULL,manifest TEXT NOT NULL,installed INTEGER NOT NULL,PRIMARY KEY(id,version,platform)); CREATE TABLE IF NOT EXISTS defaults(id TEXT PRIMARY KEY,digest TEXT NOT NULL); CREATE TABLE IF NOT EXISTS revoked(digest TEXT PRIMARY KEY);").map_err(sql)?;
        Ok(Self {
            root,
            db,
            _lock: lock,
        })
    }
    fn revoked(&self, pin: &ConnectorPin) -> Result<bool> {
        self.db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM revoked WHERE digest=?1)",
                [&pin.sha256],
                |r| r.get(0),
            )
            .map_err(sql)
    }
    fn package(
        &self,
        id: &str,
        version: Option<&str>,
        pin: Option<&ConnectorPin>,
    ) -> Result<Package> {
        let raw: Option<String> = if let Some(pin) = pin {
            self.db.query_row("SELECT manifest FROM packages WHERE id=?1 AND version=?2 AND platform=?3 AND digest=?4 AND installed=1",(&pin.id,&pin.version,model::platform(),&pin.sha256),|r|r.get(0)).optional().map_err(sql)?
        } else if let Some(version) = version {
            self.db.query_row("SELECT manifest FROM packages WHERE id=?1 AND version=?2 AND platform=?3 AND installed=1",(id,version,model::platform()),|r|r.get(0)).optional().map_err(sql)?
        } else {
            self.db.query_row("SELECT p.manifest FROM packages p JOIN defaults d ON p.id=d.id AND p.digest=d.digest WHERE p.id=?1 AND p.platform=?2 AND p.installed=1",(id,model::platform()),|r|r.get(0)).optional().map_err(sql)?
        };
        let p:Package=decode(&raw.ok_or(fail("connector not installed; use `coldctl connector install postgres --version <version>` with a trusted registry or offline bundle"))?)?;
        p.validate()?;
        Ok(p)
    }
    fn directory(&self, p: &Package) -> PathBuf {
        self.root
            .join("packages")
            .join(&p.pin().id)
            .join(&p.pin().version)
            .join(&p.platform)
            .join(&p.pin().sha256)
    }
    fn lease(&self, p: &Package, exclusive: bool) -> Result<File> {
        let directory = self.root.join("leases");
        fs_security::create_dir(&directory)?;
        let file = private_file(&directory.join(format!("{}.lock", p.pin().sha256)))?;
        let result = if exclusive {
            fs2::FileExt::try_lock_exclusive(&file)
        } else {
            fs2::FileExt::try_lock_shared(&file)
        };
        result.map_err(|_| fail("connector is in use by a running session"))?;
        Ok(file)
    }
}
fn registry_base(input: &str) -> Result<Url> {
    let url = if input.starts_with("https://") {
        Url::parse(input).map_err(|_| fail("invalid registry URL"))?
    } else {
        let path = Path::new(input);
        fs_security::check(path)?;
        let absolute = std::fs::canonicalize(path).map_err(io)?;
        Url::from_directory_path(absolute).map_err(|_| fail("invalid registry directory"))?
    };
    if !["https", "file"].contains(&url.scheme())
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.path().ends_with('/')
    {
        return Err(fail(
            "registry requires an absolute directory or an HTTPS base URL ending with /, without credentials or query parameters",
        ));
    }
    Ok(url)
}
fn read_limited(path: &Path, max: u64) -> Result<Vec<u8>> {
    fs_security::check(path)?;
    let f = File::open(path).map_err(io)?;
    if !f.metadata().map_err(io)?.is_file() {
        return Err(fail("expected regular connector file"));
    }
    let mut bytes = Vec::new();
    f.take(max + 1).read_to_end(&mut bytes).map_err(io)?;
    if bytes.len() as u64 > max {
        return Err(fail("connector file exceeds size limit"));
    }
    Ok(bytes)
}
/// Trust bootstrap is explicit and immutable. Rotation thereafter must pass TUF's old/new thresholds.
pub fn configure_registry(
    paths: &StatePaths,
    base: &str,
    root: &Path,
    expected_sha256: &str,
) -> Result<()> {
    let store = Store::open(paths)?;
    let base = registry_base(base)?;
    let bytes = read_limited(root, MAX_CATALOG)?;
    if !model::digest(expected_sha256) || format!("{:x}", Sha256::digest(&bytes)) != expected_sha256
    {
        return Err(fail("trusted root fingerprint mismatch"));
    }
    let signed: tough::schema::Signed<tough::schema::Root> =
        serde_json::from_slice(&bytes).map_err(|_| fail("invalid TUF root"))?;
    signed
        .signed
        .verify_role(&signed)
        .map_err(|_| fail("trusted root signature threshold is invalid"))?;
    let existing: Option<(String, String)> = store
        .db
        .query_row(
            "SELECT base,bootstrap_sha256 FROM registry WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(sql)?;
    if let Some(existing) = existing {
        if existing == (base.to_string(), expected_sha256.to_owned()) {
            return Ok(());
        }
        return Err(fail(
            "registry trust already configured; root rotation must be delivered through signed TUF metadata",
        ));
    }
    store
        .db
        .execute(
            "INSERT INTO registry VALUES(1,?1,?2,?3,NULL)",
            (base.as_str(), bytes, expected_sha256),
        )
        .map_err(sql)?;
    Ok(())
}
async fn target_bytes(repo: &Repository, name: &str, max: u64) -> Result<Vec<u8>> {
    let name = TargetName::new(name).map_err(|_| fail("invalid signed target name"))?;
    let entry = repo
        .targets()
        .signed
        .targets
        .get(&name)
        .ok_or(fail("required target absent from signed repository"))?;
    if entry.length > max {
        return Err(fail("signed target exceeds size limit"));
    }
    let stream = repo
        .read_target(&name)
        .await
        .map_err(|_| fail("signed target verification failed"))?
        .ok_or(fail("signed target missing"))?;
    futures_util::pin_mut!(stream);
    let mut bytes = Vec::new();
    while let Some(part) = stream.next().await {
        let part = part.map_err(|_| fail("signed target hash or length verification failed"))?;
        if bytes.len().saturating_add(part.len()) as u64 > max {
            return Err(fail("target exceeds size limit"));
        }
        bytes.extend_from_slice(&part);
    }
    Ok(bytes)
}
async fn refresh_store(store: &Store, bundle: Option<&Path>) -> Result<(Repository, Catalog)> {
    let (base, root): (String, Vec<u8>) = store
        .db
        .query_row(
            "SELECT base,root FROM registry WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(sql)?
        .ok_or(fail(
            "registry trust is not configured; use `connector registry --root --root-sha256 --url`",
        ))?;
    let base = if let Some(bundle) = bundle {
        registry_base(bundle.to_str().ok_or(fail("bundle path must be Unicode"))?)?
    } else {
        Url::parse(&base).map_err(|_| fail("invalid saved registry URL"))?
    };
    let metadata = base
        .join("metadata/")
        .map_err(|_| fail("invalid registry URL"))?;
    let targets = base
        .join("targets/")
        .map_err(|_| fail("invalid registry URL"))?;
    let datastore = store.root.join("tuf");
    fs_security::create_dir(&datastore)?;
    for entry in std::fs::read_dir(&datastore).map_err(io)? {
        fs_security::check(&entry.map_err(io)?.path())?;
    }
    let transport = transport::BoundedTransport::new(metadata.clone(), targets.clone())?;
    let limits = tough::Limits {
        max_root_updates: 32,
        max_targets_size: MAX_CATALOG,
        ..Default::default()
    };
    let repo=RepositoryLoader::new(&root,metadata,targets).limits(limits).transport(transport).datastore(&datastore).load().await.map_err(|_|fail("TUF verification failed: check signatures, metadata expiry, rollback, root rotation and registry availability"))?;
    // This registry profile uses top-level targets only, preventing delegated trust expansion.
    if repo.targets().signed.delegations.is_some() {
        return Err(fail("connector registry delegations are not supported"));
    }
    let bytes = target_bytes(&repo, "catalog.json", MAX_CATALOG).await?;
    let catalog: Catalog =
        serde_json::from_slice(&bytes).map_err(|_| fail("invalid signed connector catalog"))?;
    catalog.validate()?;
    store.db.execute_batch("BEGIN IMMEDIATE").map_err(sql)?;
    let persist = (|| {
        store
            .db
            .execute(
                "UPDATE registry SET root=?1,catalog=?2 WHERE singleton=1",
                (
                    serde_json::to_vec(repo.root())
                        .map_err(|_| fail("cannot retain trusted root"))?,
                    encode(&catalog)?,
                ),
            )
            .map_err(sql)?;
        for digest in &catalog.revoked {
            store
                .db
                .execute("INSERT OR IGNORE INTO revoked VALUES(?1)", [digest])
                .map_err(sql)?;
        }
        Ok(())
    })();
    match persist {
        Ok(()) => store.db.execute_batch("COMMIT").map_err(sql)?,
        Err(e) => {
            let _ = store.db.execute_batch("ROLLBACK");
            return Err(e);
        }
    };
    Ok((repo, catalog))
}
pub async fn refresh(paths: &StatePaths, bundle: Option<&Path>) -> Result<Catalog> {
    let store = Store::open(paths)?;
    tokio::time::timeout(Duration::from_secs(300), refresh_store(&store, bundle))
        .await
        .map_err(|_| fail("registry refresh timed out"))?
        .map(|(_, catalog)| catalog)
}
fn verify_files(store: &Store, p: &Package) -> Result<PathBuf> {
    p.compatible()?;
    if store.revoked(p.pin())? {
        return Err(fail(
            "connector package is revoked; install an approved version; pinned jobs require a reviewed migration",
        ));
    }
    let directory = store.directory(p);
    fs_security::check(&directory)?;
    if std::fs::read_dir(&directory).map_err(io)?.count() != p.files.len() {
        return Err(fail(
            "installed connector file set differs from approved package",
        ));
    }
    for entry in &p.files {
        let path = directory.join(&entry.name);
        fs_security::check(&path)?;
        let metadata = std::fs::metadata(&path).map_err(io)?;
        if !metadata.is_file()
            || metadata.len() != entry.length
            || coldctl_connector_runtime::digest(&path)? != entry.sha256
        {
            return Err(fail("installed connector checksum or length mismatch"));
        }
    }
    Ok(directory.join(&p.entrypoint))
}
async fn install_inner(
    paths: &StatePaths,
    id: &str,
    version: &str,
    bundle: Option<&Path>,
    activate: bool,
) -> Result<Package> {
    let store = Store::open(paths)?;
    let (repo, catalog) = refresh_store(&store, bundle).await?;
    let p = catalog
        .packages
        .into_iter()
        .find(|p| p.pin().id == id && p.pin().version == version && p.platform == model::platform())
        .ok_or(fail(
            "requested connector version is not available for this platform",
        ))?;
    p.compatible()?;
    if activate {
        if let Ok(current) = store.package(id, None, None) {
            if semver::Version::parse(&p.pin().version).map_err(|_| fail("invalid version"))?
                < semver::Version::parse(&current.pin().version)
                    .map_err(|_| fail("invalid version"))?
            {
                return Err(fail(
                    "installing an older default requires --no-activate followed by `connector use --allow-downgrade`",
                ));
            }
        }
    }
    if store.revoked(p.pin())? {
        return Err(fail("requested connector is revoked"));
    }
    let old: Option<String> = store
        .db
        .query_row(
            "SELECT manifest FROM packages WHERE id=?1 AND version=?2 AND platform=?3",
            (id, version, &p.platform),
            |r| r.get(0),
        )
        .optional()
        .map_err(sql)?;
    if old
        .as_ref()
        .is_some_and(|old| old != &encode(&p).unwrap_or_default())
    {
        return Err(fail(
            "connector release identity is immutable; publisher must issue a new version",
        ));
    }
    let directory = store.directory(&p);
    if !directory.exists() {
        let staging = store.root.join("staging");
        fs_security::create_dir(&staging)?;
        let temp = tempfile::Builder::new()
            .prefix("install-")
            .tempdir_in(&staging)
            .map_err(io)?;
        for entry in &p.files {
            let name =
                TargetName::new(&entry.target).map_err(|_| fail("invalid package target"))?;
            let signed = repo
                .targets()
                .signed
                .targets
                .get(&name)
                .ok_or(fail("package file absent from signed targets"))?;
            if signed.length != entry.length {
                return Err(fail("package manifest and TUF target length differ"));
            }
            let stream = repo
                .read_target(&name)
                .await
                .map_err(|_| fail("package target verification failed"))?
                .ok_or(fail("package target missing"))?;
            futures_util::pin_mut!(stream);
            let path = temp.path().join(&entry.name);
            let mut file = private_file(&path)?;
            let mut hash = Sha256::new();
            let mut length = 0u64;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| fail("package target hash verification failed"))?;
                length = length.saturating_add(chunk.len() as u64);
                if length > entry.length {
                    return Err(fail("package target exceeds signed length"));
                }
                file.write_all(&chunk).map_err(io)?;
                hash.update(&chunk);
            }
            if length != entry.length || format!("{:x}", hash.finalize()) != entry.sha256 {
                return Err(fail("package checksum mismatch"));
            }
            file.sync_all().map_err(io)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(
                    &path,
                    std::fs::Permissions::from_mode(if entry.name == p.entrypoint {
                        0o700
                    } else {
                        0o600
                    }),
                )
                .map_err(io)?;
            }
        }
        // Validate auxiliary documents before publication, without executing the package.
        for name in ["config-schema.json", "dependencies.json"] {
            let bytes = read_limited(&temp.path().join(name), MAX_CATALOG)?;
            let _: serde_json::Value = serde_json::from_slice(&bytes)
                .map_err(|_| fail("package schema or inventory is not valid JSON"))?;
        }
        let parent = directory
            .parent()
            .ok_or(fail("invalid install directory"))?;
        fs_security::create_dir(parent)?;
        std::fs::rename(temp.path(), &directory).map_err(io)?;
        #[cfg(unix)]
        File::open(parent).map_err(io)?.sync_all().map_err(io)?;
    }
    verify_files(&store, &p)?;
    store.db.execute_batch("BEGIN IMMEDIATE").map_err(sql)?;
    store.db.execute("INSERT INTO packages VALUES(?1,?2,?3,?4,?5,1) ON CONFLICT(id,version,platform) DO UPDATE SET installed=1",(id,version,&p.platform,&p.pin().sha256,encode(&p)?)).map_err(sql)?;
    if activate {
        store.db.execute("INSERT INTO defaults VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET digest=excluded.digest",(id,&p.pin().sha256)).map_err(sql)?;
    }
    store.db.execute_batch("COMMIT").map_err(sql)?;
    Ok(p)
}
pub async fn install(
    paths: &StatePaths,
    id: &str,
    version: &str,
    bundle: Option<&Path>,
    activate: bool,
) -> Result<Package> {
    tokio::time::timeout(
        Duration::from_secs(600),
        install_inner(paths, id, version, bundle, activate),
    )
    .await
    .map_err(|_| fail("connector install timed out; prior default remains available"))?
}
#[derive(Serialize)]
pub struct Installed {
    pub package: Package,
    pub active: bool,
    pub revoked: bool,
    pub healthy: bool,
    pub referenced_jobs: Vec<String>,
}
fn references(paths: &StatePaths, p: &Package) -> Result<Vec<String>> {
    Ok(crate::state::archive::job_list(paths)?
        .into_iter()
        .filter(|job| {
            !matches!(
                job.status,
                crate::state::archive::JobStatus::Completed
                    | crate::state::archive::JobStatus::Cancelled
            ) && job
                .plan
                .connector_pin
                .as_ref()
                .map_or(p.pin().id == "postgres", |pin| pin == p.pin())
        })
        .map(|job| job.id)
        .collect())
}
pub fn list(paths: &StatePaths) -> Result<Vec<Installed>> {
    let store = Store::open(paths)?;
    let raw:Vec<(String,bool)>=store.db.prepare("SELECT p.manifest,EXISTS(SELECT 1 FROM defaults d WHERE d.id=p.id AND d.digest=p.digest) FROM packages p WHERE p.installed=1 ORDER BY p.id,p.version").map_err(sql)?.query_map([],|r|Ok((r.get(0)?,r.get(1)?))).map_err(sql)?.collect::<std::result::Result<_,_>>().map_err(sql)?;
    raw.into_iter()
        .map(|(raw, active)| {
            let package: Package = decode(&raw)?;
            package.validate()?;
            Ok(Installed {
                revoked: store.revoked(package.pin())?,
                healthy: verify_files(&store, &package).is_ok(),
                referenced_jobs: references(paths, &package)?,
                package,
                active,
            })
        })
        .collect()
}
pub fn activate(paths: &StatePaths, id: &str, version: &str, allow_downgrade: bool) -> Result<()> {
    let store = Store::open(paths)?;
    let p = store.package(id, Some(version), None)?;
    verify_files(&store, &p)?;
    if let Ok(current) = store.package(id, None, None) {
        if semver::Version::parse(version).map_err(|_| fail("invalid version"))?
            < semver::Version::parse(&current.pin().version).map_err(|_| fail("invalid version"))?
            && !allow_downgrade
        {
            return Err(fail(
                "selecting an older connector requires --allow-downgrade; pinned jobs remain unchanged",
            ));
        }
    }
    store.db.execute("INSERT INTO defaults VALUES(?1,?2) ON CONFLICT(id) DO UPDATE SET digest=excluded.digest",(id,&p.pin().sha256)).map_err(sql)?;
    Ok(())
}
pub fn remove(paths: &StatePaths, id: &str, version: &str) -> Result<()> {
    let store = Store::open(paths)?;
    let p = store.package(id, Some(version), None)?;
    let _lease = store.lease(&p, true)?;
    if !references(paths, &p)?.is_empty() {
        return Err(fail(
            "connector is required by resumable jobs; complete or cancel them before removal",
        ));
    }
    // Mark unavailable first. An interrupted deletion leaves only an unselected orphan directory.
    store.db.execute_batch("BEGIN IMMEDIATE").map_err(sql)?;
    store
        .db
        .execute(
            "UPDATE packages SET installed=0 WHERE id=?1 AND version=?2 AND platform=?3",
            (id, version, &p.platform),
        )
        .map_err(sql)?;
    store
        .db
        .execute(
            "DELETE FROM defaults WHERE id=?1 AND digest=?2",
            (id, &p.pin().sha256),
        )
        .map_err(sql)?;
    store.db.execute_batch("COMMIT").map_err(sql)?;
    let directory = store.directory(&p);
    fs_security::check(&directory)?;
    // Delete only the four known regular files, never recursively follow foreign content.
    for entry in &p.files {
        let path = directory.join(&entry.name);
        fs_security::check(&path)?;
        if path.exists() {
            std::fs::remove_file(path).map_err(io)?;
        }
    }
    std::fs::remove_dir(directory).map_err(io)?;
    Ok(())
}
pub struct Selected {
    pub path: PathBuf,
    pub package: Package,
    pub lease: File,
}
pub fn select(paths: &StatePaths, id: &str, pin: Option<&ConnectorPin>) -> Result<Selected> {
    let store = Store::open(paths)?;
    let package = store.package(id, None, pin)?;
    let lease = store.lease(&package, false)?;
    let path = verify_files(&store, &package)?;
    Ok(Selected {
        path: std::fs::canonicalize(path).map_err(io)?,
        package,
        lease,
    })
}

#[cfg(test)]
mod tests;

/// Export only the selected connector's files; all metadata remains authenticated by TUF.
pub async fn bundle(paths: &StatePaths, id: &str, version: &str, out: &Path) -> Result<()> {
    let operation = async {
        let store = Store::open(paths)?;
        let (repo, catalog) = refresh_store(&store, None).await?;
        let package = catalog
            .packages
            .iter()
            .find(|p| {
                p.pin().id == id && p.pin().version == version && p.platform == model::platform()
            })
            .ok_or(fail("requested package is unavailable"))?;
        package.compatible()?;
        if store.revoked(package.pin())? {
            return Err(fail("requested package is revoked"));
        }
        fs_security::check(out)?;
        if out.exists() {
            return Err(fail("offline bundle destination must not exist"));
        }
        let absolute = std::path::absolute(out).map_err(io)?;
        let parent = absolute
            .parent()
            .ok_or(fail("invalid bundle destination"))?;
        fs_security::create_dir(parent)?;
        let temp = tempfile::Builder::new()
            .prefix("bundle-")
            .tempdir_in(parent)
            .map_err(io)?;
        let targets: Vec<String> = std::iter::once("catalog.json".into())
            .chain(package.files.iter().map(|f| f.target.clone()))
            .collect();
        repo.cache(
            temp.path().join("metadata"),
            temp.path().join("targets"),
            Some(&targets),
            true,
        )
        .await
        .map_err(|_| fail("unable to cache verified offline bundle"))?;
        let mut root = private_file(&temp.path().join("trusted-root.json"))?;
        root.write_all(encode(repo.root())?.as_bytes())
            .map_err(io)?;
        root.sync_all().map_err(io)?;
        drop(root); // Windows cannot rename the parent while this handle is open.
        std::fs::rename(temp.path(), absolute).map_err(io)?;
        Ok(())
    };
    tokio::time::timeout(Duration::from_secs(600), operation)
        .await
        .map_err(|_| fail("offline bundle export timed out"))?
}
