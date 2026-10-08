use coldctl_core::{
    destination::{
        ArchiveDestination,
        local::{LocalDestination, resolve_path},
    },
    error::Error,
    paths::StatePaths,
    source::SourceConnection,
    state::{self, destinations, sources},
};
use std::path::Path;

fn setup() -> (tempfile::TempDir, StatePaths) {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    (temp, paths)
}

#[test]
fn lifecycle_preserves_files_and_does_not_probe_during_configuration() {
    let (temp, paths) = setup();
    let root = temp.path().join("archive files");
    let saved = destinations::add_local(&paths, "local", &root).unwrap();
    assert_eq!(saved.path, root);
    assert!(uuid::Uuid::parse_str(&saved.id).is_ok());
    assert!(!root.exists());
    assert_eq!(destinations::list(&paths).unwrap().len(), 1);
    assert_eq!(destinations::show(&paths, "local").unwrap().id, saved.id);
    assert!(!root.exists());
    assert!(matches!(
        destinations::add_local(&paths, "local", &temp.path().join("other")),
        Err(Error::DestinationExists)
    ));
    assert_eq!(destinations::show(&paths, "local").unwrap().path, root);
    let local = LocalDestination::new(root.clone()).unwrap();
    let checked = local.test_access().unwrap();
    assert!(checked.writable && checked.readable && checked.probe_removed);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    let archive = root.join("existing.parquet");
    std::fs::write(&archive, b"preserve-existing-archive").unwrap();
    local.test_access().unwrap();
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    destinations::remove(&paths, "local").unwrap();
    assert_eq!(
        std::fs::read(&archive).unwrap(),
        b"preserve-existing-archive"
    );
    assert!(destinations::list(&paths).unwrap().is_empty());
    assert!(matches!(
        destinations::show(&paths, "local"),
        Err(Error::DestinationNotFound)
    ));
    assert!(matches!(
        destinations::remove(&paths, "local"),
        Err(Error::DestinationNotFound)
    ));
}

#[test]
fn file_in_place_of_directory_fails_without_overwriting_it() {
    let (temp, paths) = setup();
    let root = temp.path().join("occupied");
    std::fs::write(&root, b"keep").unwrap();
    let saved = destinations::add_local(&paths, "blocked", &root).unwrap();
    let error = LocalDestination::new(saved.path)
        .unwrap()
        .test_access()
        .unwrap_err();
    assert!(error.to_string().contains("create directory"));
    assert!(error.to_string().contains("occupied"));
    assert_eq!(std::fs::read(&root).unwrap(), b"keep");
    destinations::remove(&paths, "blocked").unwrap();
    assert_eq!(std::fs::read(&root).unwrap(), b"keep");
}

#[test]
fn configuration_requires_initialization_and_valid_names_and_paths() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("missing"))).unwrap();
    assert!(matches!(
        destinations::add_local(&paths, "local", temp.path()),
        Err(Error::NotInitialized)
    ));
    assert!(!paths.data_dir.exists());
    state::initialize(&paths, "test").unwrap();
    for name in [
        "",
        "bad name",
        "bad\nname",
        "x'; DROP TABLE destinations;--",
    ] {
        assert!(destinations::add_local(&paths, name, temp.path()).is_err());
    }
    for path in ["", "bad\npath", "bad\0path"] {
        assert!(destinations::add_local(&paths, "local", Path::new(path)).is_err());
    }
    assert!(LocalDestination::new("relative".into()).is_err());
}

#[test]
fn relative_path_is_resolved_at_configuration_time() {
    let (_temp, paths) = setup();
    let path = Path::new("coldctl-test-archive-relative");
    let saved = destinations::add_local(&paths, "relative", path).unwrap();
    assert_eq!(saved.path, std::env::current_dir().unwrap().join(path));
    assert_eq!(resolve_path(path).unwrap(), saved.path);
}

#[test]
fn migration_from_v2_preserves_sources_and_installation() {
    let (temp, paths) = setup();
    let installation = state::status(&paths).unwrap().unwrap();
    let source = sources::add(
        &paths,
        "original",
        SourceConnection::from_url_env("COLDCTL_UNUSED_URL".into()).unwrap(),
    )
    .unwrap();
    let conn = rusqlite::Connection::open(paths.database()).unwrap();
    conn.execute_batch("DROP TABLE archive_objects; DROP TABLE archive_checkpoints; DROP TABLE jobs; DROP TABLE policies; DROP TABLE destinations; DELETE FROM schema_migrations WHERE version >= 3;")
        .unwrap();
    let error = destinations::list(&paths).unwrap_err();
    assert!(error.to_string().contains("coldctl init"));
    assert!(destinations::add_local(&paths, "local", &temp.path().join("archive")).is_err());
    let upgraded = state::initialize(&paths, "upgraded").unwrap();
    assert!(!upgraded.created);
    assert_eq!(upgraded.installation, installation);
    assert_eq!(sources::show(&paths, "original").unwrap().id, source.id);
    assert!(destinations::list(&paths).unwrap().is_empty());
    state::initialize(&paths, "upgraded").unwrap();
    assert_eq!(
        conn.query_row("SELECT count(*) FROM schema_migrations", [], |row| row
            .get::<_, i64>(0))
            .unwrap(),
        11
    );
}

#[test]
fn concurrent_probes_are_unique_and_cleaned_up() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("archive");
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let root = root.clone();
            std::thread::spawn(move || LocalDestination::new(root).unwrap().test_access())
        })
        .collect();
    for handle in handles {
        assert!(handle.join().unwrap().unwrap().probe_removed);
    }
    assert_eq!(std::fs::read_dir(root).unwrap().count(), 0);
}
