use coldctl_core::{paths::StatePaths, state};
use rusqlite::Connection;

fn setup() -> (tempfile::TempDir, StatePaths) {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("nested/state"))).unwrap();
    (temp, paths)
}

#[test]
fn fresh_and_repeated_init_preserve_identity_and_migration() {
    let (_temp, paths) = setup();
    assert_eq!(state::status(&paths).unwrap(), None);
    assert!(!paths.data_dir.exists());
    let first = state::initialize(&paths, "0.1.0").unwrap();
    assert!(first.created);
    assert_eq!(first.installation.id.get_version_num(), 4);
    assert!(first.installation.initialized_at.ends_with('Z'));
    let second = state::initialize(&paths, "0.2.0").unwrap();
    assert!(!second.created);
    assert_eq!(first.installation, second.installation);
    assert_eq!(state::status(&paths).unwrap(), Some(first.installation));
    let conn = Connection::open(paths.database()).unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM schema_migrations WHERE version = 1",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
}

#[test]
fn empty_database_and_missing_installation_are_repaired() {
    let (_temp, paths) = setup();
    std::fs::create_dir_all(&paths.data_dir).unwrap();
    std::fs::write(paths.database(), []).unwrap();
    assert_eq!(state::status(&paths).unwrap(), None);
    assert!(state::initialize(&paths, "test").unwrap().created);
    Connection::open(paths.database())
        .unwrap()
        .execute("DELETE FROM installation", [])
        .unwrap();
    assert!(state::initialize(&paths, "test").unwrap().created);
}

#[test]
fn corrupt_database_is_preserved() {
    let (_temp, paths) = setup();
    std::fs::create_dir_all(&paths.data_dir).unwrap();
    let contents = b"not a SQLite database";
    std::fs::write(paths.database(), contents).unwrap();
    assert!(state::initialize(&paths, "test").is_err());
    assert!(state::status(&paths).is_err());
    assert_eq!(std::fs::read(paths.database()).unwrap(), contents);
}

#[test]
fn failed_migration_rolls_back_without_destroying_existing_tables() {
    let (_temp, paths) = setup();
    std::fs::create_dir_all(&paths.data_dir).unwrap();
    let conn = Connection::open(paths.database()).unwrap();
    conn.execute_batch(
        "CREATE TABLE installation (old_field TEXT); INSERT INTO installation VALUES ('keep');",
    )
    .unwrap();
    assert!(state::initialize(&paths, "test").is_err());
    assert_eq!(
        conn.query_row("SELECT old_field FROM installation", [], |r| r
            .get::<_, String>(0))
            .unwrap(),
        "keep"
    );
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name = 'schema_migrations'",
            [],
            |r| r.get::<_, i64>(0)
        )
        .unwrap(),
        0
    );
}

#[test]
fn newer_schema_and_invalid_identity_are_rejected() {
    let (_temp, paths) = setup();
    state::initialize(&paths, "test").unwrap();
    let conn = Connection::open(paths.database()).unwrap();
    conn.execute("INSERT INTO schema_migrations VALUES (9, 'future')", [])
        .unwrap();
    assert!(state::initialize(&paths, "test").is_err());
    assert!(state::status(&paths).is_err());
    conn.execute("DELETE FROM schema_migrations WHERE version = 9", [])
        .unwrap();
    conn.execute("UPDATE installation SET id = 'invalid'", [])
        .unwrap();
    assert!(state::initialize(&paths, "test").is_err());
}

#[test]
fn concurrent_initializations_share_one_identity() {
    let (_temp, paths) = setup();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let paths = paths.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                state::initialize(&paths, "test").unwrap()
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.created).count(), 1);
    assert!(
        results
            .iter()
            .all(|r| r.installation == results[0].installation)
    );
}

#[test]
fn data_directory_that_is_a_file_returns_context() {
    let temp = tempfile::tempdir().unwrap();
    let file = temp.path().join("file");
    std::fs::write(&file, b"keep").unwrap();
    let paths = StatePaths::resolve(Some(&file)).unwrap();
    let error = state::initialize(&paths, "test").unwrap_err();
    assert!(error.to_string().contains("file"));
    assert_eq!(std::fs::read(file).unwrap(), b"keep");
}

#[test]
fn every_supported_schema_prefix_upgrades_without_changing_identity() {
    let migrations = [
        include_str!("../migrations/0001_initial.sql"),
        include_str!("../migrations/0002_sources.sql"),
        include_str!("../migrations/0003_destinations.sql"),
        include_str!("../migrations/0004_archive.sql"),
        include_str!("../migrations/0005_durability.sql"),
        include_str!("../migrations/0006_execution.sql"),
        include_str!("../migrations/0007_observability.sql"),
        include_str!("../migrations/0008_recovery.sql"),
    ];
    for version in 1..=migrations.len() {
        let (_temp, paths) = setup();
        std::fs::create_dir_all(&paths.data_dir).unwrap();
        let conn = Connection::open(paths.database()).unwrap();
        conn.execute_batch(
            "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
        )
        .unwrap();
        for (index, sql) in migrations.iter().take(version).enumerate() {
            conn.execute_batch(sql).unwrap();
            conn.execute(
                "INSERT INTO schema_migrations VALUES (?1,'original')",
                [index as i64 + 1],
            )
            .unwrap();
        }
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO installation VALUES (?1,'2026-01-01T00:00:00Z','old')",
            [&id],
        )
        .unwrap();
        drop(conn);
        let upgraded = state::initialize(&paths, "new").unwrap();
        assert!(!upgraded.created);
        assert_eq!(upgraded.installation.id.to_string(), id);
        let conn = Connection::open(paths.database()).unwrap();
        assert_eq!(
            conn.query_row("SELECT max(version) FROM schema_migrations", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            8
        );
        assert_eq!(
            conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "ok"
        );
    }
}
