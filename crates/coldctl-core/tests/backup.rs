use coldctl_core::{
    paths::StatePaths,
    state::{self, backup},
};
use rusqlite::Connection;

#[test]
fn online_backup_includes_committed_wal_and_excludes_uncommitted_changes() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    let identity = state::initialize(&paths, "test").unwrap().installation;
    let writer = Connection::open(paths.database()).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE backup_fixture (id INTEGER PRIMARY KEY, value TEXT); INSERT INTO backup_fixture VALUES (1,'committed'); BEGIN IMMEDIATE; INSERT INTO backup_fixture VALUES (2,'uncommitted');").unwrap();
    assert!(paths.database().with_file_name("state.db-wal").exists());
    let output = temp.path().join("snapshot.db");
    let report = backup::create(&paths, &output).unwrap();
    assert!(!report.archives_included);
    assert!(report.bytes > 0);
    assert_eq!(report.sha256.len(), 64);
    let restored = Connection::open(&output).unwrap();
    assert_eq!(
        restored
            .query_row("SELECT count(*) FROM backup_fixture", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        restored
            .query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert_eq!(
        restored
            .query_row("SELECT id FROM installation", [], |r| r.get::<_, String>(0))
            .unwrap(),
        identity.id.to_string()
    );
    writer.execute_batch("ROLLBACK").unwrap();
    drop(writer);
    assert_eq!(
        restored
            .query_row("SELECT value FROM backup_fixture", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "committed"
    );
}

#[test]
fn backup_never_overwrites_and_refuses_state_files_or_missing_installations() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    let output = temp.path().join("snapshot.db");
    assert!(backup::create(&paths, &output).is_err());
    assert!(!output.exists());
    state::initialize(&paths, "test").unwrap();
    std::fs::write(&output, b"keep").unwrap();
    assert!(backup::create(&paths, &output).is_err());
    assert_eq!(std::fs::read(&output).unwrap(), b"keep");
    assert!(backup::create(&paths, &paths.database()).is_err());
    assert!(backup::create(&paths, &paths.data_dir.join("state.db-wal")).is_err());
    assert!(state::status(&paths).unwrap().is_some());
}
