use coldctl_core::{
    error::Error,
    paths::StatePaths,
    source::{SourceConnection, config::TlsMode},
    state::{self, sources},
};
use rusqlite::Connection;

fn setup() -> (tempfile::TempDir, StatePaths) {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(temp.path())).unwrap();
    state::initialize(&paths, "test").unwrap();
    (temp, paths)
}

fn connection() -> SourceConnection {
    SourceConnection::from_url(
        "postgresql://archive_user@localhost:55432/app?sslmode=disable",
        Some("COLDCTL_TEST_PASSWORD".into()),
    )
    .unwrap()
}

#[test]
fn source_lifecycle_is_local_and_duplicate_safe() {
    let (_temp, paths) = setup();
    let added = sources::add(&paths, "production", connection()).unwrap();
    assert!(uuid::Uuid::parse_str(&added.id).is_ok());
    assert_eq!(sources::list(&paths).unwrap().len(), 1);
    assert_eq!(
        sources::show(&paths, "production").unwrap().connection,
        connection()
    );
    assert!(matches!(
        sources::add(&paths, "production", connection()),
        Err(Error::SourceExists)
    ));
    assert_eq!(sources::show(&paths, "production").unwrap().id, added.id);
    sources::remove(&paths, "production").unwrap();
    assert!(sources::list(&paths).unwrap().is_empty());
    assert!(matches!(
        sources::remove(&paths, "production"),
        Err(Error::SourceNotFound)
    ));
    assert!(matches!(
        sources::show(&paths, "production"),
        Err(Error::SourceNotFound)
    ));
}

#[test]
fn missing_installation_is_not_created() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("missing"))).unwrap();
    assert!(matches!(
        sources::add(&paths, "test", connection()),
        Err(Error::NotInitialized)
    ));
    assert!(matches!(sources::list(&paths), Err(Error::NotInitialized)));
    assert!(!paths.data_dir.exists());
}

#[test]
fn configuration_rejects_secrets_unsupported_options_and_invalid_names() {
    for url in [
        "postgresql://user:supersecret@localhost/db",
        "postgresql://user@localhost/db?password=supersecret",
        "postgresql://user@localhost/db?options=supersecret",
        "postgresql://user@localhost/db?sslmode=prefer",
        "postgresql://user@localhost/db?sslmode=require&sslmode=disable",
        "postgresql://user@localhost/db#supersecret",
        "https://user@localhost/db",
        "postgresql://localhost/db",
        "postgresql://user@localhost/",
        "postgresql://user@localhost:0/db",
        "supersecret",
    ] {
        let error = SourceConnection::from_url(url, None).unwrap_err();
        assert!(!error.to_string().contains("supersecret"));
        assert!(!format!("{error:?}").contains("supersecret"));
    }
    let (_temp, paths) = setup();
    for name in ["", "bad name", "bad\nname", "x'; DROP TABLE sources;--"] {
        assert!(sources::add(&paths, name, connection()).is_err());
    }
    for variable in ["", "9BAD", "BAD=secret", "BAD\n", "BAD-NAME"] {
        assert!(SourceConnection::from_url_env(variable.into()).is_err());
        assert!(
            SourceConnection::from_url("postgres://user@localhost/db", Some(variable.into()))
                .is_err()
        );
    }
}

#[test]
fn parsing_handles_escaped_fields_ipv6_and_tls_defaults() {
    let parsed =
        SourceConnection::from_url("postgresql://archive%40user@[::1]:5433/sales%20db", None)
            .unwrap();
    assert_eq!(
        parsed,
        SourceConnection::Postgres {
            host: "::1".into(),
            port: 5433,
            database: "sales db".into(),
            user: "archive@user".into(),
            tls: TlsMode::Require,
            password_env: None
        }
    );
}

#[test]
fn environment_reference_is_not_resolved_or_stored_as_a_value() {
    let (_temp, paths) = setup();
    let reference =
        SourceConnection::from_url_env("COLDCTL_NONEXISTENT_TEST_URL_875489".into()).unwrap();
    sources::add(&paths, "reference", reference.clone()).unwrap();
    assert_eq!(
        sources::show(&paths, "reference").unwrap().connection,
        reference
    );
    let json: String = Connection::open(paths.database())
        .unwrap()
        .query_row("SELECT connection_json FROM sources", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&json).unwrap(),
        serde_json::json!({"kind": "url_env", "variable": "COLDCTL_NONEXISTENT_TEST_URL_875489"})
    );
}

#[test]
fn migration_from_v1_preserves_installation() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(temp.path())).unwrap();
    let conn = Connection::open(paths.database()).unwrap();
    conn.execute_batch("CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL); INSERT INTO schema_migrations VALUES (1, 'original');").unwrap();
    conn.execute_batch(include_str!("../migrations/0001_initial.sql"))
        .unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO installation VALUES (?1, 'original', '0.1.0')",
        [&id],
    )
    .unwrap();
    assert!(sources::list(&paths).is_err());
    let outcome = state::initialize(&paths, "0.2.0").unwrap();
    assert!(!outcome.created);
    assert_eq!(outcome.installation.id.to_string(), id);
    assert_eq!(outcome.installation.initialized_at, "original");
    assert_eq!(outcome.installation.cli_version, "0.1.0");
    assert!(sources::list(&paths).unwrap().is_empty());
}

#[test]
fn concurrent_duplicate_adds_create_one_source() {
    let (_temp, paths) = setup();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(4));
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let paths = paths.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                sources::add(&paths, "same", connection())
            })
        })
        .collect();
    let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::SourceExists)))
            .count(),
        3
    );
}

#[tokio::test]
async fn connection_failures_are_safe_and_bounded() {
    use coldctl_core::source::{DataSource, postgres::PostgresSource};
    let source = PostgresSource::new(
        SourceConnection::from_url_env("COLDCTL_NONEXISTENT_TEST_URL_875489".into()).unwrap(),
    );
    assert!(
        source
            .test_connection()
            .await
            .unwrap_err()
            .to_string()
            .contains("environment variable")
    );
    // Bind a socket without accepting PostgreSQL traffic to exercise the overall connection deadline.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!(
        "postgres://user@127.0.0.1:{}/db?sslmode=disable",
        listener.local_addr().unwrap().port()
    );
    let source = PostgresSource::new(SourceConnection::from_url(&url, None).unwrap());
    let result = tokio::time::timeout(std::time::Duration::from_secs(12), source.test_connection())
        .await
        .unwrap();
    assert!(result.unwrap_err().to_string().contains("10 seconds"));
}
