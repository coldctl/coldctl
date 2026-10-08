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
    let result = tokio::time::timeout(std::time::Duration::from_secs(20), source.test_connection())
        .await
        .unwrap();
    assert!(result.unwrap_err().to_string().contains("timed out"));
}

#[tokio::test]
async fn resume_rejects_a_changed_connector_before_connecting() {
    let connection =
        SourceConnection::from_url("postgres://user@localhost/db?sslmode=disable", None).unwrap();
    let pin = coldctl_connector_protocol::model::ConnectorPin {
        id: "postgres".into(),
        version: "old".into(),
        sha256: "0".repeat(64),
    };
    let error = coldctl_core::source::connector::launch_pinned(&connection, Some(&pin))
        .await
        .err()
        .expect("changed executable must fail");
    assert!(error.to_string().contains("digest changed"));
}

#[test]
fn mysql_configuration_keeps_engine_and_secret_references_without_resolving_them() {
    let (_temp, paths) = setup();
    let config = SourceConnection::from_url(
        "mysql://reader@localhost/data",
        Some("ABSENT_MYSQL_PASSWORD".into()),
    )
    .unwrap()
    .with_ca_env(Some("ABSENT_MYSQL_CA".into()))
    .unwrap();
    assert_eq!(config.engine(), "mysql");
    let saved = sources::add(&paths, "mysql", config.clone()).unwrap();
    assert_eq!(saved.source_type, "mysql");
    assert_eq!(sources::show(&paths, "mysql").unwrap().connection, config);
    if let SourceConnection::Mysql { port, tls, .. } = config {
        assert_eq!(port, 3306);
        assert_eq!(tls, TlsMode::Require);
    } else {
        panic!("wrong engine")
    }
    let reference = SourceConnection::from_mysql_url_env("ABSENT_MYSQL_URL".into()).unwrap();
    assert_eq!(reference.engine(), "mysql");
    assert!(reference.resolve().is_err());
    for value in [
        "mysql://u:secret@host/db",
        "mysql://u@host/db?sslmode=prefer",
        "mysql://u@host/db?local_infile=true",
    ] {
        assert!(SourceConnection::from_url(value, None).is_err());
    }
}

#[test]
fn mongodb_configuration_keeps_engine_and_secret_references_without_resolving_them() {
    let (_temp, paths) = setup();
    let config = SourceConnection::from_url(
        "mongodb://reader@localhost/data",
        Some("ABSENT_MONGODB_PASSWORD".into()),
    )
    .unwrap()
    .with_ca_env(Some("ABSENT_MONGODB_CA".into()))
    .unwrap();
    assert_eq!(config.engine(), "mongodb");
    let saved = sources::add(&paths, "mongodb", config.clone()).unwrap();
    assert_eq!(saved.source_type, "mongodb");
    assert_eq!(sources::show(&paths, "mongodb").unwrap().connection, config);
    if let SourceConnection::Mongodb { port, tls, .. } = config {
        assert_eq!(port, 27017);
        assert_eq!(tls, TlsMode::Require);
    } else {
        panic!("wrong engine")
    }
    let reference = SourceConnection::from_mongodb_url_env("ABSENT_MONGODB_URL".into()).unwrap();
    assert_eq!(reference.engine(), "mongodb");
    assert!(reference.resolve().is_err());
    for value in [
        "mongodb://u:secret@host/db",
        "mongodb://u@host/db?sslmode=prefer",
        "mongodb://u@host/db?authSource=admin",
    ] {
        assert!(SourceConnection::from_url(value, None).is_err());
    }
}

#[test]
fn mysql_migration_preserves_existing_source_policy_and_foreign_keys() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(temp.path())).unwrap();
    let conn = Connection::open(paths.database()).unwrap();
    conn.execute_batch(
        "CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)",
    )
    .unwrap();
    for (index, sql) in [
        include_str!("../migrations/0001_initial.sql"),
        include_str!("../migrations/0002_sources.sql"),
        include_str!("../migrations/0003_destinations.sql"),
        include_str!("../migrations/0004_archive.sql"),
        include_str!("../migrations/0005_durability.sql"),
        include_str!("../migrations/0006_execution.sql"),
        include_str!("../migrations/0007_observability.sql"),
        include_str!("../migrations/0008_recovery.sql"),
    ]
    .iter()
    .enumerate()
    {
        conn.execute_batch(sql).unwrap();
        conn.execute(
            "INSERT INTO schema_migrations VALUES(?1,'original')",
            [index as i64 + 1],
        )
        .unwrap();
    }
    let id = uuid::Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO installation VALUES(?1,'original','0.1.1')",
        [&id],
    )
    .unwrap();
    drop(conn);
    let old = sources::add(&paths, "old", connection()).unwrap();
    coldctl_core::state::destinations::add_local(&paths, "disk", &temp.path().join("archives"))
        .unwrap();
    let policy = coldctl_core::state::archive::policy_create(
        &paths,
        "p",
        "old",
        "disk",
        coldctl_core::policy::model::PolicyConfig {
            schema: "public".into(),
            table: "events".into(),
            time_column: "created_at".into(),
            older_than_days: 30,
            batch_size: 2,
            equals_column: None,
            equals_value: None,
        },
    )
    .unwrap();
    state::initialize(&paths, "test").unwrap();
    assert_eq!(sources::show(&paths, "old").unwrap().id, old.id);
    assert_eq!(
        coldctl_core::state::archive::policy_show(&paths, "p")
            .unwrap()
            .id,
        policy.id
    );
    sources::add(
        &paths,
        "mysql",
        SourceConnection::from_url("mysql://u@localhost/db", None).unwrap(),
    )
    .unwrap();
    sources::add(
        &paths,
        "mongodb",
        SourceConnection::from_url("mongodb://u@localhost/db", None).unwrap(),
    )
    .unwrap();
    let conn = Connection::open(paths.database()).unwrap();
    assert!(
        conn.prepare("PRAGMA foreign_key_check")
            .unwrap()
            .query([])
            .unwrap()
            .next()
            .unwrap()
            .is_none()
    );
    assert!(sources::remove(&paths, "old").is_err());
}
