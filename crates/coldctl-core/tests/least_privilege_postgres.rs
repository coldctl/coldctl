use coldctl_core::{
    archive::{executor, planner::SourceStability},
    paths::StatePaths,
    policy::model::PolicyConfig,
    source::{
        SourceConnection,
        postgres_restore::{self, Options},
    },
    state::{self, archive as store, destinations, sources},
};

async fn connect(variable: &str) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    let config: tokio_postgres::Config = std::env::var(variable).unwrap().parse().unwrap();
    let (client, connection) = config
        .connect(postgres_native_tls::MakeTlsConnector::new(
            native_tls::TlsConnector::new().unwrap(),
        ))
        .await
        .unwrap();
    (
        client,
        tokio::spawn(async move {
            let _ = connection.await;
        }),
    )
}
fn role_connection(variable: &str, role: &str) -> SourceConnection {
    let mut url = url::Url::parse(&std::env::var(variable).unwrap()).unwrap();
    url.set_username(role).unwrap();
    url.set_password(None).unwrap();
    SourceConnection::from_url(url.as_str(), Some("COLDCTL_TEST_ROLE_PASSWORD".into())).unwrap()
}

#[tokio::test]
#[ignore = "requires two disposable databases with administrator URLs and COLDCTL_TEST_ROLE_PASSWORD=coldctl_test_role_password"]
async fn dedicated_roles_export_restore_and_cannot_write_source() {
    assert_eq!(
        std::env::var("COLDCTL_TEST_ROLE_PASSWORD").unwrap(),
        "coldctl_test_role_password"
    );
    let (source, sd) = connect("COLDCTL_TEST_POSTGRES_URL").await;
    let (target, td) = connect("COLDCTL_TEST_RESTORE_URL").await;
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let reader = format!("reader_{suffix}");
    let writer = format!("writer_{suffix}");
    let schema = format!("least_{suffix}");
    source.batch_execute(&format!("CREATE ROLE {reader} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'coldctl_test_role_password'; CREATE SCHEMA {schema}; REVOKE ALL ON SCHEMA {schema} FROM PUBLIC; CREATE TABLE {schema}.events(id bigint PRIMARY KEY, created_at timestamptz NOT NULL, note text); INSERT INTO {schema}.events VALUES(1, now()-INTERVAL '500 days','synthetic'); GRANT USAGE ON SCHEMA {schema} TO {reader}; GRANT SELECT ON {schema}.events TO {reader}; ALTER ROLE {reader} SET default_transaction_read_only=on;")).await.unwrap();
    target.batch_execute(&format!("CREATE ROLE {writer} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'coldctl_test_role_password'; CREATE SCHEMA {schema}; REVOKE ALL ON SCHEMA {schema} FROM PUBLIC; GRANT USAGE,CREATE ON SCHEMA {schema} TO {writer};")).await.unwrap();
    // Authenticate as the restricted role; do not rely on admin SET ROLE membership.
    let mut reader_config: tokio_postgres::Config = std::env::var("COLDCTL_TEST_POSTGRES_URL")
        .unwrap()
        .parse()
        .unwrap();
    reader_config
        .user(&reader)
        .password("coldctl_test_role_password");
    let (restricted, connection) = reader_config
        .connect(postgres_native_tls::MakeTlsConnector::new(
            native_tls::TlsConnector::new().unwrap(),
        ))
        .await
        .unwrap();
    let rd = tokio::spawn(async move {
        let _ = connection.await;
    });
    restricted
        .batch_execute("SET default_transaction_read_only=off")
        .await
        .unwrap();
    let denied = restricted
        .execute(&format!("DELETE FROM {schema}.events"), &[])
        .await
        .unwrap_err();
    assert_eq!(denied.code().unwrap().code(), "42501");
    assert!(
        restricted
            .batch_execute(&format!("CREATE TABLE {schema}.forbidden(id int)"))
            .await
            .is_err()
    );
    drop(restricted);
    rd.abort();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(
        &paths,
        "reader",
        role_connection("COLDCTL_TEST_POSTGRES_URL", &reader),
    )
    .unwrap();
    sources::add(
        &paths,
        "writer",
        role_connection("COLDCTL_TEST_RESTORE_URL", &writer),
    )
    .unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archive")).unwrap();
    store::policy_create(
        &paths,
        "events",
        "reader",
        "disk",
        PolicyConfig {
            schema: schema.clone(),
            table: "events".into(),
            time_column: "created_at".into(),
            older_than_days: 365,
            batch_size: 1,
            equals_column: None,
            equals_value: None,
        },
    )
    .unwrap();
    let job = executor::run(
        &paths,
        "events",
        executor::RunOptions {
            source_stability: SourceStability::QuiescentCopy,
            allow_repeat: false,
            minimum_free_bytes: 1,
            execution: Default::default(),
            shutdown: Default::default(),
            progress: None,
        },
    )
    .await
    .unwrap();
    for validate_only in [false, true] {
        let report = postgres_restore::run(
            &paths,
            &job.id,
            Options {
                target: "writer".into(),
                schema: schema.clone(),
                table: "restored".into(),
                confirm_separate_target: true,
                max_batches: None,
                shutdown: Default::default(),
                validate_only,
                progress: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(report.rows, 1);
        assert!(report.values_validated);
    }
    source
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE; DROP ROLE {reader}"))
        .await
        .unwrap();
    target
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE; DROP ROLE {writer}"))
        .await
        .unwrap();
    sd.abort();
    td.abort();
}
