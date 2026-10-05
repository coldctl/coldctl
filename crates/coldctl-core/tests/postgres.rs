//! Opt-in: COLDCTL_TEST_POSTGRES_URL must point to a disposable PostgreSQL database.
use coldctl_core::{
    paths::StatePaths,
    source::{DataSource, SourceConnection, postgres::PostgresSource},
    state::{self, sources},
};

#[tokio::test]
#[ignore = "requires COLDCTL_TEST_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn postgres_connection_discovery_and_local_removal() {
    let url = std::env::var("COLDCTL_TEST_POSTGRES_URL")
        .expect("set COLDCTL_TEST_POSTGRES_URL to a disposable database");
    let config: tokio_postgres::Config = url.parse().expect("valid test URL");
    let tls = postgres_native_tls::MakeTlsConnector::new(native_tls::TlsConnector::new().unwrap());
    let (client, connection) = config
        .connect(tls)
        .await
        .expect("test database is reachable");
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let schema = format!("coldctl_test_{}", uuid::Uuid::new_v4().simple());
    // All fixture objects are inside a unique schema of the explicitly supplied test database.
    client.batch_execute(&format!(r#"
        CREATE SCHEMA {schema};
        CREATE DOMAIN {schema}.archive_date AS date;
        CREATE TABLE {schema}."Order History" (
            tenant_id integer NOT NULL,
            id bigint NOT NULL,
            created_at timestamptz NOT NULL,
            booked_on {schema}.archive_date,
            note text,
            PRIMARY KEY (tenant_id, id)
        );
        CREATE INDEX archive_time ON {schema}."Order History" (created_at) INCLUDE (note);
        CREATE INDEX expression_partial ON {schema}."Order History" (lower(note)) WHERE note = 'PRIVATE_INDEX_LITERAL';
        INSERT INTO {schema}."Order History" VALUES (1, 1, now(), current_date, 'PRIVATE_CUSTOMER_ROW');
        ANALYZE {schema}."Order History";
        CREATE TABLE {schema}.empty_without_stats (id bigint, event_date date);
        CREATE TABLE {schema}.partitioned (id integer, event_date date) PARTITION BY RANGE(event_date);
        CREATE TABLE {schema}.partition_child PARTITION OF {schema}.partitioned FOR VALUES FROM ('2020-01-01') TO ('2030-01-01');
    "#)).await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(temp.path())).unwrap();
    state::initialize(&paths, "test").unwrap();
    let saved = sources::add(
        &paths,
        "integration",
        SourceConnection::from_url_env("COLDCTL_TEST_POSTGRES_URL".into()).unwrap(),
    )
    .unwrap();
    let source = PostgresSource::new(saved.connection);
    let info = source.test_connection().await.unwrap();
    assert_eq!(info.database, config.get_dbname().unwrap());
    let discovery = source.discover().await.unwrap();
    assert!(discovery.schemas.contains(&schema));
    let table = discovery
        .tables
        .iter()
        .find(|t| t.schema == schema && t.name == "Order History")
        .unwrap();
    assert_eq!(table.primary_key, ["tenant_id", "id"]);
    assert_eq!(table.estimated_rows, Some(1.0));
    assert_eq!(table.columns.len(), 5);
    assert!(
        table
            .columns
            .iter()
            .any(|c| c.name == "created_at" && !c.nullable && c.archive_time_candidate)
    );
    assert!(
        table
            .columns
            .iter()
            .any(|c| c.name == "booked_on" && c.archive_time_candidate)
    );
    let index = table
        .indexes
        .iter()
        .find(|i| i.name == "archive_time")
        .unwrap();
    assert_eq!(index.columns, ["created_at"]);
    assert_eq!(index.included_columns, ["note"]);
    assert_eq!(index.method, "btree");
    assert!(
        table
            .indexes
            .iter()
            .any(|i| i.name == "expression_partial" && i.partial && i.has_expressions)
    );
    let unknown = discovery
        .tables
        .iter()
        .find(|t| t.schema == schema && t.name == "empty_without_stats")
        .unwrap();
    assert_eq!(unknown.estimated_rows, None);
    assert!(unknown.primary_key.is_empty());
    let partitioned = discovery
        .tables
        .iter()
        .find(|t| t.schema == schema && t.name == "partitioned")
        .unwrap();
    assert!(partitioned.partitioned);
    assert_eq!(partitioned.estimated_rows, None);
    let encoded = serde_json::to_string(&discovery).unwrap();
    assert!(!encoded.contains("PRIVATE_CUSTOMER_ROW"));
    assert!(!encoded.contains("PRIVATE_INDEX_LITERAL"));
    let analysis = source.analyze().await.unwrap();
    let assessed = analysis
        .tables
        .iter()
        .find(|t| t.table.schema == schema && t.table.name == "Order History")
        .unwrap();
    assert!(assessed.statistics.total_bytes.unwrap() > 0);
    assert_eq!(
        assessed.statistics.total_bytes.unwrap(),
        assessed.statistics.table_bytes.unwrap() + assessed.statistics.index_bytes.unwrap()
    );
    assert_eq!(assessed.table.estimated_rows, Some(1.0));
    assert!(
        assessed
            .time_candidates
            .iter()
            .any(|c| c.column == "created_at" && c.range_indexes == ["archive_time"])
    );
    let parent = analysis
        .tables
        .iter()
        .find(|t| t.table.schema == schema && t.table.name == "partitioned")
        .unwrap();
    assert_eq!(parent.statistics.total_bytes, None);
    assert!(
        parent
            .findings
            .contains(&coldctl_core::analysis::Finding::PartitionedParent)
    );
    let unknown = analysis
        .tables
        .iter()
        .find(|t| t.table.schema == schema && t.table.name == "empty_without_stats")
        .unwrap();
    assert!(
        unknown
            .findings
            .contains(&coldctl_core::analysis::Finding::UnknownRowEstimate)
    );
    let encoded = serde_json::to_string(&analysis).unwrap();
    assert!(!encoded.contains("PRIVATE_CUSTOMER_ROW"));
    assert!(!encoded.contains("PRIVATE_INDEX_LITERAL"));
    sources::remove(&paths, "integration").unwrap();
    let count: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {schema}.\"Order History\""),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 1);
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    driver.abort();
}
