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
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .batch_execute("SET TIME ZONE 'UTC'; SET DateStyle='ISO, YMD'; SET extra_float_digits=3")
        .await
        .unwrap();
    (client, driver)
}
fn options(schema: &str, table: &str) -> Options {
    Options {
        target: "target".into(),
        schema: schema.into(),
        table: table.into(),
        confirm_separate_target: true,
        max_batches: None,
        shutdown: Default::default(),
        validate_only: false,
        progress: None,
    }
}

#[tokio::test]
#[ignore = "requires separate disposable COLDCTL_TEST_POSTGRES_URL and COLDCTL_TEST_RESTORE_URL databases"]
async fn restore_round_trip_values_atomic_retry_and_safe_rejections() {
    let (source, source_driver) = connect("COLDCTL_TEST_POSTGRES_URL").await;
    let (target, target_driver) = connect("COLDCTL_TEST_RESTORE_URL").await;
    let schema = format!("restore_test_{}", uuid::Uuid::new_v4().simple());
    source.batch_execute(&format!(r#"
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.events (id bigint PRIMARY KEY,created_at timestamptz NOT NULL,small int2,middle int4,large int8,active bool,f4 real,f8 double precision,note text,v varchar(20),fixed char(5),amount numeric,day date,ts timestamp,tz timestamptz,u uuid,j json,jb jsonb);
        INSERT INTO {schema}.events VALUES
        (1,now()-INTERVAL '500 days',-32768,-2147483648,-9223372036854775808,true,'-0','-0','PRIVATE_ROW secret '' quote \ tab','snow 雪','ab',12345678901234567890.123456789012345,'0001-01-01 BC','0001-01-01 12:34:56.123456 BC','2020-01-01 01:02:03.456789+05:30','00000000-0000-0000-0000-000000000001',' {{ "a" : 9007199254740993, "b":[null,"x"] }} ','{{"n":12345678901234567890.123456789}}'),
        (2,now()-INTERVAL '500 days',32767,2147483647,9223372036854775807,false,'Infinity','-Infinity','',NULL,' ',0.0000000000000000000000001,'infinity','infinity','-infinity',NULL,'null','null'),
        (3,now()-INTERVAL '500 days',0,0,0,NULL,'NaN','NaN',NULL,'','x','NaN','-infinity','-infinity','infinity',NULL,'{{"a":1,"a":2}}','{{"nested":{{"a":[true,false]}}}}'),
        (4,now()-INTERVAL '500 days',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL);
    "#)).await.unwrap();
    target.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.occupied(id int); INSERT INTO {schema}.occupied VALUES(99);")).await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(
        &paths,
        "source",
        SourceConnection::from_url_env("COLDCTL_TEST_POSTGRES_URL".into()).unwrap(),
    )
    .unwrap();
    sources::add(
        &paths,
        "target",
        SourceConnection::from_url_env("COLDCTL_TEST_RESTORE_URL".into()).unwrap(),
    )
    .unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archive")).unwrap();
    store::policy_create(
        &paths,
        "events",
        "source",
        "disk",
        PolicyConfig {
            schema: schema.clone(),
            table: "events".into(),
            time_column: "created_at".into(),
            older_than_days: 365,
            batch_size: 2,
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
    let mut unconfirmed = options(&schema, "restored");
    unconfirmed.confirm_separate_target = false;
    assert!(
        postgres_restore::run(&paths, &job.id, unconfirmed)
            .await
            .is_err()
    );
    let mut same_source = options(&schema, "restored");
    same_source.target = "source".into();
    assert!(
        postgres_restore::run(&paths, &job.id, same_source)
            .await
            .is_err()
    );
    assert!(
        postgres_restore::run(&paths, &job.id, options(&schema, "occupied"))
            .await
            .is_err()
    );
    assert_eq!(
        target
            .query_one(&format!("SELECT id FROM {schema}.occupied"), &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        99
    );
    let mut partial = options(&schema, "restored");
    partial.max_batches = Some(1);
    let paused = postgres_restore::run(&paths, &job.id, partial)
        .await
        .unwrap();
    assert_eq!(paused.status, "paused");
    assert_eq!(paused.rows, 2);
    assert_eq!(paused.batches, 1);
    // Fail after row 3 was inserted inside the second batch: all of it must roll back.
    target
        .batch_execute(&format!(
            "ALTER TABLE {schema}.restored ADD CONSTRAINT fail_batch CHECK(id<>4)"
        ))
        .await
        .unwrap();
    let failed = postgres_restore::run(&paths, &job.id, options(&schema, "restored"))
        .await
        .unwrap_err();
    assert!(!failed.to_string().contains("PRIVATE_ROW"));
    assert_eq!(
        target
            .query_one(&format!("SELECT count(*) FROM {schema}.restored"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    target
        .batch_execute(&format!(
            "ALTER TABLE {schema}.restored DROP CONSTRAINT fail_batch"
        ))
        .await
        .unwrap();
    let completed = postgres_restore::run(&paths, &job.id, options(&schema, "restored"))
        .await
        .unwrap();
    assert_eq!(completed.rows, 4);
    assert!(completed.values_validated);
    assert_eq!(completed.status, "completed");
    // Retrying after a possibly lost commit acknowledgement cannot duplicate rows.
    assert_eq!(
        postgres_restore::run(&paths, &job.id, options(&schema, "restored"))
            .await
            .unwrap()
            .rows,
        4
    );
    let columns = job
        .plan
        .columns
        .iter()
        .map(|c| {
            format!(
                "{}::text",
                coldctl_core::source::postgres_archive::quote_identifier(&c.name)
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let source_values=source.query(&format!("SELECT pg_catalog.array_to_json(ARRAY[{columns}])::text FROM {schema}.events ORDER BY id"),&[]).await.unwrap();
    let target_values=target.query(&format!("SELECT pg_catalog.array_to_json(ARRAY[{columns}])::text FROM {schema}.restored ORDER BY id"),&[]).await.unwrap();
    assert_eq!(source_values.len(), target_values.len());
    // Do not print customer/fixture values even if equality fails.
    assert!(
        source_values
            .iter()
            .zip(&target_values)
            .all(|(a, b)| a.get::<_, String>(0) == b.get::<_, String>(0)),
        "restored values differ"
    );
    let mut validate = options(&schema, "restored");
    validate.validate_only = true;
    assert!(
        postgres_restore::run(&paths, &job.id, validate)
            .await
            .unwrap()
            .values_validated
    );
    target
        .batch_execute(&format!(
            "UPDATE {schema}.restored SET note='PRIVATE_CHANGED_VALUE' WHERE id=1"
        ))
        .await
        .unwrap();
    let mut validate = options(&schema, "restored");
    validate.validate_only = true;
    let mismatch = postgres_restore::run(&paths, &job.id, validate)
        .await
        .unwrap_err();
    assert!(!mismatch.to_string().contains("PRIVATE_CHANGED_VALUE"));
    assert!(
        postgres_restore::run(&paths, &job.id, options(&schema, "restored"))
            .await
            .is_err()
    );
    assert_eq!(
        store::job_show(&paths, &job.id).unwrap().status,
        store::JobStatus::Completed
    );
    // Corrupt archive must fail before creating another target table.
    std::fs::write(
        job.storage_root
            .join(&job.id)
            .join("batch-00000001.parquet"),
        b"damaged",
    )
    .unwrap();
    assert!(
        postgres_restore::run(&paths, &job.id, options(&schema, "blocked"))
            .await
            .is_err()
    );
    assert!(
        target
            .query_one(
                "SELECT to_regclass($1)::text",
                &[&format!("{schema}.blocked")]
            )
            .await
            .unwrap()
            .get::<_, Option<String>>(0)
            .is_none()
    );
    target
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    source
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    source_driver.abort();
    target_driver.abort();
}
