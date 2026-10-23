use coldctl_core::{
    archive::{
        controls::{ExecutionControls, Shutdown},
        deletion, executor,
        planner::SourceStability,
        recovery,
    },
    paths::StatePaths,
    policy::model::PolicyConfig,
    scheduler::Worker,
    source::{
        SourceConnection,
        connector::{self, DeleteBatchResult, DeleteSpec, Request},
    },
    state::{self, archive as jobs, destinations, scheduler as queue, sources},
};

fn options(max_batches: Option<u64>) -> deletion::Options {
    deletion::Options {
        confirm_delete: true,
        confirm_original_source: true,
        max_batches,
        shutdown: Shutdown::default(),
    }
}
fn archive_options() -> executor::RunOptions {
    executor::RunOptions {
        source_stability: SourceStability::ImmutableRows,
        allow_repeat: false,
        minimum_free_bytes: 1,
        execution: ExecutionControls::default(),
        shutdown: Shutdown::default(),
        progress: None,
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL and COLDCTL_TEST_ROLE_PASSWORD=coldctl_test_role_password"]
async fn deletion_requires_delete_privilege_but_not_update_or_create() {
    assert_eq!(
        std::env::var("COLDCTL_TEST_ROLE_PASSWORD").unwrap(),
        "coldctl_test_role_password"
    );
    let url = std::env::var("COLDCTL_TEST_POSTGRES_URL").unwrap();
    let config: tokio_postgres::Config = url.parse().unwrap();
    let (client, connection) = config
        .connect(postgres_native_tls::MakeTlsConnector::new(
            native_tls::TlsConnector::new().unwrap(),
        ))
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let schema = format!("deletion_role_{suffix}");
    let role = format!("delete_reader_{suffix}");
    client.batch_execute(&format!("CREATE ROLE {role} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'coldctl_test_role_password'; CREATE SCHEMA {schema}; REVOKE ALL ON SCHEMA {schema} FROM PUBLIC; CREATE TABLE {schema}.events(id bigint PRIMARY KEY,created_at timestamptz,note text); INSERT INTO {schema}.events VALUES(1,now()-INTERVAL '500 days','synthetic'); GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT ON {schema}.events TO {role}; ALTER ROLE {role} SET default_transaction_read_only=on;")).await.unwrap();
    let mut role_url = url::Url::parse(&url).unwrap();
    role_url.set_username(&role).unwrap();
    role_url.set_password(None).unwrap();
    let source =
        SourceConnection::from_url(role_url.as_str(), Some("COLDCTL_TEST_ROLE_PASSWORD".into()))
            .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(&paths, "source", source).unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
    jobs::policy_create(
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
    let job = executor::run(&paths, "events", archive_options())
        .await
        .unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    assert_eq!(
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.events"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    client
        .batch_execute(&format!("GRANT DELETE ON {schema}.events TO {role}"))
        .await
        .unwrap();
    let done = deletion::run(&paths, &job.id, options(None)).await.unwrap();
    assert!(done.completed);
    assert_eq!(done.rows_deleted, 1);
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE; DROP ROLE {role};"))
        .await
        .unwrap();
    driver.abort();
}

#[tokio::test]
#[ignore = "requires a disposable COLDCTL_TEST_POSTGRES_URL and the updated PostgreSQL connector"]
async fn verified_deletion_is_atomic_resumable_and_scheduler_opt_in() {
    let url = std::env::var("COLDCTL_TEST_POSTGRES_URL").unwrap();
    let config: tokio_postgres::Config = url.parse().unwrap();
    let (client, connection) = config
        .connect(postgres_native_tls::MakeTlsConnector::new(
            native_tls::TlsConnector::new().unwrap(),
        ))
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let schema = format!("deletion_test_{}", uuid::Uuid::new_v4().simple());
    client.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.events(id bigint PRIMARY KEY,created_at timestamptz,note text,active bool,amount numeric(30,9),ratio float8); INSERT INTO {schema}.events SELECT n,now()-INTERVAL '500 days','original',true,12345678901234567890.123456789,'-0'::float8 FROM generate_series(1,5)n; INSERT INTO {schema}.events VALUES(6,now(),'recent',NULL,NULL,NULL);")).await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    let source = SourceConnection::from_url_env("COLDCTL_TEST_POSTGRES_URL".into()).unwrap();
    sources::add(&paths, "source", source.clone()).unwrap();
    let root = temp.path().join("archives");
    destinations::add_local(&paths, "disk", &root).unwrap();
    let policy = PolicyConfig {
        schema: schema.clone(),
        table: "events".into(),
        time_column: "created_at".into(),
        older_than_days: 365,
        batch_size: 2,
        equals_column: None,
        equals_value: None,
    };
    jobs::policy_create(&paths, "events", "source", "disk", policy.clone()).unwrap();
    let job = executor::run(&paths, "events", archive_options())
        .await
        .unwrap();
    assert_eq!(job.rows_processed, 5);
    let count = || async {
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.events"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0)
    };

    let mut reader = connector::launch_in_state(&source, &paths, None)
        .await
        .unwrap();
    let identity: String = reader.call(Request::OpenScan, 30_000, false).await.unwrap();
    let replay_batch: Option<coldctl_core::archive::batch::DataBatch> = reader
        .call(
            Request::ReadBatch {
                plan: job.plan.clone(),
                last: Some(2.into()),
                upper: 5.into(),
                rows: 2,
                bytes: 32 * 1024 * 1024,
            },
            30_000,
            false,
        )
        .await
        .unwrap();
    let replay_rows = replay_batch.unwrap().rows;
    drop(reader);
    let mut refused = options(None);
    refused.confirm_delete = false;
    assert!(deletion::run(&paths, &job.id, refused).await.is_err());
    let stopped = options(None);
    stopped.shutdown.request();
    assert_eq!(
        deletion::run(&paths, &job.id, stopped)
            .await
            .unwrap()
            .batches,
        0
    );
    assert_eq!(count().await, 6);

    // Verification must fail before any source write.
    let batch = root.join(&job.id).join("batch-00000001.parquet");
    let original = std::fs::read(&batch).unwrap();
    std::fs::write(&batch, b"damaged").unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    assert_eq!(count().await, 6);
    std::fs::write(&batch, original).unwrap();

    // A mismatch after the first row is deleted must roll back the whole batch.
    client
        .batch_execute(&format!(
            "UPDATE {schema}.events SET note='changed' WHERE id=2"
        ))
        .await
        .unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    assert_eq!(count().await, 6);
    client.batch_execute(&format!("UPDATE {schema}.events SET note='original' WHERE id=2; CREATE TABLE {schema}.child(id bigint REFERENCES {schema}.events(id) ON DELETE CASCADE)")).await.unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    assert_eq!(count().await, 6);
    client.batch_execute(&format!("DROP TABLE {schema}.child; CREATE FUNCTION {schema}.noop() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN OLD; END'; CREATE TRIGGER unsafe BEFORE DELETE ON {schema}.events FOR EACH ROW EXECUTE FUNCTION {schema}.noop()" )).await.unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    assert_eq!(count().await, 6);
    client
        .batch_execute(&format!("DROP TRIGGER unsafe ON {schema}.events"))
        .await
        .unwrap();

    let imported_paths = StatePaths::resolve(Some(&temp.path().join("imported"))).unwrap();
    state::initialize(&imported_paths, "test").unwrap();
    let imported = recovery::import(&imported_paths, &root.join(&job.id), None).unwrap();
    assert!(
        deletion::run(&imported_paths, &imported.id, options(None))
            .await
            .is_err()
    );

    let first = deletion::run(&paths, &job.id, options(Some(1)))
        .await
        .unwrap();
    assert_eq!(first.rows_deleted, 2);
    assert!(!first.completed);
    assert_eq!(count().await, 4);

    // Commit source batch 2 but deliberately omit its local checkpoint: a lost acknowledgement.
    let mut native = connector::launch_in_state(&source, &paths, None)
        .await
        .unwrap();
    native
        .call::<_, ()>(
            Request::DeleteOpen(DeleteSpec {
                require_empty_children: false,
                plan: job.plan.clone(),
                source_identity: identity,
            }),
            30_000,
            false,
        )
        .await
        .unwrap();
    let removed: DeleteBatchResult = native
        .call(Request::DeleteBatch { rows: replay_rows }, 120_000, true)
        .await
        .unwrap();
    assert_eq!(removed.deleted, 2);
    drop(native);
    let done = deletion::run(&paths, &job.id, options(None)).await.unwrap();
    assert!(done.completed);
    assert_eq!(
        (done.rows_processed, done.rows_deleted, done.rows_absent),
        (5, 3, 2)
    );
    assert_eq!(count().await, 1);
    client
        .batch_execute(&format!(
            "INSERT INTO {schema}.events VALUES(1,now(),'new',false,1,1)"
        ))
        .await
        .unwrap();
    assert!(
        deletion::run(&paths, &job.id, options(None))
            .await
            .unwrap()
            .completed
    );
    assert_eq!(count().await, 2);

    // Queue completion includes deletion only when explicitly requested.
    client.batch_execute(&format!("CREATE TABLE {schema}.scheduled(id bigint PRIMARY KEY,created_at timestamptz,note text); INSERT INTO {schema}.scheduled SELECT n,now()-INTERVAL '500 days','scheduled' FROM generate_series(1,3)n;")).await.unwrap();
    let mut scheduled_policy = policy;
    scheduled_policy.table = "scheduled".into();
    jobs::policy_create(&paths, "scheduled", "source", "disk", scheduled_policy).unwrap();
    let entry = queue::enqueue(
        &paths,
        "scheduled",
        None,
        queue::Options {
            delete_after_archive: true,
            source_stability: SourceStability::ImmutableRows,
            allow_repeat: false,
            confirm_original_source: true,
            max_retries: 0,
            execution: ExecutionControls {
                max_duration_seconds: Some(60),
                minimum_free_bytes: 1,
                ..Default::default()
            },
        },
        0,
    )
    .unwrap();
    let worker = Worker::acquire(&paths).unwrap();
    let first = worker.tick(Shutdown::default()).await.unwrap().unwrap();
    assert_eq!(first.status, "pending");
    assert_eq!(
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.scheduled"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(worker);
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let worker = Worker::acquire(&paths).unwrap();
    assert_eq!(
        worker
            .tick(Shutdown::default())
            .await
            .unwrap()
            .unwrap()
            .status,
        "completed"
    );
    assert!(
        state::deletion::show(
            &paths,
            queue::show(&paths, &entry.id)
                .unwrap()
                .job_id
                .as_ref()
                .unwrap()
        )
        .unwrap()
        .unwrap()
        .completed
    );
    assert_eq!(
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.scheduled"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    driver.abort();
}

#[tokio::test]
#[ignore = "requires a disposable COLDCTL_TEST_POSTGRES_URL and the updated PostgreSQL connector"]
async fn bulk_deletion_checks_every_row_and_preserves_survivors() {
    let url = std::env::var("COLDCTL_TEST_POSTGRES_URL").unwrap();
    let config: tokio_postgres::Config = url.parse().unwrap();
    let (client, connection) = config
        .connect(postgres_native_tls::MakeTlsConnector::new(
            native_tls::TlsConnector::new().unwrap(),
        ))
        .await
        .unwrap();
    let driver = tokio::spawn(async move {
        let _ = connection.await;
    });
    let schema = format!("bulk_delete_{}", uuid::Uuid::new_v4().simple());
    // Physical insertion order differs from key order; NULLs exercise comparisons.
    client.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.events(id bigint PRIMARY KEY,created_at timestamptz,note text); INSERT INTO {schema}.events SELECT n,now()-INTERVAL '500 days',CASE WHEN n%7=0 THEN NULL ELSE 'original' END FROM generate_series(501,1,-1)n; INSERT INTO {schema}.events VALUES(9999,now(),'survivor');")).await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(
        &paths,
        "source",
        SourceConnection::from_url_env("COLDCTL_TEST_POSTGRES_URL".into()).unwrap(),
    )
    .unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
    jobs::policy_create(
        &paths,
        "events",
        "source",
        "disk",
        PolicyConfig {
            schema: schema.clone(),
            table: "events".into(),
            time_column: "created_at".into(),
            older_than_days: 365,
            batch_size: 500,
            equals_column: None,
            equals_value: None,
        },
    )
    .unwrap();
    let job = executor::run(&paths, "events", archive_options())
        .await
        .unwrap();
    assert_eq!(job.rows_processed, 501);
    // Last key in a full batch differs: no earlier matching row may be deleted.
    client
        .batch_execute(&format!(
            "UPDATE {schema}.events SET note='changed' WHERE id=500"
        ))
        .await
        .unwrap();
    assert!(
        deletion::run(&paths, &job.id, options(Some(1)))
            .await
            .is_err()
    );
    assert_eq!(
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.events"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        502
    );
    client.batch_execute(&format!("UPDATE {schema}.events SET note='original' WHERE id=500; DELETE FROM {schema}.events WHERE id=250")).await.unwrap();
    let first = deletion::run(&paths, &job.id, options(Some(1)))
        .await
        .unwrap();
    assert_eq!(
        (first.rows_processed, first.rows_deleted, first.rows_absent),
        (500, 499, 1)
    );
    assert!(!first.completed);
    let done = deletion::run(&paths, &job.id, options(None)).await.unwrap();
    assert!(done.completed);
    assert_eq!(
        (done.rows_processed, done.rows_deleted, done.rows_absent),
        (501, 500, 1)
    );
    assert!(
        deletion::run(&paths, &job.id, options(None))
            .await
            .unwrap()
            .completed
    );
    let survivors = client
        .query(&format!("SELECT id,note FROM {schema}.events"), &[])
        .await
        .unwrap();
    assert_eq!(survivors.len(), 1);
    assert_eq!(survivors[0].get::<_, i64>(0), 9999);
    assert_eq!(survivors[0].get::<_, String>(1), "survivor");
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    driver.abort();
}

#[tokio::test]
#[ignore = "requires disposable COLDCTL_TEST_POSTGRES_URL and updated PostgreSQL connector"]
async fn deletion_allows_unrelated_writes_and_rolls_back_concurrent_changes() {
    async fn connect() -> tokio_postgres::Client {
        let config: tokio_postgres::Config = std::env::var("COLDCTL_TEST_POSTGRES_URL")
            .unwrap()
            .parse()
            .unwrap();
        let (client, connection) = config
            .connect(postgres_native_tls::MakeTlsConnector::new(
                native_tls::TlsConnector::new().unwrap(),
            ))
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    let client = connect().await;
    let mut blocker = connect().await;
    let schema = format!("locking_{}", uuid::Uuid::new_v4().simple());
    client.batch_execute(&format!("CREATE SCHEMA {schema}; CREATE TABLE {schema}.events(id bigint PRIMARY KEY,created_at timestamptz,note text); INSERT INTO {schema}.events SELECT n,now()-interval '500 days','original' FROM generate_series(1,500)n; INSERT INTO {schema}.events VALUES(9999,now(),'recent'); CREATE TABLE {schema}.child(id bigint); CREATE FUNCTION {schema}.noop() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN RETURN OLD; END';")).await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(
        &paths,
        "source",
        SourceConnection::from_url_env("COLDCTL_TEST_POSTGRES_URL".into()).unwrap(),
    )
    .unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
    jobs::policy_create(
        &paths,
        "events",
        "source",
        "disk",
        PolicyConfig {
            schema: schema.clone(),
            table: "events".into(),
            time_column: "created_at".into(),
            older_than_days: 365,
            batch_size: 500,
            equals_column: None,
            equals_value: None,
        },
    )
    .unwrap();
    let job = executor::run(&paths, "events", archive_options())
        .await
        .unwrap();
    let tx = blocker.transaction().await.unwrap();
    tx.execute(
        &format!("UPDATE {schema}.events SET note='concurrent change' WHERE id=500"),
        &[],
    )
    .await
    .unwrap();
    let check = async {
        // Wait until the real connector holds its schema guard and waits on our row.
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let held:bool=client.query_one("SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE l.relation=$1::text::regclass AND l.mode='ShareUpdateExclusiveLock' AND l.granted AND a.application_name='coldctl-delete' AND a.wait_event_type='Lock')", &[&format!("{schema}.events")]).await.unwrap().get(0);
            if held {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "connector did not acquire write-compatible schema guard"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        client.batch_execute(&format!("SET lock_timeout='100ms'; UPDATE {schema}.events SET note='writer succeeded' WHERE id=9999; INSERT INTO {schema}.events VALUES(8888,now(),'temporary'); DELETE FROM {schema}.events WHERE id=8888;")).await.unwrap();
        // Schema changes must remain fenced while ordinary writes are allowed.
        for ddl in [
            format!("ALTER TABLE {schema}.events ADD COLUMN unsafe text"),
            format!(
                "CREATE TRIGGER unsafe BEFORE DELETE ON {schema}.events FOR EACH ROW EXECUTE FUNCTION {schema}.noop()"
            ),
            format!(
                "ALTER TABLE {schema}.child ADD FOREIGN KEY(id) REFERENCES {schema}.events(id)"
            ),
        ] {
            let error = client.batch_execute(&ddl).await.unwrap_err();
            assert_eq!(error.code().unwrap().code(), "55P03");
        }
        tx.commit().await.unwrap();
    };
    let (deleted, ()) = tokio::join!(deletion::run(&paths, &job.id, options(None)), check);
    assert!(
        deleted.is_err(),
        "concurrent changed row must roll back the batch"
    );
    assert_eq!(
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.events"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        501
    );
    assert_eq!(
        client
            .query_one(
                &format!("SELECT note FROM {schema}.events WHERE id=9999"),
                &[]
            )
            .await
            .unwrap()
            .get::<_, String>(0),
        "writer succeeded"
    );
    client
        .batch_execute(&format!(
            "UPDATE {schema}.events SET note='original' WHERE id=500"
        ))
        .await
        .unwrap();
    // Holding a target row beyond lock_timeout must fail, not skip it as absent.
    let tx = blocker.transaction().await.unwrap();
    tx.query_one(
        &format!("SELECT id FROM {schema}.events WHERE id=1 FOR UPDATE"),
        &[],
    )
    .await
    .unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    tx.rollback().await.unwrap();
    assert_eq!(
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.events"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        501
    );
    let done = deletion::run(&paths, &job.id, options(None)).await.unwrap();
    assert!(done.completed);
    assert_eq!((done.rows_deleted, done.rows_absent), (500, 0));
    assert_eq!(
        client
            .query_one(&format!("SELECT count(*) FROM {schema}.events"), &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
}
