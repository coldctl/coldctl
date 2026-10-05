use arrow_array::{Int64Array, StringArray};
use coldctl_core::{
    archive::{executor, manifest::Manifest, planner, verifier},
    paths::StatePaths,
    policy::model::PolicyConfig,
    source::{ArchiveSource, SourceConnection, postgres_archive::PostgresArchive},
    state::{self, archive as store, destinations, sources},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

#[tokio::test]
#[ignore = "requires COLDCTL_TEST_POSTGRES_URL pointing to a disposable PostgreSQL database"]
async fn archive_exports_multiple_batches_preserves_rows_and_records_failures() {
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
    let schema = format!("archive_test_{}", uuid::Uuid::new_v4().simple());
    client.batch_execute(&format!(r#"
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}."Order History" (id bigint PRIMARY KEY,created_at timestamptz,status text,amount numeric(30,9),active boolean,note text);
        INSERT INTO {schema}."Order History" SELECT n,now()-INTERVAL '500 days','completed',12345678901234567890.123456789,true,'synthetic' FROM generate_series(1,5) n;
        INSERT INTO {schema}."Order History" VALUES (-9223372036854775808,now()-INTERVAL '500 days','completed',0.00,NULL,NULL), (6,now(),'completed',1.0,false,'recent'),(7,now()-INTERVAL '500 days','pending',1.0,false,'pending'),(8,NULL,'completed',1.0,false,'undated');
        ANALYZE {schema}."Order History";
    "#)).await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    let connection = SourceConnection::from_url_env("COLDCTL_TEST_POSTGRES_URL".into()).unwrap();
    sources::add(&paths, "source", connection.clone()).unwrap();
    let root = temp.path().join("archives");
    destinations::add_local(&paths, "disk", &root).unwrap();
    let policy = PolicyConfig {
        schema: schema.clone(),
        table: "Order History".into(),
        time_column: "created_at".into(),
        older_than_days: 365,
        batch_size: 2,
        equals_column: Some("status".into()),
        equals_value: Some("completed".into()),
    };
    store::policy_create(&paths, "orders", "source", "disk", policy.clone()).unwrap();
    let plan = planner::plan(&paths, "orders").await.unwrap();
    assert!(plan.estimated_rows.is_some());
    assert!(!plan.delete);
    assert!(!root.exists());
    assert!(store::job_list(&paths).unwrap().is_empty());
    let mut impossible_space = options();
    impossible_space.minimum_free_bytes = u64::MAX;
    assert!(
        executor::run(&paths, "orders", impossible_space)
            .await
            .unwrap_err()
            .to_string()
            .contains("insufficient")
    );
    assert!(store::job_list(&paths).unwrap().is_empty());
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    client
        .batch_execute(&format!(
            "ALTER TABLE {schema}.\"Order History\" ADD COLUMN unsupported bytea"
        ))
        .await
        .unwrap();
    assert!(
        executor::run(&paths, "orders", options())
            .await
            .unwrap_err()
            .to_string()
            .contains("unsupported column")
    );
    client.batch_execute(&format!("ALTER TABLE {schema}.\"Order History\" DROP COLUMN unsupported; ALTER TABLE {schema}.\"Order History\" ENABLE ROW LEVEL SECURITY")).await.unwrap();
    assert!(
        executor::run(&paths, "orders", options())
            .await
            .unwrap_err()
            .to_string()
            .contains("row-level security")
    );
    client
        .batch_execute(&format!(
            "ALTER TABLE {schema}.\"Order History\" DISABLE ROW LEVEL SECURITY"
        ))
        .await
        .unwrap();
    assert!(store::job_list(&paths).unwrap().is_empty());
    let job = executor::run(&paths, "orders", options()).await.unwrap();
    assert!(
        job.plan
            .safety
            .as_ref()
            .unwrap()
            .available_bytes_at_preflight
            > 0
    );
    let mut no_repeat = options();
    no_repeat.allow_repeat = false;
    assert!(
        executor::run(&paths, "orders", no_repeat)
            .await
            .unwrap_err()
            .to_string()
            .contains("--allow-repeat")
    );
    assert_eq!(store::job_list(&paths).unwrap().len(), 1);
    assert_eq!(job.status, store::JobStatus::Completed);
    assert_eq!(job.rows_processed, 6);
    assert_eq!(job.objects_created, 3);
    assert!(verifier::verify(&paths, &job.id).unwrap().verified);
    let manifest: Manifest =
        serde_json::from_slice(&std::fs::read(root.join(&job.id).join("manifest.json")).unwrap())
            .unwrap();
    assert_eq!(manifest.rows, 6);
    assert!(!manifest.source_deleted);
    assert_eq!(manifest.objects.len(), 3);
    let mut bounded = options();
    bounded.execution.max_rows = Some(3);
    let trial = executor::run(&paths, "orders", bounded).await.unwrap();
    assert_eq!(trial.status, store::JobStatus::Paused);
    assert_eq!(trial.rows_processed, 3);
    assert_eq!(trial.objects_created, 2);
    let second = executor::resume(&paths, &trial.id, true).await.unwrap();
    assert_eq!(second.status, store::JobStatus::Paused);
    assert_eq!(second.rows_processed, 6);
    assert_eq!(second.plan.cutoff_utc, trial.plan.cutoff_utc);
    let final_trial = executor::resume(&paths, &trial.id, true).await.unwrap();
    assert_eq!(final_trial.status, store::JobStatus::Completed);
    verifier::verify(&paths, &trial.id).unwrap();
    // An exclusive lock must produce a bounded safe failure with our configured timeout.
    client
        .batch_execute(&format!(
            "BEGIN; LOCK TABLE {schema}.\"Order History\" IN ACCESS EXCLUSIVE MODE"
        ))
        .await
        .unwrap();
    let mut blocked = options();
    blocked.execution.lock_timeout_ms = 50;
    blocked.execution.query_timeout_ms = 500;
    let started = std::time::Instant::now();
    let blocked_result = executor::run(&paths, "orders", blocked).await;
    client.batch_execute("ROLLBACK").await.unwrap();
    assert!(blocked_result.is_err());
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
    assert!(manifest.plan.safety.is_some());
    let destination =
        coldctl_core::destination::local::LocalDestination::new(root.clone()).unwrap();
    let mut ids = Vec::new();
    for object in &manifest.objects {
        use coldctl_core::destination::ArchiveDestination;
        destination.verify(&object.object).unwrap();
        let file = std::fs::File::open(root.join(&object.object.key)).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let amounts = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                ids.push(keys.value(i));
                if keys.value(i) > 0 {
                    assert_eq!(amounts.value(i), "12345678901234567890.123456789");
                }
            }
        }
    }
    assert_eq!(ids, [i64::MIN, 1, 2, 3, 4, 5]);
    let again = executor::run(&paths, "orders", options()).await.unwrap();
    assert_ne!(job.id, again.id);
    assert_eq!(again.rows_processed, 6);
    let mut empty = policy.clone();
    empty.equals_value = Some("'; DROP TABLE whatever;--".into());
    store::policy_create(&paths, "empty", "source", "disk", empty).unwrap();
    let empty = executor::run(&paths, "empty", options()).await.unwrap();
    assert_eq!(empty.rows_processed, 0);
    assert_eq!(empty.objects_created, 0);
    let empty_manifest: Manifest =
        serde_json::from_slice(&std::fs::read(root.join(empty.id).join("manifest.json")).unwrap())
            .unwrap();
    assert!(empty_manifest.objects.is_empty());
    client.batch_execute(&format!("INSERT INTO {schema}.\"Order History\" VALUES (9,now()-INTERVAL '500 days','completed',0,true,repeat('PRIVATE_ROW',8000))")).await.unwrap();
    let failed = executor::run(&paths, "orders", options())
        .await
        .unwrap_err();
    assert!(!failed.to_string().contains("PRIVATE_ROW"));
    let id = match failed {
        coldctl_core::error::Error::JobFailed { job_id, .. } => job_id,
        _ => panic!("expected failed job"),
    };
    let failed = store::job_show(&paths, &id).unwrap();
    assert_eq!(failed.status, store::JobStatus::Failed);
    assert_eq!(failed.rows_processed, 6);
    assert_eq!(
        failed.failure.as_ref().unwrap().category,
        coldctl_core::error::FailureCategory::OversizedRow
    );
    assert!(!failed.failure.as_ref().unwrap().retryable);
    assert_eq!(
        failed.progress.as_ref().unwrap().stage,
        coldctl_core::archive::progress::Stage::Failed
    );
    assert!(
        !serde_json::to_string(&failed)
            .unwrap()
            .contains("PRIVATE_ROW")
    );
    assert!(!root.join(&id).join("manifest.json").exists());
    assert!(
        executor::resume(&paths, &id, false)
            .await
            .unwrap_err()
            .to_string()
            .contains("--confirm-original-source")
    );
    // Same PostgreSQL type name with different precision must also block resume.
    client
        .batch_execute(&format!(
            "ALTER TABLE {schema}.\"Order History\" ALTER COLUMN amount TYPE numeric(31,9)"
        ))
        .await
        .unwrap();
    assert!(executor::resume(&paths, &id, true).await.is_err());
    assert_eq!(store::job_show(&paths, &id).unwrap().rows_processed, 6);
    assert_eq!(
        store::job_show(&paths, &id)
            .unwrap()
            .failure
            .unwrap()
            .category,
        coldctl_core::error::FailureCategory::Schema
    );
    client
        .batch_execute(&format!(
            "ALTER TABLE {schema}.\"Order History\" ALTER COLUMN amount TYPE numeric(30,9)"
        ))
        .await
        .unwrap();
    // Repair the oversized fixture row and add a later row outside the saved bound.
    client.batch_execute(&format!("UPDATE {schema}.\"Order History\" SET note='repaired' WHERE id=9; INSERT INTO {schema}.\"Order History\" VALUES (11,now()-INTERVAL '500 days','completed',0,true,'after start')")).await.unwrap();
    let resumed = executor::resume(&paths, &id, true).await.unwrap();
    assert_eq!(resumed.id, id);
    assert!(resumed.failure.is_none());
    assert_eq!(
        resumed.progress.as_ref().unwrap().stage,
        coldctl_core::archive::progress::Stage::Completed
    );
    assert_eq!(resumed.rows_processed, 7);
    assert_eq!(resumed.objects_created, 4);
    assert_eq!(resumed.plan.cutoff_utc, failed.plan.cutoff_utc);
    assert!(verifier::verify(&paths, &id).unwrap().verified);
    // Repeating resume on a completed job verifies it rather than exporting again.
    assert_eq!(
        executor::resume(&paths, &id, true)
            .await
            .unwrap()
            .objects_created,
        4
    );
    client
        .batch_execute(&format!(
            "DELETE FROM {schema}.\"Order History\" WHERE id=11"
        ))
        .await
        .unwrap();
    // A changed table schema invalidates the fixed plan before its high-water mark is taken.
    let mut reader = PostgresArchive::connect(connection).await.unwrap();
    client
        .batch_execute(&format!(
            "ALTER TABLE {schema}.\"Order History\" ADD COLUMN added text"
        ))
        .await
        .unwrap();
    assert!(reader.upper_key(&plan).await.is_err());
    let count: i64 = client
        .query_one(
            &format!("SELECT count(*) FROM {schema}.\"Order History\""),
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 10);
    client
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .await
        .unwrap();
    driver.abort();
}

fn options() -> executor::RunOptions {
    executor::RunOptions {
        progress: None,
        source_stability: planner::SourceStability::QuiescentCopy,
        allow_repeat: true,
        minimum_free_bytes: 1,
        execution: Default::default(),
        shutdown: Default::default(),
    }
}
