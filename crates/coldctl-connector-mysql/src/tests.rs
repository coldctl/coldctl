use super::*;
use coldctl_core::{
    archive::{controls::ExecutionControls, executor, planner, verifier},
    paths::StatePaths,
    policy::model::PolicyConfig,
    source::{DataSource, SourceConnection, postgres::PostgresSource, postgres_restore},
    state::{self, archive as store, destinations, sources},
};
use mysql_async::{Row, prelude::Queryable};
fn run_options(max: Option<u64>) -> executor::RunOptions {
    executor::RunOptions {
        source_stability: planner::SourceStability::QuiescentCopy,
        allow_repeat: false,
        minimum_free_bytes: 1,
        execution: ExecutionControls {
            max_rows: max,
            ..Default::default()
        },
        shutdown: Default::default(),
        progress: None,
    }
}
fn restore(target: &str) -> postgres_restore::Options {
    postgres_restore::Options {
        target: "target".into(),
        schema: "coldctl_test_restore".into(),
        table: target.into(),
        confirm_separate_target: true,
        max_batches: None,
        shutdown: Default::default(),
        validate_only: false,
        progress: None,
    }
}
#[tokio::test]
#[ignore = "requires disposable MySQL 8.4 source/restore databases, TLS CA and built native connector; see MYSQL_CONNECTOR.md"]
async fn mysql_tls_unsigned_archive_resume_restore_and_least_privilege() {
    let source_config = SourceConnection::from_mysql_url_env("COLDCTL_TEST_MYSQL_URL".into())
        .unwrap()
        .with_ca_env(Some("COLDCTL_TEST_MYSQL_CA".into()))
        .unwrap();
    let target_config =
        SourceConnection::from_mysql_url_env("COLDCTL_TEST_MYSQL_RESTORE_URL".into())
            .unwrap()
            .with_ca_env(Some("COLDCTL_TEST_MYSQL_CA".into()))
            .unwrap();
    let source_resolved = source_config.resolve().unwrap();
    let target_resolved = target_config.resolve().unwrap();
    let mut admin = mysql::connect(&source_resolved, false).await.unwrap();
    let mut target = mysql::connect(&target_resolved, false).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let table = format!("events_{suffix}");
    let restored = format!("restored_{suffix}");
    let reader = format!("reader_{suffix}");
    let writer = format!("writer_{suffix}");
    let source_table = mysql::table(&source_resolved.database, &table);
    let target_table = mysql::table(&target_resolved.database, &restored);
    admin.query_drop(format!("CREATE TABLE {source_table}(id BIGINT UNSIGNED PRIMARY KEY,created_at DATETIME(6) NOT NULL, signed_value BIGINT,amount DECIMAL(65,30),note VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,bytes VARBINARY(30),real_value DOUBLE,small_float FLOAT, day DATE, duration TIME(6), stamp TIMESTAMP(6) NULL) ENGINE=InnoDB")).await.unwrap();
    for key in [0u64, 1, i64::MAX as u64, 1u64 << 63, u64::MAX] {
        admin.exec_drop(format!("INSERT INTO {source_table} VALUES(?,'2020-01-01 00:00:00.123456',-9223372036854775808,12345678901234567890123456789012345.123456789012345678901234567890,?,X'0001FF207F',1.2345678901234567,1.234567,'1000-01-01','-838:59:58.123456','2020-01-02 12:34:56.123456')"),(key,"PRIVATE_MYSQL snow 雪 ' quote \\ NUL\0")).await.unwrap();
    }
    admin.query_drop(format!("UPDATE {source_table} SET note=NULL,bytes=NULL,amount=NULL,real_value=NULL,small_float=NULL,day=NULL,duration=NULL,stamp=NULL WHERE id=1")).await.unwrap();
    admin
        .query_drop(format!(
            "CREATE USER '{reader}'@'%' IDENTIFIED BY 'coldctl_mysql_role_test' REQUIRE SSL"
        ))
        .await
        .unwrap();
    admin
        .query_drop(format!(
            "GRANT SELECT ON coldctl_test_source.* TO '{reader}'@'%'"
        ))
        .await
        .unwrap();
    admin
        .query_drop(format!("GRANT PROCESS ON *.* TO '{reader}'@'%'"))
        .await
        .unwrap();
    admin
        .query_drop(format!(
            "CREATE USER '{writer}'@'%' IDENTIFIED BY 'coldctl_mysql_role_test' REQUIRE SSL"
        ))
        .await
        .unwrap();
    admin
        .query_drop(format!(
            "GRANT SELECT,INSERT,UPDATE,CREATE ON coldctl_test_restore.* TO '{writer}'@'%'"
        ))
        .await
        .unwrap();
    admin
        .query_drop(format!("GRANT PROCESS ON *.* TO '{writer}'@'%'"))
        .await
        .unwrap();
    let role = |username: &str, db: &str| {
        SourceConnection::from_url(
            &format!(
                "mysql://{username}@localhost:{}/{db}?sslmode=require",
                source_resolved.port
            ),
            Some("COLDCTL_TEST_MYSQL_ROLE_PASSWORD".into()),
        )
        .unwrap()
        .with_ca_env(Some("COLDCTL_TEST_MYSQL_CA".into()))
        .unwrap()
    };
    let reader_config = role(&reader, "coldctl_test_source");
    let writer_config = role(&writer, "coldctl_test_restore");
    let mut restricted = mysql::connect(&reader_config.resolve().unwrap(), true)
        .await
        .unwrap();
    assert!(
        restricted
            .query_drop(format!("DELETE FROM {source_table}"))
            .await
            .is_err()
    );
    let mut no_ca = source_resolved.clone();
    no_ca.ca_pem = None;
    assert!(
        mysql::connect(&no_ca, true).await.is_err(),
        "untrusted certificate must fail"
    );
    let mut wrong = source_resolved.clone();
    wrong.password = Some("PRIVATE_WRONG_PASSWORD".into());
    let error = mysql::connect(&wrong, true).await.err().unwrap();
    assert!(!error.to_string().contains("PRIVATE_WRONG_PASSWORD"));
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(&paths, "source", reader_config.clone()).unwrap();
    sources::add(&paths, "target", writer_config).unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archive")).unwrap();
    let discovery = PostgresSource::in_state(reader_config.clone(), &paths)
        .discover()
        .await
        .unwrap();
    assert!(
        discovery
            .tables
            .iter()
            .any(|t| t.name == table && t.primary_key == ["id"])
    );
    let analysis = PostgresSource::in_state(reader_config.clone(), &paths)
        .analyze()
        .await
        .unwrap();
    assert!(analysis.tables.iter().any(
        |t| t.table.name == table && t.time_candidates.iter().any(|c| c.column == "created_at")
    ));
    store::policy_create(
        &paths,
        "events",
        "source",
        "disk",
        PolicyConfig {
            schema: "coldctl_test_source".into(),
            table: table.clone(),
            time_column: "created_at".into(),
            older_than_days: 365,
            batch_size: 2,
            equals_column: None,
            equals_value: None,
        },
    )
    .unwrap();
    let paused = executor::run(&paths, "events", run_options(Some(2)))
        .await
        .unwrap();
    assert_eq!(paused.status, store::JobStatus::Paused);
    assert_eq!(paused.rows_processed, 2);
    let done = executor::resume_with_controls(
        &paths,
        &paused.id,
        true,
        ExecutionControls {
            minimum_free_bytes: 1,
            ..Default::default()
        },
        Default::default(),
    )
    .await
    .unwrap();
    assert_eq!(done.rows_processed, 5);
    assert_eq!(done.last_key, Some(i64::MAX.into()));
    verifier::verify(&paths, &done.id).unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            temp.path()
                .join("archive")
                .join(&done.id)
                .join("manifest.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(manifest["format_version"], 3);
    let recovered = StatePaths::resolve(Some(&temp.path().join("recovered"))).unwrap();
    state::initialize(&recovered, "test").unwrap();
    coldctl_core::archive::recovery::import(
        &recovered,
        &temp.path().join("archive").join(&done.id),
        None,
    )
    .unwrap();
    // Reconcile a crash after both setup DDL statements, before recording target identity.
    let manifest_bytes = std::fs::read(
        temp.path()
            .join("archive")
            .join(&done.id)
            .join("manifest.json"),
    )
    .unwrap();
    let owner = format!("coldctl:{:x}", sha2::Sha256::digest(manifest_bytes));
    let setup_journal = format!(
        "_coldctl_restore_{:x}",
        sha2::Sha256::digest(format!("coldctl_test_restore:{restored}"))
    );
    let setup_journal = mysql::table("coldctl_test_restore", &setup_journal[..58]);
    target.query_drop(format!("CREATE TABLE {setup_journal}(id TINYINT PRIMARY KEY,batches BIGINT NOT NULL,row_count BIGINT NOT NULL,completed BOOLEAN NOT NULL,target_id BIGINT UNSIGNED NULL) ENGINE=InnoDB COMMENT='{owner}'")).await.unwrap();
    target
        .query_drop(format!("INSERT INTO {setup_journal} VALUES(1,0,0,0,NULL)"))
        .await
        .unwrap();
    target
        .query_drop(format!("CREATE TABLE {target_table} LIKE {source_table}"))
        .await
        .unwrap();
    target
        .query_drop(format!("ALTER TABLE {target_table} COMMENT='{owner}'"))
        .await
        .unwrap();
    let mut partial = restore(&restored);
    partial.max_batches = Some(1);
    let saved = postgres_restore::run(&paths, &done.id, partial)
        .await
        .unwrap();
    assert_eq!(saved.rows, 2);
    assert_eq!(saved.status, "paused");
    // A trigger injects failure after one insert without rebuilding the InnoDB table.
    target.query_drop(format!("CREATE TRIGGER coldctl_test_restore.fail_{suffix} BEFORE INSERT ON {target_table} FOR EACH ROW BEGIN IF NEW.id=9223372036854775808 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='injected failure'; END IF; END")).await.unwrap();
    assert!(
        postgres_restore::run(&paths, &done.id, restore(&restored))
            .await
            .is_err()
    );
    let count: Option<u64> = target
        .query_first(format!("SELECT COUNT(*) FROM {target_table}"))
        .await
        .unwrap();
    assert_eq!(count, Some(2));
    target
        .query_drop(format!("DROP TRIGGER coldctl_test_restore.fail_{suffix}"))
        .await
        .unwrap();
    let complete = postgres_restore::run(&paths, &done.id, restore(&restored))
        .await
        .unwrap();
    assert_eq!(complete.rows, 5);
    assert_eq!(complete.status, "completed");
    assert_eq!(
        postgres_restore::run(&paths, &done.id, restore(&restored))
            .await
            .unwrap()
            .rows,
        5
    );
    let source_rows: Vec<Row> = admin
        .exec(format!("SELECT * FROM {source_table} ORDER BY id"), ())
        .await
        .unwrap();
    let target_rows: Vec<Row> = target
        .exec(format!("SELECT * FROM {target_table} ORDER BY id"), ())
        .await
        .unwrap();
    assert!(
        source_rows
            .into_iter()
            .map(Row::unwrap)
            .eq(target_rows.into_iter().map(Row::unwrap)),
        "round-trip values differ"
    );
    // Tampering is caught by full prefix validation on the next invocation.
    target
        .query_drop(format!(
            "UPDATE {target_table} SET note='tampered' WHERE id=0"
        ))
        .await
        .unwrap();
    assert!(
        postgres_restore::run(&paths, &done.id, restore(&restored))
            .await
            .is_err()
    );
    // Removing and recreating a source with identical columns cannot resume its old identity.
    let mut fresh = run_options(Some(2));
    fresh.allow_repeat = true;
    let stopped = executor::run(&paths, "events", fresh).await.unwrap();
    admin
        .query_drop(format!("TRUNCATE TABLE {source_table}"))
        .await
        .unwrap();
    assert!(
        executor::resume_with_controls(
            &paths,
            &stopped.id,
            true,
            ExecutionControls::default(),
            Default::default()
        )
        .await
        .is_err()
    );
    admin
        .query_drop(format!("DROP TABLE {source_table}"))
        .await
        .unwrap();
    target
        .query_drop(format!("DROP TABLE {target_table}"))
        .await
        .unwrap();
    use sha2::Digest;
    let journal = format!(
        "_coldctl_restore_{:x}",
        sha2::Sha256::digest(format!("coldctl_test_restore:{restored}"))
    );
    target
        .query_drop(format!(
            "DROP TABLE {}",
            mysql::table("coldctl_test_restore", &journal[..58])
        ))
        .await
        .unwrap();
    admin
        .query_drop(format!("DROP USER '{reader}'@'%', '{writer}'@'%'"))
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires the disposable MySQL qualification environment"]
async fn mysql_rejects_unqualified_schema_and_oversized_values() {
    let config = SourceConnection::from_mysql_url_env("COLDCTL_TEST_MYSQL_URL".into())
        .unwrap()
        .with_ca_env(Some("COLDCTL_TEST_MYSQL_CA".into()))
        .unwrap();
    let resolved = config.resolve().unwrap();
    let mut admin = mysql::connect(&resolved, false).await.unwrap();
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("limits_{suffix}");
    let qualified = mysql::table(&resolved.database, &name);
    let policy = |table: &str| coldctl_core::policy::model::Policy {
        id: "fixture".into(),
        name: "fixture".into(),
        source: "fixture".into(),
        destination: "fixture".into(),
        created_at: "test".into(),
        config: PolicyConfig {
            schema: resolved.database.clone(),
            table: table.into(),
            time_column: "created_at".into(),
            older_than_days: 30,
            batch_size: 2,
            equals_column: None,
            equals_value: None,
        },
    };
    // Adjacent keys across both 2^63 and 2^64 boundaries must not compare as FLOAT.
    admin
        .query_drop(format!(
            "CREATE TABLE {qualified}(id BIGINT UNSIGNED PRIMARY KEY,created_at DATE) ENGINE=InnoDB"
        ))
        .await
        .unwrap();
    let expected = [0u64, 1, i64::MAX as u64, 1u64 << 63, u64::MAX - 1, u64::MAX];
    for key in expected {
        admin
            .exec_drop(
                format!("INSERT INTO {qualified} VALUES(?,'2020-01-01')"),
                (key,),
            )
            .await
            .unwrap();
    }
    let mut scan = mysql_archive::MysqlArchive::connect(resolved.clone())
        .await
        .unwrap();
    let mut one = policy(&name);
    one.config.batch_size = 1;
    let plan = scan
        .plan(one, std::path::PathBuf::from("unused"))
        .await
        .unwrap();
    let upper = scan.upper_key(&plan).await.unwrap().unwrap();
    let mut last = None;
    for key in expected {
        let batch = scan.read_batch(&plan, last, upper).await.unwrap().unwrap();
        assert_eq!(batch.rows.len(), 1);
        assert_eq!(batch.rows[0][0].as_deref(), Some(key.to_string().as_str()));
        last = Some(batch.last_key.integer().unwrap());
    }
    assert!(scan.read_batch(&plan, last, upper).await.unwrap().is_none());
    admin
        .query_drop(format!("DROP TABLE {qualified}"))
        .await
        .unwrap();
    for definition in [
        "id BIGINT PRIMARY KEY,created_at DATE,payload JSON",
        "id VARCHAR(20) PRIMARY KEY,created_at DATE",
        "id INT,other INT,created_at DATE,PRIMARY KEY(id,other)",
    ] {
        admin
            .query_drop(format!(
                "CREATE TABLE {qualified}({definition}) ENGINE=InnoDB"
            ))
            .await
            .unwrap();
        let mut scan = mysql_archive::MysqlArchive::connect(resolved.clone())
            .await
            .unwrap();
        assert!(
            scan.plan(policy(&name), std::path::PathBuf::from("unused"))
                .await
                .is_err()
        );
        admin
            .query_drop(format!("DROP TABLE {qualified}"))
            .await
            .unwrap();
    }
    admin
        .query_drop(format!(
            "CREATE TABLE {qualified}(id BIGINT PRIMARY KEY,created_at DATE) ENGINE=MyISAM"
        ))
        .await
        .unwrap();
    let mut scan = mysql_archive::MysqlArchive::connect(resolved.clone())
        .await
        .unwrap();
    assert!(
        scan.plan(policy(&name), std::path::PathBuf::from("unused"))
            .await
            .is_err()
    );
    admin
        .query_drop(format!("DROP TABLE {qualified}"))
        .await
        .unwrap();
    admin.query_drop(format!("CREATE TABLE {qualified}(id BIGINT PRIMARY KEY,created_at DATE,payload LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin) ENGINE=InnoDB")).await.unwrap();
    admin.query_drop(format!("INSERT INTO {qualified} VALUES(-9223372036854775808,'2020-01-01','low'),(9223372036854775807,'2020-01-01','high')")).await.unwrap();
    let mut scan = mysql_archive::MysqlArchive::connect(resolved.clone())
        .await
        .unwrap();
    let plan = scan
        .plan(policy(&name), std::path::PathBuf::from("unused"))
        .await
        .unwrap();
    let upper = scan.upper_key(&plan).await.unwrap().unwrap();
    assert_eq!(upper, i64::MAX);
    let batch = scan.read_batch(&plan, None, upper).await.unwrap().unwrap();
    assert_eq!(batch.rows[0][0].as_deref(), Some("-9223372036854775808"));
    assert_eq!(batch.last_key, i64::MAX.into());
    admin
        .query_drop(format!(
            "UPDATE {qualified} SET payload=REPEAT('x',31000) WHERE id=9223372036854775807"
        ))
        .await
        .unwrap();
    assert!(scan.read_batch(&plan, None, upper).await.is_err());
    admin.query_drop("SET SESSION sql_mode=''").await.unwrap();
    admin
        .query_drop(format!(
            "UPDATE {qualified} SET payload='',created_at='0000-00-00' WHERE id=9223372036854775807"
        ))
        .await
        .unwrap();
    assert!(scan.read_batch(&plan, None, upper).await.is_err());
    admin
        .query_drop(format!("DROP TABLE {qualified}"))
        .await
        .unwrap();
}
