use super::*;
use coldctl_core::{
    archive::{controls::ExecutionControls, executor, planner, verifier},
    paths::StatePaths,
    policy::model::PolicyConfig,
    source::{DataSource, SourceConnection, postgres::PostgresSource, postgres_restore},
    state::{self, archive as store, destinations, sources},
};
use mysql_async::{Row, prelude::Queryable};

#[test]
fn incoming_fk_refusal_survives_connector_error_transport() {
    let error = classify(&Error::Archive(
        "MySQL deletion refuses triggers or incoming foreign keys: parent deletion blocked for every incoming FK, including CASCADE and SET NULL; child archival cannot be assumed",
    ));
    assert_eq!(error, runtime::Error::IncomingForeignKey);
    let encoded = serde_json::to_string(&error).unwrap();
    let decoded: runtime::Error = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, error);
    assert!(decoded.to_string().contains("incoming foreign keys"));
}

#[tokio::test]
#[ignore = "requires disposable MySQL 8.4, TLS environment and built connector"]
async fn mysql_verified_deletion_rollback_replay_and_resume() {
    use coldctl_core::{archive::deletion, source::connector::DeleteSpec};
    let config = SourceConnection::from_mysql_url_env("COLDCTL_TEST_MYSQL_URL".into())
        .unwrap()
        .with_ca_env(Some("COLDCTL_TEST_MYSQL_CA".into()))
        .unwrap();
    let resolved = config.resolve().unwrap();
    let mut admin = mysql::connect(&resolved, false).await.unwrap();
    let name = format!(
        "delete_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let target = mysql::table(&resolved.database, &name);
    admin.query_drop(format!("CREATE TABLE {target}(id BIGINT UNSIGNED PRIMARY KEY, created_at DATETIME(6), note TEXT, bytes VARBINARY(8)) ENGINE=InnoDB")).await.unwrap();
    for key in [0u64, 1, u64::MAX] {
        admin
            .exec_drop(
                format!("INSERT INTO {target} VALUES(?,'2020-01-01','original',X'00FF')"),
                (key,),
            )
            .await
            .unwrap();
    }
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(&paths, "source", config).unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
    store::policy_create(
        &paths,
        "events",
        "source",
        "disk",
        PolicyConfig {
            schema: resolved.database.clone(),
            table: name.clone(),
            time_column: "created_at".into(),
            older_than_days: 365,
            batch_size: 2,
            equals_column: None,
            equals_value: None,
        },
    )
    .unwrap();
    let job = executor::run(&paths, "events", run_options(None))
        .await
        .unwrap();
    let options = |max_batches| deletion::Options {
        confirm_delete: true,
        confirm_original_source: true,
        max_batches,
        shutdown: Default::default(),
    };
    // A mismatch on the second row must roll back deletion of the first.
    admin
        .query_drop(format!(
            "UPDATE {target} SET note=REPEAT('x',31000) WHERE id=1"
        ))
        .await
        .unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    assert_eq!(
        admin
            .query_first::<u64, _>(format!("SELECT count(*) FROM {target}"))
            .await
            .unwrap(),
        Some(3)
    );
    admin
        .query_drop(format!("UPDATE {target} SET note='original' WHERE id=1"))
        .await
        .unwrap();
    admin
        .query_drop(format!(
            "CREATE TRIGGER {} BEFORE DELETE ON {target} FOR EACH ROW SET @coldctl_trigger=1",
            mysql::q(&format!("tr_{name}"))
        ))
        .await
        .unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    admin
        .query_drop(format!("DROP TRIGGER {}", mysql::q(&format!("tr_{name}"))))
        .await
        .unwrap();
    let child = mysql::table("coldctl_test_restore", &name);
    admin.query_drop(format!("CREATE TABLE {child}(id BIGINT UNSIGNED PRIMARY KEY, FOREIGN KEY(id) REFERENCES {target}(id) ON DELETE CASCADE) ENGINE=InnoDB")).await.unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    admin
        .query_drop(format!("DROP TABLE {child}"))
        .await
        .unwrap();
    // Commit without a host checkpoint, then replay through the real protocol.
    let identity: String = admin
        .query_first("SELECT @@server_uuid")
        .await
        .unwrap()
        .unwrap();
    let native: Vec<Row> = admin
        .query(format!("SELECT * FROM {target} ORDER BY id LIMIT 2"))
        .await
        .unwrap();
    let rows = native
        .into_iter()
        .map(|r| mysql::encode(r, &job.plan.columns).unwrap())
        .collect::<Vec<_>>();
    admin
        .query_drop(format!(
            "CREATE USER '{name}'@'%' IDENTIFIED BY 'coldctl_delete_role' REQUIRE SSL"
        ))
        .await
        .unwrap();
    admin
        .query_drop(format!(
            "GRANT SELECT, LOCK TABLES ON {}.* TO '{name}'@'%'",
            mysql::q(&resolved.database)
        ))
        .await
        .unwrap();
    admin
        .query_drop(format!("GRANT PROCESS ON *.* TO '{name}'@'%' "))
        .await
        .unwrap();
    let mut restricted = resolved.clone();
    restricted.user = name.clone();
    restricted.password = Some("coldctl_delete_role".into());
    let spec = DeleteSpec {
        require_empty_children: false,
        plan: job.plan.clone(),
        source_identity: identity.clone(),
    };
    let mut denied = crate::mysql_delete::DeleteSession::open(restricted.clone(), spec.clone())
        .await
        .unwrap();
    assert!(denied.batch(&rows).await.is_err());
    drop(denied);
    admin
        .query_drop(format!("GRANT TRIGGER ON {target} TO '{name}'@'%' "))
        .await
        .unwrap();
    let mut denied = crate::mysql_delete::DeleteSession::open(restricted.clone(), spec.clone())
        .await
        .unwrap();
    assert!(denied.batch(&rows).await.is_err());
    drop(denied);
    admin
        .query_drop(format!("GRANT DELETE ON {target} TO '{name}'@'%' "))
        .await
        .unwrap();
    // A child table invisible to this role still prevents cascading deletion.
    admin.query_drop(format!("CREATE TABLE {child}(id BIGINT UNSIGNED PRIMARY KEY, FOREIGN KEY(id) REFERENCES {target}(id) ON DELETE CASCADE) ENGINE=InnoDB")).await.unwrap();
    let mut denied = crate::mysql_delete::DeleteSession::open(restricted.clone(), spec)
        .await
        .unwrap();
    assert!(denied.batch(&rows).await.is_err());
    drop(denied);
    admin
        .query_drop(format!("DROP TABLE {child}"))
        .await
        .unwrap();
    let mut direct = crate::mysql_delete::DeleteSession::open(
        restricted,
        DeleteSpec {
            require_empty_children: false,
            plan: job.plan.clone(),
            source_identity: identity,
        },
    )
    .await
    .unwrap();
    assert_eq!(direct.batch(&rows).await.unwrap().deleted, 2);
    drop(direct);
    admin
        .query_drop(format!("DROP USER '{name}'@'%' "))
        .await
        .unwrap();
    let paused = deletion::run(&paths, &job.id, options(Some(1)))
        .await
        .unwrap();
    assert_eq!(paused.rows_absent, 2);
    assert!(!paused.completed);
    let done = deletion::run(&paths, &job.id, options(None)).await.unwrap();
    assert!(done.completed);
    assert_eq!(done.rows_deleted, 1);
    assert_eq!(
        admin
            .query_first::<u64, _>(format!("SELECT count(*) FROM {target}"))
            .await
            .unwrap(),
        Some(0)
    );
    for key in [0u64, 1, u64::MAX] {
        admin
            .exec_drop(
                format!("INSERT INTO {target} VALUES(?,'2020-01-01','original',X'00FF')"),
                (key,),
            )
            .await
            .unwrap();
    }
    assert!(
        deletion::run(&paths, &job.id, options(None))
            .await
            .unwrap()
            .completed
    );
    assert_eq!(
        admin
            .query_first::<u64, _>(format!("SELECT count(*) FROM {target}"))
            .await
            .unwrap(),
        Some(3)
    );
    use coldctl_core::{archive::controls::Shutdown, scheduler::Worker, state::scheduler as queue};
    queue::enqueue(
        &paths,
        "events",
        None,
        queue::Options {
            delete_after_archive: true,
            source_stability: planner::SourceStability::QuiescentCopy,
            allow_repeat: true,
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
    assert_eq!(
        worker
            .tick(Shutdown::default())
            .await
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
    assert_eq!(
        admin
            .query_first::<u64, _>(format!("SELECT count(*) FROM {target}"))
            .await
            .unwrap(),
        Some(1)
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
    assert_eq!(
        admin
            .query_first::<u64, _>(format!("SELECT count(*) FROM {target}"))
            .await
            .unwrap(),
        Some(0)
    );
    admin
        .query_drop(format!("DROP TABLE {target}"))
        .await
        .unwrap();
}
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

#[tokio::test]
#[ignore = "requires disposable MySQL 8.4 with PROCESS and performance_schema access"]
async fn mysql_deletion_allows_unrelated_writes_and_guards_schema() {
    use coldctl_core::source::connector::DeleteSpec;
    let config = SourceConnection::from_mysql_url_env("COLDCTL_TEST_MYSQL_URL".into())
        .unwrap()
        .with_ca_env(Some("COLDCTL_TEST_MYSQL_CA".into()))
        .unwrap();
    let resolved = config.resolve().unwrap();
    let mut admin = mysql::connect(&resolved, false).await.unwrap();
    let mut blocker = mysql::connect(&resolved, false).await.unwrap();
    let mut writer = mysql::connect(&resolved, false).await.unwrap();
    writer
        .query_drop("SET SESSION lock_wait_timeout=1")
        .await
        .unwrap();
    writer
        .query_drop("SET SESSION innodb_lock_wait_timeout=1")
        .await
        .unwrap();
    let name = format!(
        "locking_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let target = mysql::table(&resolved.database, &name);
    let child = mysql::table(&resolved.database, &format!("child_{name}"));
    admin.query_drop(format!("CREATE TABLE {target}(id BIGINT PRIMARY KEY,created_at DATETIME(6),note TEXT) ENGINE=InnoDB")).await.unwrap();
    admin.query_drop(format!("INSERT INTO {target} VALUES(1,'2020-01-01','original'),(2,'2020-01-01','original'),(9999,UTC_TIMESTAMP(),'recent')")).await.unwrap();
    admin
        .query_drop(format!(
            "CREATE TABLE {child}(id BIGINT PRIMARY KEY) ENGINE=InnoDB"
        ))
        .await
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(&paths, "source", config).unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
    store::policy_create(
        &paths,
        "events",
        "source",
        "disk",
        PolicyConfig {
            schema: resolved.database.clone(),
            table: name.clone(),
            time_column: "created_at".into(),
            older_than_days: 365,
            batch_size: 20,
            equals_column: None,
            equals_value: None,
        },
    )
    .unwrap();
    let job = executor::run(&paths, "events", run_options(None))
        .await
        .unwrap();
    let identity: String = admin
        .query_first("SELECT @@server_uuid")
        .await
        .unwrap()
        .unwrap();
    let native: Vec<Row> = admin
        .query(format!("SELECT * FROM {target} WHERE id<3 ORDER BY id"))
        .await
        .unwrap();
    let rows = native
        .into_iter()
        .map(|r| mysql::encode(r, &job.plan.columns).unwrap())
        .collect::<Vec<_>>();
    let ddls = [
        format!("ALTER TABLE {target} ADD COLUMN unexpected INT"),
        format!(
            "CREATE TRIGGER {} BEFORE DELETE ON {target} FOR EACH ROW SET @coldctl_trigger=1",
            mysql::table(&resolved.database, &format!("tr_{name}"))
        ),
        format!(
            "ALTER TABLE {child} ADD FOREIGN KEY(id) REFERENCES {target}(id) ON DELETE CASCADE"
        ),
    ];
    for (phase, ddl) in ddls.iter().map(Some).chain([None, None]).enumerate() {
        blocker.query_drop("START TRANSACTION").await.unwrap();
        blocker
            .query_drop(format!("UPDATE {target} SET note='changed' WHERE id=2"))
            .await
            .unwrap();
        let mut deletion = crate::mysql_delete::DeleteSession::open(
            resolved.clone(),
            DeleteSpec {
                require_empty_children: false,
                plan: job.plan.clone(),
                source_identity: identity.clone(),
            },
        )
        .await
        .unwrap();
        let batch = rows.clone();
        let task = tokio::spawn(async move { deletion.batch(&batch).await });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let waiting: u64 = admin.exec_first("SELECT COUNT(*) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON w.REQUESTING_ENGINE_LOCK_ID=l.ENGINE_LOCK_ID WHERE l.OBJECT_SCHEMA=? AND l.OBJECT_NAME=?",(&resolved.database,&name)).await.unwrap().unwrap();
            if waiting > 0 {
                break;
            }
            assert!(
                !task.is_finished() && std::time::Instant::now() < deadline,
                "deletion did not reach row lock wait"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let started = std::time::Instant::now();
        writer
            .query_drop(format!(
                "UPDATE {target} SET note='writer succeeded' WHERE id=9999"
            ))
            .await
            .unwrap();
        writer
            .query_drop(format!(
                "INSERT INTO {target} VALUES(8888,UTC_TIMESTAMP(),'temporary')"
            ))
            .await
            .unwrap();
        writer
            .query_drop(format!("DELETE FROM {target} WHERE id=8888"))
            .await
            .unwrap();
        eprintln!(
            "phase {phase}: unrelated writes completed in {:?}",
            started.elapsed()
        );
        if let Some(ddl) = ddl {
            let error = writer.query_drop(ddl).await.unwrap_err();
            assert!(
                matches!(error,mysql_async::Error::Server(ref e) if e.code==1205),
                "{error}"
            );
            blocker.query_drop("COMMIT").await.unwrap();
            let error = task.await.unwrap().unwrap_err();
            assert!(error.to_string().contains("row differs"), "{error}");
        } else if phase == 3 {
            let error = task.await.unwrap().unwrap_err();
            assert!(error.to_string().contains("timeout"), "{error}");
            blocker.query_drop("ROLLBACK").await.unwrap();
        } else {
            blocker.query_drop("ROLLBACK").await.unwrap();
            let result = task.await.unwrap().unwrap();
            assert_eq!(result.deleted, 2);
            assert_eq!(result.absent, 0);
        }
        let count: u64 = admin
            .query_first(format!("SELECT COUNT(*) FROM {target}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count, if phase == 4 { 1 } else { 3 });
        if phase < 4 {
            admin
                .query_drop(format!("UPDATE {target} SET note='original' WHERE id=2"))
                .await
                .unwrap();
        }
    }
    let spec = DeleteSpec {
        require_empty_children: false,
        plan: job.plan.clone(),
        source_identity: identity,
    };
    let mut replay = crate::mysql_delete::DeleteSession::open(resolved.clone(), spec.clone())
        .await
        .unwrap();
    let result = replay.batch(&rows).await.unwrap();
    assert_eq!(result.deleted, 0);
    assert_eq!(result.absent, 2);
    drop(replay);
    let trigger = mysql::table(&resolved.database, &format!("tr_{name}"));
    admin
        .query_drop(format!(
            "CREATE TRIGGER {trigger} BEFORE DELETE ON {target} FOR EACH ROW SET @coldctl_trigger=1"
        ))
        .await
        .unwrap();
    let mut denied = crate::mysql_delete::DeleteSession::open(resolved.clone(), spec.clone())
        .await
        .unwrap();
    assert!(
        denied
            .batch(&rows)
            .await
            .unwrap_err()
            .to_string()
            .contains("triggers or incoming")
    );
    drop(denied);
    admin
        .query_drop(format!("DROP TRIGGER {trigger}"))
        .await
        .unwrap();
    admin
        .query_drop(format!(
            "ALTER TABLE {child} ADD FOREIGN KEY(id) REFERENCES {target}(id) ON DELETE CASCADE"
        ))
        .await
        .unwrap();
    let mut denied = crate::mysql_delete::DeleteSession::open(resolved, spec)
        .await
        .unwrap();
    assert!(
        denied
            .batch(&rows)
            .await
            .unwrap_err()
            .to_string()
            .contains("triggers or incoming")
    );
    drop(denied);
    admin
        .query_drop(format!("DROP TABLE {child}"))
        .await
        .unwrap();
    admin
        .query_drop(format!("DROP TABLE {target}"))
        .await
        .unwrap();
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
    let myisam = admin
        .query_drop(format!(
            "CREATE TABLE {qualified}(id BIGINT PRIMARY KEY,created_at DATE) ENGINE=MyISAM"
        ))
        .await;
    match myisam {
        Ok(()) => {
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
        Err(mysql_async::Error::Server(error)) if error.code == 3161 => {
            eprintln!(
                "MyISAM connector rejection subcase skipped: server disables that engine (3161)"
            );
        }
        Err(error) => panic!("cannot create MyISAM fixture: {error}"),
    }
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
