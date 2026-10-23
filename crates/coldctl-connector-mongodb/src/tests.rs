use super::*;
use coldctl_core::{
    archive::{
        controls::{ExecutionControls, Shutdown},
        executor,
        planner::SourceStability,
        recovery, verifier,
    },
    paths::StatePaths,
    policy::model::PolicyConfig,
    source::{ConnectorSource, SourceConnection, config::TlsMode},
    state::{self, archive as store, destinations, sources},
};
use futures_util::TryStreamExt;
use mongodb::bson::{Bson, DateTime, Decimal128, RawDocumentBuf, doc, oid::ObjectId};

#[tokio::test]
#[ignore = "requires disposable MongoDB 8.0 replica set, deletion_writer and built connector"]
async fn mongodb_verified_deletion_rollback_replay_and_resume() {
    use coldctl_core::{archive::deletion, format::bson as codec, source::connector::DeleteSpec};
    let resolved = config("coldctl_mongo_source", "deletion_writer");
    let native = mongo::Mongo::connect(resolved.clone()).await.unwrap();
    let name = format!("delete_{}", uuid::Uuid::new_v4().simple());
    let target = native.db.collection::<mongodb::bson::Document>(&name);
    let docs=(1..=3).map(|n|doc!{"_id":ObjectId::from_bytes([n;12]),"created_at":DateTime::from_millis(0),"value":Bson::Int64(1),"note":"original"}).collect::<Vec<_>>();
    target.insert_many(docs.clone()).await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    sources::add(
        &paths,
        "source",
        connection("coldctl_mongo_source", "deletion_writer"),
    )
    .unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
    store::policy_create(
        &paths,
        "events",
        "source",
        "disk",
        PolicyConfig {
            schema: "coldctl_mongo_source".into(),
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
    let second = doc! {"_id":ObjectId::from_bytes([2;12])};
    // Numerically equal but different BSON type is a changed document.
    target
        .update_one(second.clone(), doc! {"$set":{"value":Bson::Int32(1)}})
        .await
        .unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    assert_eq!(target.count_documents(doc! {}).await.unwrap(), 3);
    target
        .update_one(
            second.clone(),
            doc! {"$set":{"value":Bson::Int64(1),"note":"x".repeat(32000)}},
        )
        .await
        .unwrap();
    assert!(deletion::run(&paths, &job.id, options(None)).await.is_err());
    assert_eq!(target.count_documents(doc! {}).await.unwrap(), 3);
    target.replace_one(second, docs[1].clone()).await.unwrap();
    let rows = docs[..2]
        .iter()
        .map(|d| codec::encode(&mongodb::bson::to_vec(d).unwrap(), "created_at").unwrap())
        .collect::<Vec<_>>();
    let mut direct = crate::deletion::DeleteSession::open(
        resolved.clone(),
        DeleteSpec {
            require_empty_children: false,
            plan: job.plan.clone(),
            source_identity: native.identity().into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(direct.batch(&rows).await.unwrap().deleted, 2);
    drop(direct);
    let paused = deletion::run(&paths, &job.id, options(Some(1)))
        .await
        .unwrap();
    assert_eq!(paused.rows_absent, 2);
    assert!(!paused.completed);
    let done = deletion::run(&paths, &job.id, options(None)).await.unwrap();
    assert!(done.completed);
    assert_eq!(done.rows_deleted, 1);
    assert_eq!(target.count_documents(doc! {}).await.unwrap(), 0);
    // Completion is terminal even if keys are reused later.
    target.insert_many(docs.clone()).await.unwrap();
    assert!(
        deletion::run(&paths, &job.id, options(None))
            .await
            .unwrap()
            .completed
    );
    assert_eq!(target.count_documents(doc! {}).await.unwrap(), 3);
    use coldctl_core::{scheduler::Worker, state::scheduler as queue};
    queue::enqueue(
        &paths,
        "events",
        None,
        queue::Options {
            delete_after_archive: true,
            source_stability: SourceStability::QuiescentCopy,
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
    assert_eq!(target.count_documents(doc! {}).await.unwrap(), 1);
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
    assert_eq!(target.count_documents(doc! {}).await.unwrap(), 0);
    let mut stale = crate::deletion::DeleteSession::open(
        resolved,
        DeleteSpec {
            require_empty_children: false,
            plan: job.plan.clone(),
            source_identity: native.identity().into(),
        },
    )
    .await
    .unwrap();
    target.drop().await.unwrap();
    target.insert_many(docs).await.unwrap();
    assert!(stale.batch(&rows).await.is_err());
    assert_eq!(target.count_documents(doc! {}).await.unwrap(), 3);
    target.drop().await.unwrap();
}

fn config(database: &str, user: &str) -> ResolvedConnection {
    ResolvedConnection {
        host: "localhost".into(),
        port: std::env::var("COLDCTL_TEST_MONGODB_PORT")
            .unwrap_or("15443".into())
            .parse()
            .unwrap(),
        database: database.into(),
        user: user.into(),
        tls: TlsMode::Require,
        password: Some("coldctl_mongodb_fixture_only".into()),
        ca_pem: Some(std::env::var("COLDCTL_TEST_MONGODB_CA").unwrap()),
    }
}
fn connection(database: &str, user: &str) -> SourceConnection {
    let port = config(database, user).port;
    SourceConnection::from_url(
        &format!("mongodb://{user}@localhost:{port}/{database}"),
        Some("COLDCTL_TEST_MONGODB_PASSWORD".into()),
    )
    .unwrap()
    .with_ca_env(Some("COLDCTL_TEST_MONGODB_CA".into()))
    .unwrap()
}
fn run_options(max_rows: Option<u64>) -> executor::RunOptions {
    executor::RunOptions {
        source_stability: SourceStability::QuiescentCopy,
        allow_repeat: true,
        minimum_free_bytes: 1,
        execution: ExecutionControls {
            max_rows,
            ..Default::default()
        },
        shutdown: Shutdown::default(),
        progress: None,
    }
}
fn restore_options(
    table: &str,
    max_batches: Option<u64>,
    validate_only: bool,
) -> coldctl_core::source::restore::Options {
    coldctl_core::source::restore::Options {
        target: "target".into(),
        schema: "coldctl_mongo_restore".into(),
        table: table.into(),
        confirm_separate_target: true,
        max_batches,
        shutdown: Shutdown::default(),
        validate_only,
        progress: None,
    }
}

#[tokio::test]
#[ignore = "requires disposable authenticated MongoDB 8.0 replica set, CA/password environment and native connector; see MONGODB_PHASE5.md"]
async fn mongodb_live_archive_resume_restore_and_rejections() {
    let admin = mongo::Mongo::connect(config("admin", "fixture_admin"))
        .await
        .unwrap();
    let source_db = admin.client.database("coldctl_mongo_source");
    let target_db = admin.client.database("coldctl_mongo_restore");
    // These are dedicated disposable databases; clear only this test's prior fixtures.
    for database in [&source_db, &target_db] {
        for name in database.list_collection_names().await.unwrap() {
            if name.starts_with("phase5_") || name.starts_with("_coldctl_restore_") {
                database
                    .collection::<mongodb::bson::Document>(&name)
                    .drop()
                    .await
                    .unwrap();
            }
        }
    }
    let name = format!("phase5_{}", uuid::Uuid::new_v4().simple());
    let source = source_db.collection::<RawDocumentBuf>(&name);
    let mut docs = Vec::new();
    for n in 1..=5u8 {
        let mut id = [255; 12];
        id[11] = n;
        let document = doc! {"_id": ObjectId::from_bytes(id), "created_at": DateTime::from_millis(0), "wide": i64::MAX, "decimal": "1.234567890123456789".parse::<Decimal128>().unwrap(), "binary": mongodb::bson::Binary {subtype: mongodb::bson::spec::BinarySubtype::UserDefined(128), bytes: vec![0,255,n]}, "nullable": Bson::Null, "nested": {"items": [Bson::Int32(1), Bson::Int64(2), Bson::Null]}, "nan": f64::from_bits(0x7ff8000000000042), "negative_zero": -0.0f64};
        docs.push(RawDocumentBuf::from_bytes(mongodb::bson::to_vec(&document).unwrap()).unwrap());
    }
    source.insert_many(docs.clone()).await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
    state::initialize(&paths, "test").unwrap();
    let src = connection(source_db.name(), "archive_reader");
    sources::add(&paths, "source", src.clone()).unwrap();
    sources::add(
        &paths,
        "target",
        connection(target_db.name(), "restore_writer"),
    )
    .unwrap();
    let connector = ConnectorSource::new(src.clone());
    mongo::MongoSource::new(src.resolve().unwrap())
        .test_connection()
        .await
        .unwrap();
    connector.test_connection().await.unwrap();
    let discovered = connector.discover().await.unwrap();
    assert!(discovered.tables.iter().any(|t| {
        t.name == name
            && t.columns
                .iter()
                .any(|c| c.name == "created_at" && c.archive_time_candidate)
    }));
    assert!(
        connector
            .analyze()
            .await
            .unwrap()
            .method
            .contains("one document")
    );
    let reader = mongo::Mongo::connect(config(source_db.name(), "archive_reader"))
        .await
        .unwrap();
    assert!(
        reader
            .db
            .collection::<mongodb::bson::Document>(&name)
            .insert_one(doc! {"denied": true})
            .await
            .is_err()
    );
    let mut wrong = config(source_db.name(), "archive_reader");
    wrong.password = Some("DO_NOT_LEAK_MONGO_PASSWORD".into());
    let error = mongo::MongoSource::new(wrong)
        .test_connection()
        .await
        .unwrap_err()
        .to_string();
    assert!(!error.contains("DO_NOT_LEAK"));
    let mut untrusted = config(source_db.name(), "archive_reader");
    untrusted.ca_pem = None;
    assert!(
        mongo::MongoSource::new(untrusted)
            .test_connection()
            .await
            .is_err()
    );
    destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
    store::policy_create(
        &paths,
        "documents",
        "source",
        "disk",
        PolicyConfig {
            schema: source_db.name().into(),
            table: name.clone(),
            time_column: "created_at".into(),
            older_than_days: 1,
            batch_size: 2,
            equals_column: None,
            equals_value: None,
        },
    )
    .unwrap();
    let paused = executor::run(&paths, "documents", run_options(Some(2)))
        .await
        .unwrap();
    assert_eq!(paused.status, store::JobStatus::Paused);
    assert_eq!(paused.rows_processed, 2);
    let completed = executor::resume_with_controls(
        &paths,
        &paused.id,
        true,
        ExecutionControls::default(),
        Shutdown::default(),
    )
    .await
    .unwrap();
    assert_eq!(completed.rows_processed, 5);
    assert_eq!(completed.objects_created, 3);
    verifier::verify(&paths, &completed.id).unwrap();
    let directory = completed.plan.destination_path.join(&completed.id);
    assert_eq!(
        recovery::verify_directory(&directory, None)
            .unwrap()
            .format_version,
        4
    );
    let fresh = StatePaths::resolve(Some(&temp.path().join("recovered"))).unwrap();
    state::initialize(&fresh, "test").unwrap();
    assert_eq!(
        recovery::import(&fresh, &directory, None)
            .unwrap()
            .rows_processed,
        5
    );
    // Commit executes, but a write-concern error makes its acknowledgement uncertain.
    // A fresh connector must recover the committed rows from its durable journal.
    let uncertain_name = format!("{name}_uncertain");
    let spec = || coldctl_core::source::connector::RestoreSpec {
        plan: completed.plan.clone(),
        objects_created: completed.objects_created,
        rows_processed: completed.rows_processed,
        schema: target_db.name().into(),
        table: uncertain_name.clone(),
        validate_only: false,
        manifest_hash: coldctl_connector_runtime::digest(&directory.join("manifest.json")).unwrap(),
    };
    let mut uncertain =
        restore::RestoreSession::open(config(target_db.name(), "restore_writer"), spec())
            .await
            .unwrap();
    assert_eq!(uncertain.begin(true).await.unwrap().rows, 0);
    admin.db.run_command(doc! {"configureFailPoint": "failCommand", "mode": "alwaysOn", "data": {"failCommands": ["commitTransaction"], "writeConcernError": {"code": 64, "errmsg": "fixture acknowledgement failure"}, "errorLabels": ["UnknownTransactionCommitResult"], "appName": "coldctl-mongodb"}}).await.unwrap();
    let rows: Vec<_> = docs[..2]
        .iter()
        .map(|d| coldctl_core::format::bson::encode(d.as_bytes(), "created_at").unwrap())
        .collect();
    let outcome = uncertain.apply(1, &rows).await;
    admin
        .db
        .run_command(doc! {"configureFailPoint": "failCommand", "mode": "off"})
        .await
        .unwrap();
    assert!(outcome.is_err());
    drop(uncertain);
    let mut recovered =
        restore::RestoreSession::open(config(target_db.name(), "restore_writer"), spec())
            .await
            .unwrap();
    assert_eq!(recovered.begin(true).await.unwrap().rows, 2);
    recovered.validate(&rows).await.unwrap();
    recovered.finish(false).await.unwrap();
    drop(recovered);
    target_db
        .collection::<mongodb::bson::Document>(&uncertain_name)
        .drop()
        .await
        .unwrap();
    let restored = coldctl_core::source::restore::run(
        &paths,
        &completed.id,
        restore_options(&name, Some(1), false),
    )
    .await
    .unwrap();
    assert_eq!(restored.rows, 2);
    // Fail the journal update after documents were inserted inside the transaction.
    admin.db.run_command(doc! {"configureFailPoint": "failCommand", "mode": {"times": 1}, "data": {"failCommands": ["update"], "errorCode": 121, "appName": "coldctl-mongodb"}}).await.unwrap();
    assert!(
        coldctl_core::source::restore::run(
            &paths,
            &completed.id,
            restore_options(&name, None, false)
        )
        .await
        .is_err()
    );
    assert_eq!(
        target_db
            .collection::<mongodb::bson::Document>(&name)
            .count_documents(doc! {})
            .await
            .unwrap(),
        2
    );
    let restored = coldctl_core::source::restore::run(
        &paths,
        &completed.id,
        restore_options(&name, None, false),
    )
    .await
    .unwrap();
    assert_eq!(restored.rows, 5);
    assert!(restored.values_validated);
    // Repeated invocation uses target progress rather than reinserting a committed batch.
    assert_eq!(
        coldctl_core::source::restore::run(
            &paths,
            &completed.id,
            restore_options(&name, None, false)
        )
        .await
        .unwrap()
        .rows,
        5
    );
    let actual: Vec<RawDocumentBuf> = target_db
        .collection::<RawDocumentBuf>(&name)
        .find(doc! {})
        .sort(doc! {"_id": 1})
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(
        actual.iter().map(|d| d.as_bytes()).collect::<Vec<_>>(),
        docs.iter().map(|d| d.as_bytes()).collect::<Vec<_>>()
    );
    target_db
        .collection::<mongodb::bson::Document>(&name)
        .update_one(
            doc! {"_id": docs[0].get_object_id("_id").unwrap()},
            doc! {"$set": {"wide": 0}},
        )
        .await
        .unwrap();
    assert!(
        coldctl_core::source::restore::run(
            &paths,
            &completed.id,
            restore_options(&name, None, true)
        )
        .await
        .is_err()
    );
    // Native preflight rejects missing/null/array Date fields and mixed keys, without filtering them out.
    for invalid in [
        doc! {"_id": ObjectId::new()},
        doc! {"_id": ObjectId::new(), "created_at": Bson::Null},
        doc! {"_id": ObjectId::new(), "created_at": [DateTime::now()]},
        doc! {"_id": "custom", "created_at": DateTime::now()},
        doc! {"_id": ObjectId::new(), "created_at": DateTime::now(), "large": "x".repeat(31*1024)},
    ] {
        let key = invalid.get("_id").unwrap().clone();
        source_db
            .collection::<mongodb::bson::Document>(&name)
            .insert_one(invalid)
            .await
            .unwrap();
        assert!(
            coldctl_core::archive::planner::plan(&paths, "documents")
                .await
                .is_err()
        );
        source_db
            .collection::<mongodb::bson::Document>(&name)
            .delete_one(doc! {"_id": key})
            .await
            .unwrap();
    }
    let interrupted = executor::run(&paths, "documents", run_options(Some(2)))
        .await
        .unwrap();
    source.drop().await.unwrap();
    source.insert_many(docs).await.unwrap();
    assert!(
        executor::resume_with_controls(
            &paths,
            &interrupted.id,
            true,
            ExecutionControls::default(),
            Shutdown::default()
        )
        .await
        .is_err()
    );
    // Ordinary-looking views and time-series collections cannot enter the archive path.
    let view = format!("{name}_view");
    source_db
        .run_command(doc! {"create": &view, "viewOn": &name, "pipeline": []})
        .await
        .unwrap();
    let native = mongo::Mongo::connect(config(source_db.name(), "archive_reader"))
        .await
        .unwrap();
    assert!(native.info(&view).await.is_err());
    let timeseries = format!("{name}_time");
    source_db
        .run_command(doc! {"create": &timeseries, "timeseries": {"timeField": "created_at"}})
        .await
        .unwrap();
    assert!(native.info(&timeseries).await.is_err());
    source_db
        .collection::<mongodb::bson::Document>(&view)
        .drop()
        .await
        .unwrap();
    source_db
        .collection::<mongodb::bson::Document>(&timeseries)
        .drop()
        .await
        .unwrap();
    source.drop().await.unwrap();
    target_db
        .collection::<mongodb::bson::Document>(&name)
        .drop()
        .await
        .unwrap();
}
