use arrow_array::{Array, BooleanArray, Int64Array, StringArray};
use coldctl_core::{
    archive::batch::{ArchiveColumn, DataBatch},
    destination::{ArchiveDestination, local::LocalDestination},
    format::parquet::encode,
    paths::StatePaths,
    policy::model::PolicyConfig,
    source::SourceConnection,
    state::{self, archive as store, destinations, sources},
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

fn config() -> PolicyConfig {
    PolicyConfig {
        schema: "public".into(),
        table: "orders".into(),
        time_column: "created_at".into(),
        older_than_days: 365,
        batch_size: 2,
        equals_column: Some("status".into()),
        equals_value: Some("completed".into()),
    }
}

#[test]
fn policy_lifecycle_pins_references_and_migration_preserves_existing_state() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(temp.path())).unwrap();
    state::initialize(&paths, "test").unwrap();
    let installation = state::status(&paths).unwrap();
    sources::add(
        &paths,
        "source",
        SourceConnection::from_url_env("UNUSED_URL".into()).unwrap(),
    )
    .unwrap();
    destinations::add_local(&paths, "disk", &temp.path().join("archives")).unwrap();
    let conn = rusqlite::Connection::open(paths.database()).unwrap();
    conn.execute_batch(
        "DROP TABLE archive_objects; DROP TABLE archive_checkpoints; DROP TABLE jobs; DROP TABLE policies; DELETE FROM schema_migrations WHERE version>=4;",
    )
    .unwrap();
    assert!(store::policy_list(&paths).is_err());
    state::initialize(&paths, "new").unwrap();
    assert_eq!(state::status(&paths).unwrap(), installation);
    let policy = store::policy_create(&paths, "orders", "source", "disk", config()).unwrap();
    assert_eq!(store::policy_show(&paths, "orders").unwrap().id, policy.id);
    assert_eq!(store::policy_list(&paths).unwrap().len(), 1);
    assert!(store::policy_create(&paths, "orders", "source", "disk", config()).is_err());
    assert!(sources::remove(&paths, "source").is_err());
    assert!(destinations::remove(&paths, "disk").is_err());
    assert!(!temp.path().join("archives").exists());
    assert!(store::job_list(&paths).unwrap().is_empty());
    store::policy_remove(&paths, "orders").unwrap();
    sources::remove(&paths, "source").unwrap();
    destinations::remove(&paths, "disk").unwrap();
    assert!(store::policy_remove(&paths, "missing").is_err());
}

#[test]
fn retention_and_batch_limits_are_validated() {
    let mut p = config();
    p.batch_size = 0;
    assert!(p.validate().is_err());
    p.batch_size = 1001;
    assert!(p.validate().is_err());
    p.batch_size = 1;
    p.older_than_days = -1;
    assert!(p.validate().is_err());
    p.older_than_days = 365;
    p.equals_value = None;
    assert!(p.validate().is_err());
    assert_eq!(
        coldctl_core::source::postgres_archive::quote_identifier("odd\"name;--"),
        "\"odd\"\"name;--\""
    );
}

#[test]
fn storage_preflight_checks_capacity_and_preserves_existing_files() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("archives");
    let dest = LocalDestination::new(root.clone()).unwrap();
    assert!(dest.preflight(1).unwrap() > 0);
    std::fs::write(root.join("existing"), b"keep").unwrap();
    assert!(dest.preflight(u64::MAX).is_err());
    assert_eq!(std::fs::read(root.join("existing")).unwrap(), b"keep");
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
    let not_directory = temp.path().join("file");
    std::fs::write(&not_directory, b"keep").unwrap();
    assert!(
        LocalDestination::new(not_directory)
            .unwrap()
            .preflight(1)
            .is_err()
    );
}

#[test]
fn parquet_round_trip_preserves_types_nulls_unicode_and_exact_decimals() {
    let columns = vec![
        ArchiveColumn {
            name: "id".into(),
            postgres_type: "int8".into(),
            nullable: false,
        },
        ArchiveColumn {
            name: "active".into(),
            postgres_type: "bool".into(),
            nullable: true,
        },
        ArchiveColumn {
            name: "amount".into(),
            postgres_type: "numeric".into(),
            nullable: false,
        },
        ArchiveColumn {
            name: "note".into(),
            postgres_type: "text".into(),
            nullable: true,
        },
    ];
    let batch = DataBatch {
        last_key: 2,
        rows: vec![
            vec![
                Some(i64::MIN.to_string()),
                Some("true".into()),
                Some("12345678901234567890.123456789".into()),
                Some("hello 🌍\nquoted \"text\"".into()),
            ],
            vec![Some("2".into()), None, Some("0.00".into()), None],
        ],
    };
    let file = encode(&columns, &batch).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(file.reopen().unwrap())
        .unwrap()
        .build()
        .unwrap();
    let data = reader.next().unwrap().unwrap();
    assert_eq!(data.num_rows(), 2);
    assert_eq!(
        data.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        i64::MIN
    );
    let booleans = data
        .column(1)
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    assert!(booleans.value(0));
    assert!(booleans.is_null(1));
    let decimals = data
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(decimals.value(0), "12345678901234567890.123456789");
    assert_eq!(
        data.schema().field(2).metadata()["coldctl.postgres_type"],
        "numeric"
    );
    assert_eq!(
        data.column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "hello 🌍\nquoted \"text\""
    );
}

#[test]
fn publishing_is_no_clobber_and_verification_detects_tampering() {
    let temp = tempfile::tempdir().unwrap();
    let destination = LocalDestination::new(temp.path().join("output")).unwrap();
    let file = temp.path().join("encoded");
    std::fs::write(&file, b"archive-content").unwrap();
    let object = destination.put_file("job/batch.parquet", &file).unwrap();
    destination.verify(&object).unwrap();
    assert!(destination.put_file("job/batch.parquet", &file).is_err());
    for key in [
        "../escape",
        "/absolute",
        "job/../escape",
        "C:/escape",
        "job\\escape",
        "job/:stream",
    ] {
        assert!(destination.put_file(key, &file).is_err());
    }
    std::fs::write(temp.path().join("output/job/batch.parquet"), b"tampered").unwrap();
    assert!(destination.verify(&object).is_err());
    assert_eq!(
        std::fs::read_dir(temp.path().join("output/job"))
            .unwrap()
            .count(),
        1
    );
}
