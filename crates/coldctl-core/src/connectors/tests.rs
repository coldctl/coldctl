use super::*;
use coldctl_connector_protocol::capability::*;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::{Value, json};
use std::collections::BTreeSet;

fn canonical(value: &Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut serializer =
        serde_json::Serializer::with_formatter(&mut bytes, olpc_cjson::CanonicalFormatter::new());
    value.serialize(&mut serializer).unwrap();
    bytes
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
struct Fixture {
    temp: tempfile::TempDir,
    paths: StatePaths,
    key: Ed25519KeyPair,
    key_id: String,
    root: Value,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let paths = StatePaths::resolve(Some(&temp.path().join("state"))).unwrap();
        crate::state::initialize(&paths, "test").unwrap();
        let key = Ed25519KeyPair::from_seed_unchecked(&[42; 32]).unwrap(); // Test key; never a shipped root.
        let public = json!({"keytype":"ed25519","scheme":"ed25519","keyval":{"public":hex(key.public_key().as_ref())}});
        let key_id = format!("{:x}", Sha256::digest(canonical(&public)));
        let mut roles = serde_json::Map::new();
        for role in ["root", "targets", "snapshot", "timestamp"] {
            roles.insert(role.into(), json!({"keyids":[key_id],"threshold":1}));
        }
        let root = json!({"_type":"root","spec_version":"1.0.0","version":1,"expires":"2099-01-01T00:00:00Z","consistent_snapshot":false,"keys":{key_id.clone():public},"roles":roles});
        let fixture = Self {
            temp,
            paths,
            key,
            key_id,
            root,
        };
        std::fs::create_dir_all(fixture.repo().join("metadata")).unwrap();
        std::fs::create_dir_all(fixture.repo().join("targets")).unwrap();
        std::fs::write(
            fixture.repo().join("root.json"),
            fixture.sign(&fixture.root),
        )
        .unwrap();
        std::fs::write(
            fixture.repo().join("metadata/1.root.json"),
            fixture.sign(&fixture.root),
        )
        .unwrap();
        fixture
    }
    fn repo(&self) -> PathBuf {
        self.temp.path().join("repository")
    }
    fn sign(&self, value: &Value) -> Vec<u8> {
        canonical(
            &json!({"signed":value,"signatures":[{"keyid":self.key_id,"sig":hex(self.key.sign(&canonical(value)).as_ref())}]}),
        )
    }
    fn configure(&self) {
        let path = self.repo().join("root.json");
        let hash = coldctl_connector_runtime::digest(&path).unwrap();
        configure_registry(&self.paths, self.repo().to_str().unwrap(), &path, &hash).unwrap();
    }
    fn package(&self, version: &str, binary: &[u8]) -> Package {
        let executable = if cfg!(windows) {
            "connector.exe"
        } else {
            "connector"
        };
        let mut files = Vec::new();
        for (name, bytes) in [
            (executable, binary),
            ("LICENSE", b"test license".as_slice()),
            ("config-schema.json", b"{}".as_slice()),
            ("dependencies.json", b"[]".as_slice()),
        ] {
            let sha256 = format!("{:x}", Sha256::digest(bytes));
            let target = format!("{sha256}-{name}");
            std::fs::write(self.repo().join("targets").join(&target), bytes).unwrap();
            files.push(model::FileEntry {
                name: name.into(),
                target,
                length: bytes.len() as u64,
                sha256,
            });
        }
        Package {
            hello: Hello {
                protocol: CURRENT,
                connector: ConnectorPin {
                    id: "postgres".into(),
                    version: version.into(),
                    sha256: files[0].sha256.clone(),
                },
                features: BTreeSet::from(["legacy-postgres-v2".into()]),
                required_host_features: BTreeSet::from(["legacy-postgres-v2".into()]),
                capabilities: Capabilities {
                    source: Some(SourceCapabilities {
                        analyze: true,
                        encodings: BTreeSet::from(["postgres-text-v2".into()]),
                        cursor_versions: BTreeSet::from([1]),
                    }),
                    sink: None,
                    restore: Some(RestoreCapabilities {
                        transactional_checkpoint: true,
                        value_validation: true,
                    }),
                },
            },
            platform: model::platform().into(),
            agent: ">=0.1.0, <1.0.0".into(),
            entrypoint: executable.into(),
            files,
            requirements: vec![],
        }
    }
    fn publish(&self, version: u64, packages: &[Package], revoked: Vec<String>, expired: bool) {
        let catalog = Catalog {
            format: 1,
            packages: packages.to_vec(),
            revoked,
        };
        let bytes = serde_json::to_vec(&catalog).unwrap();
        std::fs::write(self.repo().join("targets/catalog.json"), &bytes).unwrap();
        let mut targets = serde_json::Map::new();
        targets.insert("catalog.json".into(), descriptor(&bytes, None));
        for p in packages {
            for file in &p.files {
                let bytes = std::fs::read(self.repo().join("targets").join(&file.target)).unwrap();
                targets.insert(file.target.clone(), descriptor(&bytes, None));
            }
        }
        let expires = if expired {
            "2000-01-01T00:00:00Z"
        } else {
            "2099-01-01T00:00:00Z"
        };
        let target=self.sign(&json!({"_type":"targets","spec_version":"1.0.0","version":version,"expires":expires,"targets":targets}));
        let snapshot=self.sign(&json!({"_type":"snapshot","spec_version":"1.0.0","version":version,"expires":expires,"meta":{"targets.json":descriptor(&target,Some(version))}}));
        let timestamp=self.sign(&json!({"_type":"timestamp","spec_version":"1.0.0","version":version,"expires":expires,"meta":{"snapshot.json":descriptor(&snapshot,Some(version))}}));
        for (name, bytes) in [
            ("targets.json", target),
            ("snapshot.json", snapshot),
            ("timestamp.json", timestamp),
        ] {
            std::fs::write(self.repo().join("metadata").join(name), bytes).unwrap();
        }
    }
}
fn descriptor(bytes: &[u8], version: Option<u64>) -> Value {
    let mut v =
        json!({"length":bytes.len(),"hashes":{"sha256":format!("{:x}",Sha256::digest(bytes))}});
    if let Some(version) = version {
        v["version"] = json!(version)
    }
    v
}

#[tokio::test]
async fn signed_install_upgrade_pin_rollback_and_leases() {
    let f = Fixture::new();
    let old = f.package("1.0.0", b"old executable");
    let new = f.package("2.0.0", b"new executable");
    f.publish(1, &[old.clone(), new.clone()], vec![], false);
    f.configure();
    install(&f.paths, "postgres", "1.0.0", None, true)
        .await
        .unwrap();
    install(&f.paths, "postgres", "2.0.0", None, true)
        .await
        .unwrap();
    assert_eq!(
        select(&f.paths, "postgres", None).unwrap().package.pin(),
        new.pin()
    );
    let lease = select(&f.paths, "postgres", Some(old.pin())).unwrap();
    assert!(remove(&f.paths, "postgres", "1.0.0").is_err());
    drop(lease);
    assert!(activate(&f.paths, "postgres", "1.0.0", false).is_err());
    activate(&f.paths, "postgres", "1.0.0", true).unwrap();
    assert_eq!(
        select(&f.paths, "postgres", None).unwrap().package.pin(),
        old.pin()
    );
    remove(&f.paths, "postgres", "2.0.0").unwrap();
    assert_eq!(list(&f.paths).unwrap().len(), 1);
}
#[tokio::test]
async fn tampered_payload_and_incomplete_install_preserve_previous_default() {
    let f = Fixture::new();
    let old = f.package("1.0.0", b"old");
    let new = f.package("2.0.0", b"new");
    f.publish(1, &[old.clone(), new.clone()], vec![], false);
    f.configure();
    install(&f.paths, "postgres", "1.0.0", None, true)
        .await
        .unwrap();
    std::fs::write(f.repo().join("targets").join(&new.files[0].target), b"BAD").unwrap();
    assert!(
        install(&f.paths, "postgres", "2.0.0", None, true)
            .await
            .is_err()
    );
    assert_eq!(
        select(&f.paths, "postgres", None).unwrap().package.pin(),
        old.pin()
    );
    let orphan = home(&f.paths).join("staging/interrupted");
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::write(orphan.join("connector.exe"), b"incomplete").unwrap();
    assert_eq!(list(&f.paths).unwrap().len(), 1);
    let selected = select(&f.paths, "postgres", None).unwrap();
    let path = selected.path.clone();
    drop(selected);
    std::fs::write(path, b"BAD").unwrap();
    assert!(select(&f.paths, "postgres", None).is_err());
    assert!(!list(&f.paths).unwrap()[0].healthy);
}
#[tokio::test]
async fn signatures_expiry_rollback_and_revocation_fail_closed() {
    let f = Fixture::new();
    let package = f.package("1.0.0", b"binary");
    f.publish(2, std::slice::from_ref(&package), vec![], false);
    f.configure();
    install(&f.paths, "postgres", "1.0.0", None, true)
        .await
        .unwrap();
    f.publish(1, std::slice::from_ref(&package), vec![], false);
    assert!(refresh(&f.paths, None).await.is_err());
    f.publish(3, std::slice::from_ref(&package), vec![], true);
    assert!(refresh(&f.paths, None).await.is_err());
    f.publish(
        4,
        std::slice::from_ref(&package),
        vec![package.pin().sha256.clone()],
        false,
    );
    refresh(&f.paths, None).await.unwrap();
    assert!(select(&f.paths, "postgres", Some(package.pin())).is_err());
    f.publish(5, std::slice::from_ref(&package), vec![], false);
    refresh(&f.paths, None).await.unwrap();
    assert!(select(&f.paths, "postgres", None).is_err());
    let path = f.repo().join("metadata/timestamp.json");
    let mut bad: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    bad["signed"]["version"] = json!(100);
    std::fs::write(path, serde_json::to_vec(&bad).unwrap()).unwrap();
    assert!(refresh(&f.paths, None).await.is_err());
}
#[tokio::test]
async fn wrong_platform_protocol_and_path_are_rejected_before_publication() {
    for change in 0..4 {
        let f = Fixture::new();
        let mut p = f.package("1.0.0", b"binary");
        match change {
            0 => {
                p.platform = if cfg!(windows) {
                    "x86_64-unknown-linux-gnu"
                } else {
                    "x86_64-pc-windows-msvc"
                }
                .into()
            }
            1 => p.hello.protocol.major = 99,
            2 => p.files[0].name = "../escape".into(),
            _ => p.agent = ">=99.0.0".into(),
        }
        f.publish(1, &[p], vec![], false);
        f.configure();
        assert!(
            install(&f.paths, "postgres", "1.0.0", None, true)
                .await
                .is_err()
        );
        assert!(list(&f.paths).unwrap().is_empty());
    }
}
#[tokio::test]
async fn offline_bundle_uses_existing_trust_and_runs_without_registry() {
    let f = Fixture::new();
    let p = f.package("1.0.0", b"binary");
    f.publish(1, std::slice::from_ref(&p), vec![], false);
    f.configure();
    let output = f.temp.path().join("offline");
    bundle(&f.paths, "postgres", "1.0.0", &output)
        .await
        .unwrap();
    std::fs::rename(f.repo(), f.temp.path().join("unavailable")).unwrap();
    install(&f.paths, "postgres", "1.0.0", Some(&output), true)
        .await
        .unwrap();
    assert_eq!(
        select(&f.paths, "postgres", None).unwrap().package.pin(),
        p.pin()
    );
    let foreign = Fixture::new();
    assert!(
        install(&foreign.paths, "postgres", "1.0.0", Some(&output), true)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn approved_orphan_publication_is_reconciled_without_overwriting() {
    let f = Fixture::new();
    let p = f.package("1.0.0", b"binary");
    f.publish(1, std::slice::from_ref(&p), vec![], false);
    f.configure();
    install(&f.paths, "postgres", "1.0.0", None, true)
        .await
        .unwrap();
    {
        let s = Store::open(&f.paths).unwrap();
        s.db.execute("UPDATE packages SET installed=0", []).unwrap();
    }
    install(&f.paths, "postgres", "1.0.0", None, true)
        .await
        .unwrap();
    assert!(select(&f.paths, "postgres", None).is_ok());
}

#[tokio::test]
async fn pinned_resumable_jobs_block_removal_but_completed_archives_do_not() {
    let f = Fixture::new();
    let p = f.package("1.0.0", b"binary");
    f.publish(1, std::slice::from_ref(&p), vec![], false);
    f.configure();
    install(&f.paths, "postgres", "1.0.0", None, true)
        .await
        .unwrap();
    let plan = json!({"connector_pin":p.pin(),"policy":{"id":"policy","name":"p","source":"source","destination":"disk","config":{"schema":"public","table":"t","time_column":"created","older_than_days":30,"batch_size":2,"equals_column":null,"equals_value":null},"created_at":"test"},"destination_path":f.temp.path(),"cutoff_utc":"2026-01-01T00:00:00Z","primary_key":"id","table_oid":1,"columns":[],"estimated_rows":null,"delete":false,"warnings":[]});
    let db = Connection::open(f.paths.database()).unwrap();
    db.execute("INSERT INTO jobs(id,policy_name,plan_json,status,started_at) VALUES('job','p',?1,'paused','test')",[plan.to_string()]).unwrap();
    assert!(remove(&f.paths, "postgres", "1.0.0").is_err());
    db.execute("UPDATE jobs SET status='completed' WHERE id='job'", [])
        .unwrap();
    remove(&f.paths, "postgres", "1.0.0").unwrap();
    assert_eq!(crate::state::archive::job_list(&f.paths).unwrap().len(), 1);
}

#[tokio::test]
async fn root_rotation_requires_old_and_new_keys_and_persists_trust() {
    let mut f = Fixture::new();
    let p = f.package("1.0.0", b"binary");
    f.publish(1, std::slice::from_ref(&p), vec![], false);
    f.configure();
    refresh(&f.paths, None).await.unwrap();
    let key = Ed25519KeyPair::from_seed_unchecked(&[43; 32]).unwrap();
    let public = json!({"keytype":"ed25519","scheme":"ed25519","keyval":{"public":hex(key.public_key().as_ref())}});
    let id = format!("{:x}", Sha256::digest(canonical(&public)));
    let mut next = f.root.clone();
    next["version"] = json!(2);
    next["keys"] = json!({id.clone():public});
    for role in ["root", "targets", "snapshot", "timestamp"] {
        next["roles"][role] = json!({"keyids":[id],"threshold":1});
    }
    let mut signed: Value = serde_json::from_slice(&f.sign(&next)).unwrap();
    signed["signatures"]
        .as_array_mut()
        .unwrap()
        .push(json!({"keyid":id,"sig":hex(key.sign(&canonical(&next)).as_ref())}));
    std::fs::write(f.repo().join("metadata/2.root.json"), canonical(&signed)).unwrap();
    f.key = key;
    f.key_id = id;
    f.root = next;
    f.publish(2, std::slice::from_ref(&p), vec![], false);
    refresh(&f.paths, None).await.unwrap();
    {
        let store = Store::open(&f.paths).unwrap();
        let root: Vec<u8> = store
            .db
            .query_row("SELECT root FROM registry", [], |r| r.get(0))
            .unwrap();
        let root: Value = serde_json::from_slice(&root).unwrap();
        assert_eq!(root["signed"]["version"], 2);
    }
    // A replay of the original metadata is rejected even after a process/store restart.
    f.publish(1, std::slice::from_ref(&p), vec![], false);
    assert!(refresh(&f.paths, None).await.is_err());
}

#[test]
fn trust_bootstrap_never_accepts_a_bundle_key_implicitly() {
    let f = Fixture::new();
    assert!(
        configure_registry(
            &f.paths,
            f.repo().to_str().unwrap(),
            &f.repo().join("root.json"),
            &"0".repeat(64)
        )
        .is_err()
    );
    f.configure();
    let other = f.temp.path().join("other");
    std::fs::create_dir(&other).unwrap();
    let hash = coldctl_connector_runtime::digest(&f.repo().join("root.json")).unwrap();
    assert!(
        configure_registry(
            &f.paths,
            other.to_str().unwrap(),
            &f.repo().join("root.json"),
            &hash
        )
        .is_err()
    );
}

#[tokio::test]
#[ignore = "requires COLDCTL_SIGNED_TEST_BINARY pointing to the built native connector"]
async fn installed_native_connector_runs_with_a_package_lease() {
    native_package("COLDCTL_SIGNED_TEST_BINARY", "postgres").await;
}
#[tokio::test]
#[ignore = "requires built MySQL connector"]
async fn installed_mysql_native_connector_runs_with_a_package_lease() {
    native_package("COLDCTL_SIGNED_TEST_MYSQL_BINARY", "mysql").await;
}
#[tokio::test]
#[ignore = "requires built MongoDB connector"]
async fn installed_mongodb_native_connector_runs_with_a_package_lease() {
    native_package("COLDCTL_SIGNED_TEST_MONGODB_BINARY", "mongodb").await;
}
async fn native_package(variable: &str, engine: &str) {
    use coldctl_connector_runtime::Client;
    let path = PathBuf::from(std::env::var_os(variable).expect("native test binary"));
    let probe = Client::launch(
        path.clone(),
        engine,
        "probe".into(),
        crate::source::connector::engine_features(engine),
    )
    .await
    .unwrap();
    let hello = probe.hello.clone();
    drop(probe);
    let f = Fixture::new();
    let mut package = f.package(&hello.connector.version, &std::fs::read(path).unwrap());
    package.hello = hello;
    f.publish(1, std::slice::from_ref(&package), vec![], false);
    f.configure();
    install(&f.paths, engine, &package.pin().version, None, true)
        .await
        .unwrap();
    let selected = select(&f.paths, engine, None).unwrap();
    let mut client = Client::launch(
        selected.path,
        engine,
        "installed".into(),
        crate::source::connector::engine_features(engine),
    )
    .await
    .unwrap();
    assert_eq!(client.hello, selected.package.hello);
    client.retain_package_lease(selected.lease);
    let connection = crate::source::config::ResolvedConnection {
        host: "localhost".into(),
        port: 5432,
        database: "test".into(),
        user: "test".into(),
        tls: crate::source::config::TlsMode::Disable,
        password: None,
        ca_pem: None,
    };
    client
        .call::<_, ()>(
            crate::source::connector::Request::Configure(connection),
            15_000,
            false,
        )
        .await
        .unwrap();
    assert!(remove(&f.paths, engine, &package.pin().version).is_err());
    drop(client);
}
