use std::{
    path::Path,
    process::{Command, Output},
};

fn run(path: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_coldctl"))
        .arg("--data-dir")
        .arg(path)
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn azure_cli_configuration_and_show_are_offline() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    assert!(run(&state, &["init"]).status.success());
    let added = run(
        &state,
        &[
            "destination",
            "add",
            "azure",
            "--name",
            "azure-test",
            "--account",
            "customeraccount",
            "--container",
            "archive",
            "--account-key-env",
            "COLDCTL_MISSING_AZURE_KEY",
            "--output",
            "json",
        ],
    );
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let saved: serde_json::Value = serde_json::from_slice(&added.stdout).unwrap();
    assert_eq!(saved["destination_type"], "azure");
    assert_eq!(
        saved["config"]["account_key_env"],
        "COLDCTL_MISSING_AZURE_KEY"
    );
    assert!(!state.join("staging").exists());
    let shown = run(
        &state,
        &["destination", "show", "azure-test", "--output", "json"],
    );
    assert!(shown.status.success());
    assert_eq!(
        saved,
        serde_json::from_slice::<serde_json::Value>(&shown.stdout).unwrap()
    );
    let checked = run(&state, &["destination", "test", "azure-test"]);
    assert!(!checked.status.success());
    assert!(String::from_utf8_lossy(&checked.stderr).contains("environment variable"));
    assert!(
        run(&state, &["destination", "remove", "azure-test"])
            .status
            .success()
    );
}

#[test]
fn s3_cli_configuration_and_show_are_offline() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    assert!(run(&state, &["init"]).status.success());
    let added = run(
        &state,
        &[
            "destination",
            "add",
            "s3",
            "--name",
            "remote",
            "--bucket",
            "customer-archive",
            "--region",
            "us-east-1",
            "--access-key-env",
            "COLDCTL_MISSING_S3_ACCESS",
            "--secret-key-env",
            "COLDCTL_MISSING_S3_SECRET",
            "--output",
            "json",
        ],
    );
    assert!(
        added.status.success(),
        "{}",
        String::from_utf8_lossy(&added.stderr)
    );
    let saved: serde_json::Value = serde_json::from_slice(&added.stdout).unwrap();
    assert_eq!(saved["destination_type"], "s3");
    assert_eq!(saved["config"]["bucket"], "customer-archive");
    assert!(!state.join("staging").exists());
    let shown = run(
        &state,
        &["destination", "show", "remote", "--output", "json"],
    );
    assert!(shown.status.success());
    assert_eq!(
        saved,
        serde_json::from_slice::<serde_json::Value>(&shown.stdout).unwrap()
    );
    assert!(
        !run(
            &state,
            &[
                "destination",
                "add",
                "s3",
                "--name",
                "unsafe",
                "--bucket",
                "customer-archive",
                "--region",
                "us-east-1",
                "--endpoint",
                "http://example.com",
                "--allow-http"
            ]
        )
        .status
        .success()
    );
    assert!(
        run(&state, &["destination", "remove", "remote"])
            .status
            .success()
    );
}

#[test]
fn destination_cli_lifecycle_with_json_and_spaces_in_path() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let root = temp.path().join("archive with spaces");
    assert!(run(&state, &["init"]).status.success());
    let add = run(
        &state,
        &[
            "destination",
            "add",
            "local",
            "--name",
            "disk",
            "--path",
            root.to_str().unwrap(),
            "--output",
            "json",
        ],
    );
    assert!(
        add.status.success(),
        "{}",
        String::from_utf8_lossy(&add.stderr)
    );
    let record: serde_json::Value = serde_json::from_slice(&add.stdout).unwrap();
    assert_eq!(record["destination_type"], "local");
    assert_eq!(record["path"], root.to_str().unwrap());
    assert!(!root.exists());
    for args in [
        vec!["destination", "list", "--output", "json"],
        vec!["destination", "--output", "json", "show", "disk"],
    ] {
        let output = run(&state, &args);
        assert!(output.status.success());
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap();
        assert!(!root.exists());
    }
    let checked = run(&state, &["destination", "test", "disk", "--output", "json"]);
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
    let result: serde_json::Value = serde_json::from_slice(&checked.stdout).unwrap();
    assert_eq!(result["probe_removed"], true);
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    std::fs::write(root.join("keep.parquet"), b"keep").unwrap();
    assert!(
        run(&state, &["destination", "remove", "disk"])
            .status
            .success()
    );
    assert_eq!(std::fs::read(root.join("keep.parquet")).unwrap(), b"keep");
    assert!(
        !run(&state, &["destination", "show", "disk"])
            .status
            .success()
    );
}

#[test]
fn help_validation_and_failed_access_have_nonzero_errors() {
    let temp = tempfile::tempdir().unwrap();
    for args in [
        vec!["destination", "--help"],
        vec!["destination", "add", "local", "--help"],
        vec!["destination", "test", "--help"],
    ] {
        assert!(run(temp.path(), &args).status.success());
    }
    assert!(
        !run(
            temp.path(),
            &["destination", "add", "local", "--name", "disk"]
        )
        .status
        .success()
    );
    assert!(!run(temp.path(), &["destination", "test"]).status.success());
    assert!(run(temp.path(), &["init"]).status.success());
    let file = temp.path().join("occupied");
    std::fs::write(&file, b"keep").unwrap();
    let args = [
        "destination",
        "add",
        "local",
        "--name",
        "disk",
        "--path",
        file.to_str().unwrap(),
    ];
    assert!(run(temp.path(), &args).status.success());
    assert!(!run(temp.path(), &args).status.success());
    let failed = run(
        temp.path(),
        &["destination", "test", "disk", "--output", "json"],
    );
    assert!(!failed.status.success());
    assert!(failed.stdout.is_empty());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("Storage"));
    assert!(String::from_utf8_lossy(&failed.stderr).contains("Check disk space"));
    assert_eq!(std::fs::read(file).unwrap(), b"keep");
}

#[test]
fn cloud_identity_modes_persist_without_secret_variables() {
    let temp = tempfile::tempdir().unwrap();
    assert!(run(temp.path(), &["init"]).status.success());
    for args in [
        vec![
            "destination",
            "add",
            "s3",
            "--name",
            "role",
            "--bucket",
            "customer-archive",
            "--region",
            "us-east-1",
            "--role-auth",
            "--output",
            "json",
        ],
        vec![
            "destination",
            "add",
            "azure",
            "--name",
            "identity",
            "--account",
            "customeraccount",
            "--container",
            "archive",
            "--managed-identity",
            "--managed-identity-client-id",
            "00000000-0000-0000-0000-000000000001",
            "--output",
            "json",
        ],
    ] {
        let added = run(temp.path(), &args);
        assert!(
            added.status.success(),
            "{}",
            String::from_utf8_lossy(&added.stderr)
        );
        let saved: serde_json::Value = serde_json::from_slice(&added.stdout).unwrap();
        let name = saved["name"].as_str().unwrap();
        let shown = run(
            temp.path(),
            &["destination", "show", name, "--output", "json"],
        );
        assert!(shown.status.success());
        assert_eq!(
            saved,
            serde_json::from_slice::<serde_json::Value>(&shown.stdout).unwrap()
        );
        assert_eq!(
            saved["config"][if name == "role" {
                "role_auth"
            } else {
                "managed_identity"
            }],
            true
        );
    }
}
