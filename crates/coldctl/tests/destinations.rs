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
