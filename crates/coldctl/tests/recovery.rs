use std::process::{Command, Output};
fn run(state: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_coldctl"))
        .arg("--data-dir")
        .arg(state)
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn backup_json_is_clean_and_restorable_without_overwriting() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("state");
    let backup = temp.path().join("backup.db");
    assert!(run(&state, &["init"]).status.success());
    let output = run(
        &state,
        &[
            "state",
            "backup",
            "--to",
            backup.to_str().unwrap(),
            "--output",
            "json",
        ],
    );
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["archives_included"], false);
    assert!(report["bytes"].as_u64().unwrap() > 0);
    let duplicate = run(
        &state,
        &[
            "state",
            "backup",
            "--to",
            backup.to_str().unwrap(),
            "--output",
            "json",
        ],
    );
    assert!(!duplicate.status.success());
    assert!(duplicate.stdout.is_empty());
    let fresh = temp.path().join("fresh");
    std::fs::create_dir(&fresh).unwrap();
    std::fs::copy(&backup, fresh.join("state.db")).unwrap();
    let status = run(&fresh, &["status"]);
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains("Installation ID"));
}

#[test]
fn standalone_errors_do_not_create_state_and_import_requires_acknowledgement() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("missing state");
    let directory = temp.path().join("missing archive");
    for command in ["inspect", "verify-directory"] {
        let output = run(
            &state,
            &[
                "archive",
                command,
                directory.to_str().unwrap(),
                "--output",
                "json",
            ],
        );
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert!(!state.exists());
    }
    let output = run(&state, &["archive", "import", directory.to_str().unwrap()]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("--accept-untrusted-manifest"));
    assert!(!state.exists());
}
