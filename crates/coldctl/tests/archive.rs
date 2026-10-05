use std::process::Command;
#[test]
fn policy_and_job_cli_parse_and_persist_without_touching_storage() {
    let temp = tempfile::tempdir().unwrap();
    let output = temp.path().join("archive files");
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_coldctl"))
            .arg("--data-dir")
            .arg(temp.path())
            .args(args)
            .output()
            .unwrap()
    };
    for args in [
        vec!["policy", "create", "--help"],
        vec!["archive", "run", "--help"],
        vec!["jobs", "show", "--help"],
        vec!["archive", "resume", "--help"],
        vec!["archive", "verify", "--help"],
        vec!["jobs", "cancel", "--help"],
        vec!["jobs", "retry", "--help"],
    ] {
        assert!(run(&args).status.success());
    }
    assert!(run(&["init"]).status.success());
    let missing_contract = run(&["archive", "run", "orders"]);
    assert!(!missing_contract.status.success());
    assert!(String::from_utf8_lossy(&missing_contract.stderr).contains("--source-stability"));
    assert!(
        !run(&["archive", "run", "orders", "--source-stability", "mutable"])
            .status
            .success()
    );
    assert!(
        !run(&[
            "archive",
            "run",
            "orders",
            "--source-stability",
            "immutable-rows",
            "--min-free-bytes",
            "0"
        ])
        .status
        .success()
    );
    for args in [
        ["archive", "resume", "missing"],
        ["archive", "verify", "missing"],
        ["jobs", "cancel", "missing"],
        ["jobs", "retry", "missing"],
    ] {
        assert!(!run(&args).status.success());
    }
    assert!(
        run(&[
            "source",
            "add",
            "postgres",
            "--name",
            "source",
            "--url-env",
            "UNUSED_URL"
        ])
        .status
        .success()
    );
    assert!(
        run(&[
            "destination",
            "add",
            "local",
            "--name",
            "disk",
            "--path",
            output.to_str().unwrap()
        ])
        .status
        .success()
    );
    let args = [
        "policy",
        "create",
        "--name",
        "orders",
        "--source",
        "source",
        "--destination",
        "disk",
        "--schema",
        "public",
        "--table",
        "orders",
        "--time-column",
        "created_at",
        "--older-than-days",
        "365",
        "--equals-column",
        "status",
        "--equals-value",
        "completed",
        "--batch-size",
        "2",
        "--output",
        "json",
    ];
    let policy = run(&args);
    assert!(
        policy.status.success(),
        "{}",
        String::from_utf8_lossy(&policy.stderr)
    );
    assert!(!run(&args).status.success());
    let json: serde_json::Value = serde_json::from_slice(&policy.stdout).unwrap();
    assert_eq!(json["config"]["batch_size"], 2);
    assert!(run(&["policy", "show", "orders"]).status.success());
    assert!(!run(&["source", "remove", "source"]).status.success());
    assert!(!run(&["archive", "plan", "missing"]).status.success());
    let jobs = run(&["jobs", "list", "--output", "json"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&jobs.stdout).unwrap(),
        serde_json::json!([])
    );
    assert!(run(&["policy", "remove", "orders"]).status.success());
    assert!(!output.exists());
}
