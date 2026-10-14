use std::process::Command;

#[test]
fn recurring_cli_requires_delete_authorization_and_persists_stop() {
    let temp = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_coldctl"))
            .arg("--data-dir")
            .arg(temp.path())
            .args(args)
            .output()
            .unwrap()
    };
    for args in [
        vec!["init"],
        vec![
            "source",
            "add",
            "postgres",
            "--name",
            "pg",
            "--url-env",
            "UNUSED_RECURRING_URL",
        ],
        vec![
            "destination",
            "add",
            "s3",
            "--name",
            "s3",
            "--bucket",
            "test-archives",
            "--region",
            "us-east-1",
        ],
        vec![
            "policy",
            "create",
            "--name",
            "old",
            "--source",
            "pg",
            "--destination",
            "s3",
            "--schema",
            "public",
            "--table",
            "events",
            "--time-column",
            "created_at",
            "--older-than-days",
            "90",
        ],
    ] {
        let output = run(&args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut args = vec![
        "agent",
        "scheduler",
        "recurring",
        "create",
        "old",
        "--every-seconds",
        "60",
        "--source-stability",
        "immutable-rows",
        "--confirm-original-source",
    ];
    assert!(!run(&args).status.success());
    args.push("--delete-after-archive");
    let output = run(&args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let id = value["id"].as_str().unwrap();
    assert_eq!(value["status"], "active");
    assert!(
        run(&["agent", "scheduler", "recurring", "stop", id])
            .status
            .success()
    );
    assert!(
        run(&["agent", "scheduler", "run", "--once"])
            .status
            .success()
    );
    let output = run(&["agent", "scheduler", "list"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!([])
    );
    assert!(!temp.path().join("staging").exists());
}

#[test]
fn scheduler_cli_persists_delayed_work_and_requires_resume_authorization() {
    let temp = tempfile::tempdir().unwrap();
    let destination = temp.path().join("archives");
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_coldctl"))
            .arg("--data-dir")
            .arg(temp.path())
            .args(args)
            .output()
            .unwrap()
    };
    for args in [
        vec!["init"],
        vec![
            "source",
            "add",
            "postgres",
            "--name",
            "source",
            "--url-env",
            "SCHEDULER_UNUSED_URL",
        ],
        vec![
            "destination",
            "add",
            "local",
            "--name",
            "disk",
            "--path",
            destination.to_str().unwrap(),
        ],
        vec![
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
        ],
    ] {
        let result = run(&args);
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let args = [
        "agent",
        "scheduler",
        "enqueue",
        "orders",
        "--source-stability",
        "immutable-rows",
    ];
    assert!(!run(&args).status.success());
    let mut args = args.to_vec();
    args.extend(["--confirm-original-source", "--delay-seconds", "3600"]);
    let result = run(&args);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let entry: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    let id = entry["id"].as_str().unwrap();
    assert_eq!(entry["status"], "pending");
    assert_eq!(entry["options"]["execution"]["max_duration_seconds"], 60);
    assert!(!run(&args).status.success());
    assert!(
        run(&["agent", "scheduler", "run", "--once"])
            .status
            .success()
    );
    let result = run(&["agent", "scheduler", "show", id]);
    let saved: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(saved["status"], "pending");
    assert!(saved["job_id"].is_null());
    assert!(run(&["agent", "scheduler", "cancel", id]).status.success());
    assert!(
        !run(&[
            "agent",
            "scheduler",
            "retry",
            id,
            "--confirm-original-source"
        ])
        .status
        .success()
    );
    assert!(!temp.path().join("archives").exists());
}
