use std::process::{Command, Output};

fn run(path: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_coldctl"))
        .arg("--data-dir")
        .arg(path)
        .args(args)
        .env(
            "COLDCTL_CLI_SECRET_URL",
            "postgres://user:supersecret@localhost/db",
        )
        .output()
        .unwrap()
}

#[test]
fn source_commands_parse_and_keep_credential_values_out_of_state_and_output() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    assert!(run(path, &["init"]).status.success());
    for command in ["add", "list", "show", "test", "discover", "remove"] {
        assert!(run(path, &["source", command, "--help"]).status.success());
    }
    assert!(
        run(
            path,
            &[
                "source",
                "add",
                "postgres",
                "--name",
                "prod",
                "--url-env",
                "COLDCTL_CLI_SECRET_URL"
            ]
        )
        .status
        .success()
    );
    for args in [
        vec!["source", "show", "prod"],
        vec!["source", "list"],
        vec!["source", "show", "prod", "--output", "json"],
        vec!["source", "--output", "json", "list"],
    ] {
        let output = run(path, &args);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("supersecret"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("supersecret"));
        if args.contains(&"json") {
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap();
        }
    }
    let data = std::fs::read(path.join("state.db")).unwrap();
    assert!(!String::from_utf8_lossy(&data).contains("supersecret"));
    let duplicate = run(
        path,
        &[
            "source",
            "add",
            "postgres",
            "--name",
            "prod",
            "--url-env",
            "COLDCTL_CLI_SECRET_URL",
        ],
    );
    assert!(!duplicate.status.success());
    assert!(String::from_utf8_lossy(&duplicate.stderr).contains("already exists"));
    assert!(
        run(path, &["source", "remove", "prod", "--output", "json"])
            .status
            .success()
    );
    assert!(!run(path, &["source", "show", "prod"]).status.success());
    let list = run(path, &["source", "list", "--output", "json"]);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&list.stdout).unwrap(),
        serde_json::json!([])
    );
}

#[test]
fn invalid_source_arguments_and_urls_fail_without_echoing_secrets() {
    let temp = tempfile::tempdir().unwrap();
    for args in [
        vec!["source", "add", "postgres", "--name", "prod"],
        vec![
            "source",
            "add",
            "postgres",
            "--name",
            "prod",
            "--url",
            "postgres://user:supersecret@localhost/db",
        ],
        vec![
            "source",
            "add",
            "postgres",
            "--name",
            "prod",
            "--url",
            "postgres://user@localhost/db?password=supersecret",
        ],
    ] {
        let output = run(temp.path(), &args);
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("supersecret"));
        assert!(output.stdout.is_empty());
    }
    assert!(
        !run(
            temp.path(),
            &[
                "source",
                "add",
                "postgres",
                "--name",
                "prod",
                "--url-env",
                "URL",
                "--url",
                "postgres://user@localhost/db"
            ]
        )
        .status
        .success()
    );
    assert!(
        !run(
            temp.path(),
            &[
                "source",
                "add",
                "postgres",
                "--name",
                "prod",
                "--url-env",
                "URL",
                "--password-env",
                "PASSWORD"
            ]
        )
        .status
        .success()
    );
}

#[test]
fn credential_resolution_errors_are_sanitized() {
    let temp = tempfile::tempdir().unwrap();
    assert!(run(temp.path(), &["init"]).status.success());
    assert!(
        run(
            temp.path(),
            &[
                "source",
                "add",
                "postgres",
                "--name",
                "bad",
                "--url-env",
                "COLDCTL_BAD_URL"
            ]
        )
        .status
        .success()
    );
    for command in ["test", "discover"] {
        let output = Command::new(env!("CARGO_BIN_EXE_coldctl"))
            .arg("--data-dir")
            .arg(temp.path())
            .args(["source", command, "bad"])
            .env(
                "COLDCTL_BAD_URL",
                "postgres://user:supersecret@localhost/db?options=secret-token",
            )
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(!String::from_utf8_lossy(&output.stderr).contains("supersecret"));
        assert!(!String::from_utf8_lossy(&output.stderr).contains("secret-token"));
    }
}

#[test]
#[ignore = "requires COLDCTL_TEST_POSTGRES_URL pointing to a disposable PostgreSQL database"]
fn live_cli_test_and_discover_json() {
    let temp = tempfile::tempdir().unwrap();
    assert!(std::env::var("COLDCTL_TEST_POSTGRES_URL").is_ok());
    assert!(run(temp.path(), &["init"]).status.success());
    assert!(
        run(
            temp.path(),
            &[
                "source",
                "add",
                "postgres",
                "--name",
                "live",
                "--url-env",
                "COLDCTL_TEST_POSTGRES_URL"
            ]
        )
        .status
        .success()
    );
    for command in ["test", "discover"] {
        let output = run(
            temp.path(),
            &["source", command, "live", "--output", "json"],
        );
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let json: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        if command == "test" {
            assert!(json["database"].is_string());
        } else {
            assert!(json["schemas"].is_array());
            assert!(json["tables"].is_array());
        }
    }
    let analyzed = run(temp.path(), &["analyze", "live", "--output", "json"]);
    assert!(
        analyzed.status.success(),
        "{}",
        String::from_utf8_lossy(&analyzed.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&analyzed.stdout).unwrap();
    assert_eq!(report["analysis"]["method"], "metadata_only");
    assert!(report["analysis"]["tables"].is_array());
    let human = run(temp.path(), &["analyze", "live"]);
    assert!(human.status.success());
    if report["analysis"]["tables"].as_array().unwrap().is_empty() {
        assert!(String::from_utf8_lossy(&human.stdout).contains("No accessible tables found"));
    }
    assert!(
        run(temp.path(), &["source", "remove", "live"])
            .status
            .success()
    );
}

#[test]
fn mysql_source_configuration_cli_selects_engine_without_resolving_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    assert!(run(path, &["init"]).status.success());
    assert!(
        run(
            path,
            &[
                "source",
                "add",
                "mysql",
                "--name",
                "mysql",
                "--url-env",
                "ABSENT_MYSQL_URL",
                "--tls-ca-env",
                "ABSENT_MYSQL_CA"
            ]
        )
        .status
        .success()
    );
    let output = run(path, &["source", "show", "mysql", "--output", "json"]);
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["source_type"], "mysql");
    assert_eq!(value["connection"]["kind"], "mysql_url_env");
    assert!(
        !run(
            path,
            &[
                "source",
                "add",
                "postgres",
                "--name",
                "wrong",
                "--url",
                "mysql://u@localhost/db"
            ]
        )
        .status
        .success()
    );
    assert!(
        run(
            path,
            &[
                "source",
                "add",
                "mysql",
                "--name",
                "explicit",
                "--url",
                "mysql://u@localhost/db",
                "--password-env",
                "ABSENT_MYSQL_PASSWORD"
            ]
        )
        .status
        .success()
    );
}

#[test]
fn mongodb_source_configuration_cli_selects_engine_without_resolving_credentials() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path();
    assert!(run(path, &["init"]).status.success());
    assert!(
        run(
            path,
            &[
                "source",
                "add",
                "mongodb",
                "--name",
                "mongodb",
                "--url-env",
                "ABSENT_MONGODB_URL",
                "--tls-ca-env",
                "ABSENT_MONGODB_CA"
            ]
        )
        .status
        .success()
    );
    let output = run(path, &["source", "show", "mongodb", "--output", "json"]);
    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["source_type"], "mongodb");
    assert_eq!(value["connection"]["kind"], "mongodb_url_env");
    assert!(
        !run(
            path,
            &[
                "source",
                "add",
                "postgres",
                "--name",
                "wrong",
                "--url",
                "mongodb://u@localhost/db"
            ]
        )
        .status
        .success()
    );
    assert!(
        run(
            path,
            &[
                "source",
                "add",
                "mongodb",
                "--name",
                "explicit",
                "--url",
                "mongodb://u@localhost/db",
                "--password-env",
                "ABSENT_MONGODB_PASSWORD"
            ]
        )
        .status
        .success()
    );
}
