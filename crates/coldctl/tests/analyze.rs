use std::process::Command;

#[test]
fn analyze_parsing_and_missing_source_are_actionable() {
    let temp = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_coldctl"))
            .arg("--data-dir")
            .arg(temp.path())
            .args(args)
            .output()
            .unwrap()
    };
    let help = run(&["analyze", "--help"]);
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("--output"));
    assert!(!run(&["analyze"]).status.success());
    assert!(
        !run(&["analyze", "local", "--output", "invalid"])
            .status
            .success()
    );
    assert!(run(&["init"]).status.success());
    let missing = run(&["analyze", "missing"]);
    assert!(!missing.status.success());
    assert!(missing.stdout.is_empty());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("Source not found"));
    assert!(
        run(&[
            "source",
            "add",
            "postgres",
            "--name",
            "local",
            "--url-env",
            "COLDCTL_ANALYZE_SECRET"
        ])
        .status
        .success()
    );
    let error = Command::new(env!("CARGO_BIN_EXE_coldctl"))
        .arg("--data-dir")
        .arg(temp.path())
        .args(["analyze", "local", "--output", "json"])
        .env(
            "COLDCTL_ANALYZE_SECRET",
            "postgres://user:NEVER_PRINT_THIS@localhost/db?password=NEVER_PRINT_THIS",
        )
        .output()
        .unwrap();
    assert!(!error.status.success());
    assert!(error.stdout.is_empty());
    assert!(!String::from_utf8_lossy(&error.stderr).contains("NEVER_PRINT_THIS"));
}
