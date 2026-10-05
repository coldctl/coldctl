use std::process::{Command, Output};

fn run(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_coldctl"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn help_version_and_invalid_arguments() {
    for args in [&["--help"][..], &["--version"], &["init", "--help"]] {
        assert!(run(args).status.success());
    }
    assert!(!run(&["init", "--force"]).status.success());
    assert!(!run(&["source", "test"]).status.success());
}

#[test]
fn init_status_and_errors() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path().join("state");
    let path = dir.to_str().unwrap();
    let status = run(&["--data-dir", path, "status"]);
    assert!(status.status.success());
    assert!(!dir.exists());
    let first = run(&["init", "--data-dir", path]);
    assert!(
        first.status.success(),
        "{}",
        String::from_utf8_lossy(&first.stderr)
    );
    assert!(String::from_utf8_lossy(&first.stdout).contains("initialized successfully"));
    let second = run(&["--data-dir", path, "init"]);
    assert!(second.status.success());
    assert!(String::from_utf8_lossy(&second.stdout).contains("already initialized"));
    let status = run(&["status", "--data-dir", path]);
    assert!(status.status.success());
    assert!(String::from_utf8_lossy(&status.stdout).contains("Installation ID"));
    std::fs::write(dir.join("state.db"), b"corrupt").unwrap();
    let failed = run(&["init", "--data-dir", path]);
    assert!(!failed.status.success());
    assert!(failed.stdout.is_empty());
    assert!(String::from_utf8_lossy(&failed.stderr).contains("state.db"));
}
