use std::process::Command;
#[test]
fn restore_requires_explicit_target_and_separation_confirmation() {
    let temp = tempfile::tempdir().unwrap();
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_coldctl"))
            .arg("--data-dir")
            .arg(temp.path().join("missing"))
            .args(args)
            .output()
            .unwrap()
    };
    for command in ["restore", "validate-restore"] {
        assert!(run(&["archive", command, "--help"]).status.success());
        let missing = run(&["archive", command, "job"]);
        assert!(!missing.status.success());
        assert!(missing.stdout.is_empty());
    }
    let missing = run(&[
        "archive", "restore", "job", "--target", "target", "--schema", "public", "--table",
        "restored",
    ]);
    assert!(!missing.status.success());
    assert!(String::from_utf8_lossy(&missing.stderr).contains("--confirm-separate-target"));
    assert!(!temp.path().join("missing").exists());
}
