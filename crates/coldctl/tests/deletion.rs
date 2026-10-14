use std::process::Command;

#[test]
fn deletion_requires_explicit_confirmations_before_opening_state() {
    let temp = tempfile::tempdir().unwrap();
    let state = temp.path().join("uninitialized");
    let run = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_coldctl"))
            .arg("--data-dir")
            .arg(&state)
            .args(args)
            .output()
            .unwrap()
    };
    assert!(run(&["archive", "delete", "--help"]).status.success());
    for args in [
        vec!["archive", "delete", "job"],
        vec!["archive", "delete", "job", "--confirm-delete"],
        vec!["archive", "delete", "job", "--confirm-original-source"],
        vec![
            "archive",
            "delete",
            "job",
            "--confirm-delete",
            "--confirm-original-source",
            "--max-batches",
            "0",
        ],
    ] {
        assert!(!run(&args).status.success());
        assert!(!state.exists());
    }
}
