use coldctl_core::{
    monitoring::{AlertConfig, Monitor},
    monitoring_agent,
    paths::StatePaths,
    state,
};

#[test]
fn failures_between_scrapes_are_transactional_and_survive_monitor_restart() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(temp.path())).unwrap();
    state::initialize(&paths, "test").unwrap();
    let c = rusqlite::Connection::open(paths.database()).unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    c.execute("INSERT INTO jobs(id,policy_name,plan_json,status,started_at) VALUES(?1,'private-customer-name','{}','running','2026-10-14T00:00:00Z')",[&id]).unwrap();
    c.execute(
        "UPDATE jobs SET status='failed',failure_json='{}' WHERE id=?1",
        [&id],
    )
    .unwrap();
    c.execute(
        "UPDATE jobs SET status='running',failure_json=NULL WHERE id=?1",
        [&id],
    )
    .unwrap();
    c.execute_batch("BEGIN; UPDATE jobs SET status='failed'; ROLLBACK;")
        .unwrap();
    let monitor = Monitor::open(
        &temp.path().join("monitoring.sqlite"),
        "agent",
        AlertConfig::default(),
    )
    .unwrap();
    let metrics = monitoring_agent::sample(&paths, &monitor).unwrap();
    let history = monitor.history().unwrap();
    assert_eq!(history["events"].as_array().unwrap().len(), 2);
    assert_eq!(history["events"][0]["state"], "resolved");
    assert!(!metrics.contains("private-customer-name"));
    assert!(!metrics.contains(&id));
    let reopened = Monitor::open(
        &temp.path().join("monitoring.sqlite"),
        "agent",
        AlertConfig::default(),
    )
    .unwrap();
    monitoring_agent::sample(&paths, &reopened).unwrap();
    assert_eq!(history, reopened.history().unwrap());
    // Restoring an older source backup must not silently skip new transitions.
    c.execute_batch("DELETE FROM operational_events; UPDATE sqlite_sequence SET seq=0 WHERE name='operational_events';")
        .unwrap();
    assert!(monitoring_agent::sample(&paths, &reopened).is_err());
    assert_eq!(history, reopened.history().unwrap());
}

#[test]
fn journal_retention_exposes_a_gap_instead_of_claiming_complete_history() {
    let temp = tempfile::tempdir().unwrap();
    let paths = StatePaths::resolve(Some(temp.path())).unwrap();
    state::initialize(&paths, "test").unwrap();
    let mut c = rusqlite::Connection::open(paths.database()).unwrap();
    let tx = c.transaction().unwrap();
    let id = uuid::Uuid::new_v4().to_string();
    for _ in 0..10003 {
        tx.execute("INSERT INTO operational_events(kind,entity,failed,occurred_at) VALUES('job',?1,1,unixepoch())",[&id]).unwrap();
    }
    tx.commit().unwrap();
    let count: i64 = c
        .query_row("SELECT count(*) FROM operational_events", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 10000);
    let monitor = Monitor::open(
        &temp.path().join("monitoring.sqlite"),
        "agent",
        AlertConfig::default(),
    )
    .unwrap();
    let metrics = monitoring_agent::sample(&paths, &monitor).unwrap();
    assert!(metrics.contains("coldctl_agent_journal_gap 1"));
    assert!(
        monitor
            .metrics()
            .unwrap()
            .contains("coldctl_monitor_journal_gaps_total 1")
    );
}
