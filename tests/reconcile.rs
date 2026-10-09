//! Reconciliation: what a restarted daemon rebuilds, what it refuses to guess about, and the
//! runtime events that make it look again in between.

mod support;

use std::path::Path;
use std::sync::Arc;

use blocklyd::manager::Precondition;
use blocklyd::protocol::{EnsureOutcome, WorkloadState};
use blocklyd::runtime::ContainerRuntime;
use blocklyd::store::RESTORE_COMPLETE;
use support::{fixture, id, manager_on, spec};
use tokio_util::sync::CancellationToken;

/// Events are how blocklyd hears of a crash between its own looks: this one reaches the
/// workload's state with nothing else looking (no stats, no resync).
#[tokio::test]
async fn a_crash_reaches_the_workload_through_the_event_stream() {
    let f = fixture("").await;
    let no_retries =
        support::spec_with(|v| v["restart"] = serde_json::json!({ "policy": "on-failure", "maxRetries": 0 }));
    f.manager.ensure(id("w"), no_retries, Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    let cancel = CancellationToken::new();
    let follower = tokio::spawn(blocklyd::reconcile::follow_events(f.manager.clone(), cancel.clone()));
    // Lets it subscribe first: the stream, like Docker's, doesn't replay what came before.
    tokio::task::yield_now().await;
    f.fake.crash("blockly-test-w", 3, false);
    let view = support::eventually("the crash, heard of by its event alone", async || {
        let view = f.manager.view(&id("w")).unwrap();
        (view.state == WorkloadState::Crashed, view)
    })
    .await;
    assert_eq!(view.exit.as_ref().unwrap().code, 3);
    assert!(view.last_failure_at.is_some());
    cancel.cancel();
    follower.await.unwrap();
}

#[tokio::test]
async fn a_restarted_daemon_finds_everything_and_hands_out_no_port_twice() {
    let f = fixture("").await;
    let a = f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    f.manager.ensure(id("b"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("a"), None).await.unwrap();
    let a_ports: Vec<u16> = a.workload.ports.iter().map(|p| p.host_port).collect();

    // A new process: nothing in memory, the same disk and the same runtime.
    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    let report = restarted.reconcile(true).await;
    assert!(report.error.is_none(), "{report:?}");
    assert_eq!(report.workloads, 2);
    let a_view = restarted.view(&id("a")).unwrap();
    assert_eq!(a_view.state, WorkloadState::Running, "still running: blocklyd never owned the process");
    assert_eq!(a_view.ports.iter().map(|p| p.host_port).collect::<Vec<_>>(), a_ports);
    assert_eq!(restarted.view(&id("b")).unwrap().state, WorkloadState::Created);

    let c = restarted.ensure(id("c"), spec(), Precondition::None, None).await.unwrap();
    for p in &c.workload.ports {
        assert!(!a_ports.contains(&p.host_port), "port {} was already a's", p.host_port);
    }
    assert_eq!(restarted.ports_usage().0, 6);
}

#[tokio::test]
async fn a_lost_state_directory_is_rebuilt_from_container_labels() {
    let f = fixture("").await;
    let made = f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("a"), None).await.unwrap();
    let record = f.manager.store.workload_dir(&id("a")).join("workload.json");
    let original: serde_json::Value = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    std::fs::remove_file(&record).unwrap();

    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    let report = restarted.reconcile(true).await;
    assert_eq!(report.adopted, 1);
    let view = restarted.view(&id("a")).unwrap();
    assert_eq!(view.state, WorkloadState::Running);
    assert_eq!(view.spec_digest, made.workload.spec_digest);
    assert_eq!(view.ports, made.workload.ports);
    assert!(view.issues.iter().any(|i| i.code == "record_rebuilt"), "rebuilt, and says so");
    let rebuilt: serde_json::Value = serde_json::from_slice(&std::fs::read(&record).unwrap()).unwrap();
    for field in ["id", "generation", "spec", "specDigest", "ports", "containerName", "createdAt"] {
        assert_eq!(rebuilt[field], original[field], "{field}");
    }
    // And nothing is recreated or restarted: the same container keeps running.
    assert_eq!(f.fake.count_calls("create "), 1);
    assert_eq!(f.fake.count_calls("start "), 1);
}

#[tokio::test]
async fn a_create_interrupted_after_the_container_existed_is_completed() {
    let f = fixture("").await;
    f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    // As if blocklyd died between creating the container and recording it.
    let path = f.manager.store.workload_dir(&id("a")).join("workload.json");
    let mut record: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    record["phase"] = "creating".into();
    record["containerId"] = serde_json::Value::Null;
    std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();

    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    let report = restarted.reconcile(true).await;
    assert_eq!(report.adopted, 1);
    let view = restarted.view(&id("a")).unwrap();
    assert_eq!(view.state, WorkloadState::Created);
    assert!(view.locate.container_id.is_some());
    let again = restarted.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    assert_eq!(again.outcome, EnsureOutcome::Unchanged);
}

#[tokio::test]
async fn a_create_interrupted_before_the_container_existed_is_finished_by_the_next_put() {
    let f = fixture("").await;
    f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    f.fake.vanish("blockly-test-a");
    let path = f.manager.store.workload_dir(&id("a")).join("workload.json");
    let mut record: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    record["phase"] = "creating".into();
    std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();

    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    restarted.reconcile(true).await;
    let view = restarted.view(&id("a")).unwrap();
    assert_eq!(view.state, WorkloadState::Creating);
    assert!(view.issues.iter().any(|i| i.code == "create_incomplete"));
    let done = restarted.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    assert_eq!(done.workload.state, WorkloadState::Created);
    assert!(done.workload.issues.is_empty());
}

#[tokio::test]
async fn a_container_the_runtime_lost_is_reported_not_recreated() {
    let f = fixture("").await;
    f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    f.fake.vanish("blockly-test-a");
    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    restarted.reconcile(true).await;
    let view = restarted.view(&id("a")).unwrap();
    assert_eq!(view.state, WorkloadState::Missing);
    assert!(view.issues.iter().any(|i| i.code == "container_missing"));
    assert_eq!(f.fake.count_calls("create "), 1, "blocklyd doesn't recreate on its own: it holds no secrets");
    assert!(!view.ports.is_empty(), "its ports stay held for when it's asked back");
}

#[tokio::test]
async fn orphaned_data_is_reported_and_never_touched() {
    let f = fixture("").await;
    f.manager.reconcile(true).await;
    let orphan = f.manager.store.workloads_dir().join("nobody").join("data");
    std::fs::create_dir_all(&orphan).unwrap();
    std::fs::write(orphan.join("level.dat"), b"someone's world").unwrap();
    let report = f.manager.reconcile(true).await;
    assert!(report.error.is_none());
    let host = f.manager.node_status().await;
    assert!(host.issues.iter().any(|i| i.code == "orphan_data"));
    assert!(orphan.join("level.dat").exists());
}

#[tokio::test]
async fn another_deployments_containers_are_invisible() {
    let f = fixture("").await;
    f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    // A second daemon's config (another deployment) over the same runtime.
    let dir = tempfile::tempdir().unwrap();
    let other =
        support::manager_on(dir.path(), f.fake.clone() as Arc<dyn ContainerRuntime>, "deployment_id = \"prod\"\n");
    let report = other.reconcile(true).await;
    assert_eq!(report.workloads, 0, "staging's containers are not production's to adopt");
    assert!(other.view(&id("a")).is_err());
}

#[tokio::test]
async fn two_records_claiming_one_port_are_both_flagged() {
    let f = fixture("").await;
    let a = f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    f.manager.ensure(id("b"), spec(), Precondition::None, None).await.unwrap();
    // Corrupt b's record so it claims a's game port.
    let path = f.manager.store.workload_dir(&id("b")).join("workload.json");
    let mut record: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    record["ports"][0]["hostPort"] = a.workload.ports[0].host_port.into();
    std::fs::write(&path, serde_json::to_vec(&record).unwrap()).unwrap();

    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    restarted.reconcile(true).await;
    for w in ["a", "b"] {
        let view = restarted.view(&id(w)).unwrap();
        assert!(view.issues.iter().any(|i| i.code == "port_conflict"), "{w}: {:?}", view.issues);
    }
    let b = restarted.view(&id("b")).unwrap();
    assert!(b.issues.iter().any(|i| i.code == "port_mismatch"), "the record no longer matches b's container");
}

/// A world in `dir` that says `what`.
fn world_saying(dir: &Path, what: &str) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("level.dat"), what).unwrap();
}

fn says(dir: &Path) -> String {
    std::fs::read_to_string(dir.join("level.dat")).unwrap()
}

#[tokio::test]
async fn a_restore_cut_short_once_its_data_was_complete_is_finished_at_startup() {
    let f = fixture("").await;
    for w in ["renamed", "exchanged"] {
        f.manager.ensure(id(w), spec(), Precondition::None, None).await.unwrap();
    }
    let store = &f.manager.store;
    // Two renames (a filesystem that can't exchange), cut between them: the old data is in the
    // trash and no data/ is left, only the complete new data beside it.
    let restoring = store.restoring_dir(&id("renamed"));
    world_saying(&restoring, "restored");
    std::fs::write(restoring.join(RESTORE_COMPLETE), b"").unwrap();
    std::fs::rename(store.data_dir(&id("renamed")), store.trash_dir().join("renamed-replaced.1")).unwrap();
    // One exchange, cut before what it replaced went to the trash: that is still beside the data.
    world_saying(&store.data_dir(&id("exchanged")), "restored");
    std::fs::write(store.data_dir(&id("exchanged")).join(RESTORE_COMPLETE), b"").unwrap();
    world_saying(&store.restoring_dir(&id("exchanged")), "old");

    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    assert!(restarted.reconcile(true).await.error.is_none());
    for w in ["renamed", "exchanged"] {
        let data = restarted.store.data_dir(&id(w));
        assert_eq!(says(&data), "restored", "{w}");
        assert!(!data.join(RESTORE_COMPLETE).exists(), "{w}");
        assert!(!restarted.store.restoring_dir(&id(w)).exists(), "{w}");
        let issues = restarted.view(&id(w)).unwrap().issues;
        assert!(issues.iter().any(|i| i.code == "restore_finished"), "{w}: and says so: {issues:?}");
    }
    // What the exchange replaced went where a finished restore puts it.
    let trashed: Vec<_> = std::fs::read_dir(restarted.store.trash_dir())
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with("exchanged-replaced."))
        .collect();
    assert_eq!(trashed.len(), 1, "{trashed:?}");
    assert_eq!(says(&trashed[0]), "old");
}

#[tokio::test]
async fn a_restore_cut_short_before_it_replaced_anything_is_removed_at_startup() {
    let f = fixture("").await;
    for w in ["unpacking", "unswapped"] {
        f.manager.ensure(id(w), spec(), Precondition::None, None).await.unwrap();
        world_saying(&f.manager.store.data_dir(&id(w)), "mine");
        world_saying(&f.manager.store.restoring_dir(&id(w)), "theirs");
    }
    // One was still unpacking; the other was complete, about to be swapped in.
    std::fs::write(f.manager.store.restoring_dir(&id("unswapped")).join(RESTORE_COMPLETE), b"").unwrap();

    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    assert!(restarted.reconcile(true).await.error.is_none());
    for w in ["unpacking", "unswapped"] {
        assert_eq!(says(&restarted.store.data_dir(&id(w))), "mine", "{w}: the data is as it was");
        assert!(!restarted.store.restoring_dir(&id(w)).exists(), "{w}");
        assert!(restarted.view(&id(w)).unwrap().issues.is_empty(), "{w}: the restore never happened");
    }
}

#[tokio::test]
async fn what_a_crash_left_in_the_spool_and_of_snapshots_is_cleared_at_startup() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    let store = &f.manager.store;
    let snapshot = |s: &str| blocklyd::ids::SnapshotId::parse(s).unwrap();
    let (unfinished, damaged) = ("0b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10", "1b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10");
    // A download and a snapshot's copy cut short, and a snapshot whose description was damaged.
    std::fs::create_dir_all(store.spool_dir()).unwrap();
    std::fs::write(store.spool_dir().join("w-0b6f.tar.gz"), b"half an archive").unwrap();
    for s in [unfinished, damaged] {
        world_saying(&store.prepare_snapshot(&id("w"), &snapshot(s)).unwrap(), "copied");
    }
    std::fs::write(store.snapshot_dir(&id("w"), &snapshot(damaged)).join("snapshot.json"), b"{not json").unwrap();

    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    assert!(restarted.reconcile(true).await.error.is_none());
    assert_eq!(std::fs::read_dir(store.spool_dir()).unwrap().count(), 0, "the spool is empty");
    assert!(!store.snapshot_dir(&id("w"), &snapshot(unfinished)).exists());
    assert!(store.snapshot_dir(&id("w"), &snapshot(damaged)).join("data/level.dat").exists(), "left for a human");
    let issues = restarted.node_status().await.issues;
    assert!(issues.iter().any(|i| i.code == "unreadable_snapshot" && i.detail.contains(damaged)), "{issues:?}");
    // Only from disk: a pass on a schedule never looks.
    std::fs::write(store.spool_dir().join("w-1b6f.tar.gz"), b"a download under way").unwrap();
    assert!(restarted.reconcile(false).await.error.is_none());
    assert!(store.spool_dir().join("w-1b6f.tar.gz").exists());
}

#[tokio::test]
async fn reconciliation_with_the_runtime_down_keeps_the_records() {
    let f = fixture("").await;
    f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    f.fake.set_available(false);
    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    let report = restarted.reconcile(true).await;
    assert!(report.error.is_some());
    assert!(!restarted.reconciled());
    assert_eq!(restarted.view(&id("a")).unwrap().state, WorkloadState::Unknown, "known, but not observable");
    f.fake.set_available(true);
    let report = restarted.reconcile(false).await;
    assert!(report.error.is_none());
    assert_eq!(restarted.view(&id("a")).unwrap().state, WorkloadState::Created);
}
