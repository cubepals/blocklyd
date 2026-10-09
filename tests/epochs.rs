//! Placement epochs: the fencing token that keeps an old copy of a workload from running once
//! the control plane has placed it anew. Against the in-memory runtime.

mod support;

use blocklyd::manager::{NodeError, Precondition};
use blocklyd::protocol::{EnsureOutcome, ExecRequest, WorkloadState};
use blocklyd::runtime::ContainerStatus;
use support::{fixture, id, manager_on, spec};
use tokio_util::sync::CancellationToken;

fn container(f: &support::Fixture) -> blocklyd::runtime::fake::FakeContainer {
    let name = f.manager.view(&id("w")).unwrap().locate.container_name;
    f.fake.container(&name).expect("container exists")
}

#[tokio::test]
async fn a_placed_copy_answers_only_to_its_own_epoch() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    assert_eq!(f.manager.view(&id("w")).unwrap().epoch, Some(3));
    assert_eq!(container(&f).spec.labels.get("blocklyd.epoch").map(String::as_str), Some("3"));

    let missing = f.manager.start(id("w"), None).await.unwrap_err();
    assert!(matches!(missing, NodeError::EpochRequired { current: 3 }), "{missing:?}");
    let stale = f.manager.start(id("w"), Some(2)).await.unwrap_err();
    assert!(matches!(stale, NodeError::StaleEpoch { asked: 2, current: Some(3) }), "{stale:?}");
    let ahead = f.manager.start(id("w"), Some(4)).await.unwrap_err();
    assert!(matches!(ahead, NodeError::EpochAhead { asked: 4, .. }), "a newer epoch must place it first: {ahead:?}");
    assert_eq!(f.fake.count_calls("start "), 0, "nothing reached the runtime");

    assert!(f.manager.start(id("w"), Some(3)).await.unwrap().changed);
    let exec = ExecRequest { command: vec!["true".into()], timeout_seconds: 5 };
    let err = f.manager.exec(id("w"), exec, None, Some(2)).await.unwrap_err();
    assert!(matches!(err, NodeError::StaleEpoch { .. }), "{err:?}");
}

#[tokio::test]
async fn a_newer_epoch_may_always_tear_an_older_copy_down() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    f.manager.start(id("w"), Some(3)).await.unwrap();
    let stopped = f.manager.stop(id("w"), None, Some(7)).await.unwrap();
    assert!(stopped.changed);
    let err = f.manager.stop(id("w"), None, Some(2)).await.unwrap_err();
    assert!(matches!(err, NodeError::StaleEpoch { .. }), "an older caller can't: {err:?}");
    let deleted = f.manager.delete(id("w"), blocklyd::protocol::DataDisposition::Keep, None, Some(7)).await.unwrap();
    assert!(deleted.existed);
}

#[tokio::test]
async fn a_new_epoch_takes_the_copy_over_keeping_data_and_ports() {
    let f = fixture("").await;
    let first = f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    let marker = std::path::Path::new(&first.workload.locate.data_dir).join("level.dat");
    std::fs::write(&marker, b"world").unwrap();

    // Same spec, newer epoch: made again so its labels say which placement it runs for.
    let again = f.manager.ensure(id("w"), spec(), Precondition::None, Some(5)).await.unwrap();
    assert_eq!(again.outcome, EnsureOutcome::Replaced);
    assert_eq!(again.workload.epoch, Some(5));
    assert_eq!(again.workload.generation, first.workload.generation + 1);
    assert_eq!(again.workload.ports[0].host_port, first.workload.ports[0].host_port);
    assert_eq!(container(&f).spec.labels.get("blocklyd.epoch").map(String::as_str), Some("5"));
    assert_eq!(std::fs::read(&marker).unwrap(), b"world");

    // And the same epoch again is the usual no-op.
    let same = f.manager.ensure(id("w"), spec(), Precondition::None, Some(5)).await.unwrap();
    assert_eq!(same.outcome, EnsureOutcome::Unchanged);
    let stale = f.manager.ensure(id("w"), spec(), Precondition::None, Some(4)).await.unwrap_err();
    assert!(matches!(stale, NodeError::StaleEpoch { .. }), "{stale:?}");
}

#[tokio::test]
async fn a_fence_the_disk_couldnt_record_still_stops_the_copy() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    f.manager.start(id("w"), Some(3)).await.unwrap();
    // The record can't be written (a full or read-only disk), even by root: a directory, not empty,
    // stands where its new version is renamed to.
    let record = f.dir.path().join("state/workloads/w/workload.json");
    std::fs::remove_file(&record).unwrap();
    std::fs::create_dir_all(record.join("in-the-way")).unwrap();
    assert!(f.manager.fence(id("w"), 4).await.is_err(), "the failure is reported");
    assert_eq!(f.fake.count_calls("disable_restart "), 1);
    assert_eq!(f.manager.view(&id("w")).unwrap().state, WorkloadState::Fenced);
    assert_eq!(f.fake.count_calls("stop "), 1, "stopped all the same");
    let err = f.manager.start(id("w"), Some(3)).await.unwrap_err();
    assert!(matches!(err, NodeError::Superseded { by: 4 }), "{err:?}");
}

/// Resyncs as often as blocklyd allows, so a test sees one within seconds.
const QUICK_RESYNC: &str = "[intervals]\nresync_seconds = 5\n";

/// Fences `w` at `by` while the runtime doesn't answer: recorded, but the copy keeps running.
async fn fence_unapplied(f: &support::Fixture, w: &str, by: u64) {
    f.fake.set_available(false);
    assert!(f.manager.fence(id(w), by).await.is_err(), "the runtime didn't answer");
    f.fake.set_available(true);
    assert_eq!(f.manager.view(&id(w)).unwrap().superseded_by, Some(by), "recorded all the same");
    assert_eq!(f.fake.container(&format!("blockly-test-{w}")).unwrap().status, ContainerStatus::Running);
}

#[tokio::test]
async fn a_fence_the_runtime_couldnt_apply_is_applied_at_the_next_resync() {
    let f = fixture(QUICK_RESYNC).await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    f.manager.start(id("w"), Some(3)).await.unwrap();
    fence_unapplied(&f, "w", 4).await;
    // The control plane doesn't send it again (the node reports it recorded): the node applies it.
    let cancel = CancellationToken::new();
    tokio::spawn(blocklyd::reconcile::periodic(f.manager.clone(), cancel.clone()));
    support::until(&f.manager, "w", "the copy fenced", |v| v.state == WorkloadState::Fenced).await;
    cancel.cancel();
    assert_eq!(f.fake.count_calls("stop "), 1);
    assert_eq!(f.fake.count_calls("disable_restart "), 1);
    assert!(matches!(f.manager.start(id("w"), Some(3)).await, Err(NodeError::Superseded { by: 4 })));
}

#[tokio::test]
async fn the_resync_never_stops_a_copy_that_isnt_superseded() {
    let f = fixture(QUICK_RESYNC).await;
    for (w, epoch) in [("current", 5), ("old", 3)] {
        f.manager.ensure(id(w), spec(), Precondition::None, Some(epoch)).await.unwrap();
        f.manager.start(id(w), Some(epoch)).await.unwrap();
    }
    assert!(!f.manager.fence(id("current"), 4).await.unwrap().changed, "a late fence");
    fence_unapplied(&f, "old", 4).await;
    let cancel = CancellationToken::new();
    tokio::spawn(blocklyd::reconcile::periodic(f.manager.clone(), cancel.clone()));
    // The resync ran, and fenced what it should.
    support::until(&f.manager, "old", "the old copy fenced", |v| v.state == WorkloadState::Fenced).await;
    cancel.cancel();
    assert_eq!(f.manager.view(&id("current")).unwrap().state, WorkloadState::Running);
    assert_eq!(f.fake.count_calls("stop blockly-test-current"), 0);
    assert_eq!(f.fake.count_calls("disable_restart blockly-test-current"), 0);
}

#[tokio::test]
async fn a_fenced_copy_stops_and_never_starts_again() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    f.manager.start(id("w"), Some(3)).await.unwrap();

    let fenced = f.manager.fence(id("w"), 4).await.unwrap();
    assert!(fenced.changed && fenced.stopped);
    assert_eq!(fenced.workload.state, WorkloadState::Fenced);
    assert_eq!(fenced.workload.superseded_by, Some(4));
    assert_eq!(f.fake.count_calls("disable_restart "), 1, "Docker can't bring it back either");

    for asked in [Some(3), Some(4)] {
        let err = f.manager.start(id("w"), asked).await.unwrap_err();
        assert!(matches!(err, NodeError::Superseded { by: 4 } | NodeError::EpochAhead { .. }), "{err:?}");
    }
    let exec = ExecRequest { command: vec!["true".into()], timeout_seconds: 5 };
    assert!(matches!(f.manager.exec(id("w"), exec, None, Some(3)).await, Err(NodeError::Superseded { by: 4 })));
    let err = f.manager.ensure(id("w"), spec(), Precondition::None, Some(4)).await.unwrap_err();
    assert!(matches!(err, NodeError::Superseded { by: 4 }), "the superseding epoch itself runs elsewhere: {err:?}");
    assert_eq!(f.fake.count_calls("start "), 1, "only the first start reached the runtime");

    // Repeated or late fences change nothing.
    assert!(!f.manager.fence(id("w"), 4).await.unwrap().changed);
    assert!(!f.manager.fence(id("w"), 2).await.unwrap().changed);
    // The data stays until the control plane deletes it, which a newer epoch may do.
    let deleted = f.manager.delete(id("w"), blocklyd::protocol::DataDisposition::Keep, None, Some(4)).await.unwrap();
    assert!(deleted.existed);
}

#[tokio::test]
async fn a_fenced_copy_survives_a_restart_as_fenced() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    f.manager.fence(id("w"), 4).await.unwrap();
    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    restarted.reconcile(true).await;
    let view = restarted.view(&id("w")).unwrap();
    assert_eq!(view.state, WorkloadState::Fenced);
    assert!(matches!(restarted.start(id("w"), Some(3)).await, Err(NodeError::Superseded { by: 4 })));
}

#[tokio::test]
async fn a_fenced_copy_can_be_placed_here_again_under_a_newer_epoch() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    f.manager.fence(id("w"), 4).await.unwrap();
    let back = f.manager.ensure(id("w"), spec(), Precondition::None, Some(5)).await.unwrap();
    assert_eq!(back.outcome, EnsureOutcome::Replaced);
    assert_eq!((back.workload.epoch, back.workload.superseded_by), (Some(5), None));
    assert!(f.manager.start(id("w"), Some(5)).await.unwrap().changed);
}

#[tokio::test]
async fn a_late_fence_leaves_the_current_copy_alone() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(5)).await.unwrap();
    f.manager.start(id("w"), Some(5)).await.unwrap();
    let late = f.manager.fence(id("w"), 4).await.unwrap();
    assert!(!late.changed && !late.stopped);
    assert_eq!(late.workload.state, WorkloadState::Running);
    assert_eq!(f.fake.count_calls("disable_restart "), 0);
}

#[tokio::test]
async fn the_epoch_is_rebuilt_from_labels_when_the_state_directory_is_lost() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(9)).await.unwrap();
    std::fs::remove_file(f.manager.store.workloads_dir().join("w").join("workload.json")).unwrap();
    let restarted = manager_on(f.dir.path(), f.fake.clone(), "");
    restarted.reconcile(true).await;
    assert_eq!(restarted.view(&id("w")).unwrap().epoch, Some(9));
    assert!(matches!(restarted.start(id("w"), Some(8)).await, Err(NodeError::StaleEpoch { .. })));
}

#[tokio::test]
async fn workloads_without_epochs_keep_the_single_node_protocol() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    assert!(f.manager.start(id("w"), None).await.unwrap().changed);
    // A first epoch adopts it (a placement taking over a workload made before epochs).
    let adopted = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    assert_eq!(adopted.workload.epoch, Some(1));
    assert!(matches!(f.manager.stop(id("w"), None, None).await, Err(NodeError::EpochRequired { current: 1 })));
}
