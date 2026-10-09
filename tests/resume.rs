//! A host that restarts under running workloads: they come back by themselves, as their restart
//! policy brings back a failure, and nothing else does. In fleet mode only once the control plane
//! has answered, applied its fences, and granted a lease.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use blocklyd::config::FleetConfig;
use blocklyd::manager::{Manager, Precondition, Records};
use blocklyd::metrics::Metrics;
use blocklyd::protocol::WorkloadState;
use blocklyd::runtime::fake::FakeRuntime;
use blocklyd::store::Store;
use support::{id, spec};
use tokio_util::sync::CancellationToken;

const NAME: &str = "blockly-test-w";

/// blocklyd on `dir`, in one boot of the host, single-node or fleet.
fn daemon(dir: &Path, fake: &Arc<FakeRuntime>, boot: &str, fleet: bool) -> Arc<Manager> {
    let mut config = support::config(dir, "");
    if fleet {
        config.fleet = Some(FleetConfig {
            url: "https://control-plane.test:8443".into(),
            ca: dir.join("tls/ca.pem"),
            enrollment_token_file: None,
            heartbeat_seconds: 5,
            api_address: None,
            labels: Default::default(),
            restart_requires_contact_seconds: 120,
        });
    }
    let store = Store::open(&config.state_dir).unwrap();
    let probe: blocklyd::ports::Probe = Arc::new(|_, _| true);
    Manager::with_boot(Arc::new(config), fake.clone(), store, Arc::new(Metrics::new()), probe, Some(boot.into()))
}

/// Boot one: the workload runs. Then the host shuts down, and Docker stops it on the way.
async fn running_then_host_down(fleet: bool) -> (tempfile::TempDir, Arc<FakeRuntime>) {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeRuntime::new());
    let first = daemon(dir.path(), &fake, "boot-1", fleet);
    first.reconcile(Records::FromDisk).await;
    first.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    first.start(id("w"), Some(1)).await.unwrap();
    fake.crash(NAME, 143, false);
    (dir, fake)
}

async fn after_restart(dir: &Path, fake: &Arc<FakeRuntime>, fleet: bool) -> Arc<Manager> {
    let second = daemon(dir, fake, "boot-2", fleet);
    assert!(second.reconcile(Records::FromDisk).await.error.is_none());
    tokio::spawn(second.clone().restart_supervisor(CancellationToken::new()));
    second
}

async fn until(m: &Arc<Manager>, want: WorkloadState) {
    support::until(m, "w", &format!("{want:?}"), |v| v.state == want).await;
}

/// Time let pass on purpose, to see that something doesn't happen. The restart supervisor acts at
/// once on what is due, so all of it is margin, for a slow CI runner.
const QUIET: Duration = Duration::from_secs(1);

#[tokio::test]
async fn a_workload_the_host_went_down_under_comes_back_by_itself() {
    let (dir, fake) = running_then_host_down(false).await;
    let second = daemon(dir.path(), &fake, "boot-2", false);
    second.reconcile(Records::FromDisk).await;
    assert_eq!(second.view(&id("w")).unwrap().state, WorkloadState::Restarting, "coming back, not stopped");
    tokio::spawn(second.clone().restart_supervisor(CancellationToken::new()));
    until(&second, WorkloadState::Running).await;
    assert_eq!(fake.count_calls("start "), 2, "the requested start, then the resume");
    assert_eq!(second.view(&id("w")).unwrap().restart_count, 0, "a restart of the host isn't a failure");
}

#[tokio::test]
async fn in_a_fleet_the_resume_waits_for_the_lease() {
    let (dir, fake) = running_then_host_down(true).await;
    let second = after_restart(dir.path(), &fake, true).await;
    tokio::time::sleep(QUIET).await;
    assert_eq!(second.view(&id("w")).unwrap().state, WorkloadState::Restarting);
    assert_eq!(fake.count_calls("start "), 1, "nothing starts before the control plane answers");
    second.fleet_contact(Ok("active"), Duration::from_millis(3));
    second.grant_lease(Some(120), Instant::now());
    until(&second, WorkloadState::Running).await;
}

#[tokio::test]
async fn a_node_held_lost_resumes_nothing_now_or_after_the_next_restart() {
    let (dir, fake) = running_then_host_down(true).await;
    let second = after_restart(dir.path(), &fake, true).await;
    second.fleet_contact(Ok("lost"), Duration::from_millis(3));
    second.grant_lease(Some(0), Instant::now());
    until(&second, WorkloadState::Stopped).await;
    assert_eq!(fake.count_calls("start "), 1);
    // Given up for good: the next restart of the host finds nothing to bring back either.
    let third = daemon(dir.path(), &fake, "boot-3", false);
    third.reconcile(Records::FromDisk).await;
    assert_eq!(third.view(&id("w")).unwrap().state, WorkloadState::Stopped);
}

#[tokio::test]
async fn a_fence_in_the_same_answer_wins_over_the_resume() {
    let (dir, fake) = running_then_host_down(true).await;
    let second = after_restart(dir.path(), &fake, true).await;
    // As the heartbeat applies an answer: its fences, then its lease.
    second.fence(id("w"), 2).await.unwrap();
    second.fleet_contact(Ok("active"), Duration::from_millis(3));
    second.grant_lease(Some(120), Instant::now());
    tokio::time::sleep(QUIET).await;
    assert_eq!(second.view(&id("w")).unwrap().state, WorkloadState::Fenced);
    assert_eq!(fake.count_calls("start "), 1, "the copy placed elsewhere is the one that runs");
}

#[tokio::test]
async fn a_fence_the_disk_couldnt_record_still_keeps_the_copy_from_resuming() {
    let (dir, fake) = running_then_host_down(true).await;
    let second = after_restart(dir.path(), &fake, true).await;
    // The record can't be written (a full or read-only disk): a directory, not empty, stands where
    // its new version is renamed to, which fails even for root.
    let record = dir.path().join("state/workloads/w/workload.json");
    std::fs::remove_file(&record).unwrap();
    std::fs::create_dir_all(record.join("in-the-way")).unwrap();
    assert!(second.fence(id("w"), 2).await.is_err(), "the fence couldn't be recorded");
    // The heartbeat grants the lease all the same.
    second.fleet_contact(Ok("active"), Duration::from_millis(3));
    second.grant_lease(Some(120), Instant::now());
    tokio::time::sleep(QUIET).await;
    assert_eq!(fake.count_calls("start "), 1, "a superseded copy never runs again");
    assert_eq!(second.view(&id("w")).unwrap().state, WorkloadState::Fenced);
}

#[tokio::test]
async fn what_had_stopped_before_the_host_went_down_stays_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeRuntime::new());
    let first = daemon(dir.path(), &fake, "boot-1", false);
    first.reconcile(Records::FromDisk).await;
    // One stopped on request; one that exited on its own, which blocklyd saw.
    for w in ["asked", "quit"] {
        first.ensure(id(w), spec(), Precondition::None, None).await.unwrap();
        first.start(id(w), None).await.unwrap();
    }
    first.stop(id("asked"), None, None).await.unwrap();
    fake.crash("blockly-test-quit", 0, false);
    first.stats(&id("quit")).await.ok();
    let second = daemon(dir.path(), &fake, "boot-2", false);
    second.reconcile(Records::FromDisk).await;
    tokio::spawn(second.clone().restart_supervisor(CancellationToken::new()));
    tokio::time::sleep(QUIET).await;
    for w in ["asked", "quit"] {
        assert_eq!(second.view(&id(w)).unwrap().state, WorkloadState::Stopped, "{w}");
    }
    assert_eq!(fake.count_calls("start "), 2, "only the two requested starts");
}

#[tokio::test]
async fn a_restart_of_blocklyd_alone_is_not_a_restart_of_the_host() {
    let (dir, fake) = running_then_host_down(false).await;
    // The same boot: blocklyd restarted, and found the workload dead with nobody watching.
    let again = daemon(dir.path(), &fake, "boot-1", false);
    again.reconcile(Records::FromDisk).await;
    tokio::spawn(again.clone().restart_supervisor(CancellationToken::new()));
    tokio::time::sleep(QUIET).await;
    assert_eq!(again.view(&id("w")).unwrap().state, WorkloadState::Stopped);
    assert_eq!(fake.count_calls("start "), 1);
}
