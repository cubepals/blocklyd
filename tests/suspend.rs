// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Suspending a workload with its memory kept, and resuming it: from memory when it comes back in
//! time, afresh on its data when it doesn't. A spec sent while it sleeps waits for its next boot,
//! and stop, kill, fence, delete and a restart of the host each end the sleep.

mod support;

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use blocklyd::manager::{ConflictCode, Manager, NodeError, Precondition, Records};
use blocklyd::metrics::Metrics;
use blocklyd::protocol::{DataDisposition, EnsureOutcome, WorkloadSpec, WorkloadState};
use blocklyd::runtime::ContainerStatus;
use blocklyd::runtime::fake::FakeRuntime;
use blocklyd::store::Store;
use hyper::StatusCode;
use support::{CLIENT, client, fixture, id, pki, request, serve, spec_with};
use tokio_util::sync::CancellationToken;

const NAME: &str = "blockly-test-w";
/// A resume waits a second here, so one that hangs is given up on quickly.
const QUICK_RESUME: &str = "[workloads]\nresume_timeout_seconds = 1\n";

/// The test spec, waking into "1.21.8".
fn sleepy() -> WorkloadSpec {
    spec_with(|v| v["resumesOn"] = "1.21.8".into())
}

/// The same server on another version, which the same `resumesOn` says it may wake into.
fn upgraded() -> WorkloadSpec {
    spec_with(|v| {
        v["resumesOn"] = "1.21.8".into();
        v["env"]["VERSION"] = "1.21.9".into();
    })
}

/// `w`, made from `sleepy`, running and then suspended.
async fn suspended(extra: &str) -> support::Fixture {
    let f = fixture(extra).await;
    f.manager.ensure(id("w"), sleepy(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    let r = f.manager.suspend(id("w"), None).await.unwrap();
    assert!(r.changed);
    assert_eq!(r.workload.state, WorkloadState::Suspended);
    f
}

fn digest(spec: &WorkloadSpec) -> String {
    spec.record().digest()
}

#[tokio::test]
async fn a_suspended_workload_resumes_from_memory_as_the_same_process() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), sleepy(), Precondition::None, None).await.unwrap();
    let before = f.manager.start(id("w"), None).await.unwrap().workload;
    let r = f.manager.suspend(id("w"), None).await.unwrap();
    assert!(r.changed && r.resumed.is_none());
    assert_eq!(r.workload.state, WorkloadState::Suspended);
    assert_eq!(f.fake.container(NAME).unwrap().status, ContainerStatus::Paused, "frozen, not stopped");
    let again = f.manager.suspend(id("w"), None).await.unwrap();
    assert!(!again.changed, "a repeat is a no-op");
    let r = f.manager.start(id("w"), None).await.unwrap();
    assert!(r.changed);
    assert_eq!(r.resumed, Some(true));
    assert_eq!(r.workload.state, WorkloadState::Running);
    assert_eq!(r.workload.locate.container_id, before.locate.container_id, "the same container");
    assert_eq!(r.workload.started_at, before.started_at, "never started again, only thawed");
    assert_eq!(f.fake.count_calls("start "), 1);
    // Running again: a start is the no-op it always was, and says nothing about resuming.
    let r = f.manager.start(id("w"), None).await.unwrap();
    assert!(!r.changed && r.resumed.is_none());
}

#[tokio::test]
async fn only_a_running_workload_can_be_suspended() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), sleepy(), Precondition::None, None).await.unwrap();
    let refused = f.manager.suspend(id("w"), None).await.unwrap_err();
    assert!(matches!(refused, NodeError::Conflict { code: ConflictCode::NotRunning, .. }), "{refused:?}");
    assert!(matches!(f.manager.suspend(id("nope"), None).await, Err(NodeError::NotFound(_))));
}

#[tokio::test]
async fn a_resume_that_hangs_is_killed_and_started_afresh_on_its_data() {
    let f = suspended(QUICK_RESUME).await;
    let frozen_since = f.manager.view(&id("w")).unwrap().started_at;
    f.fake.hang_unpause.store(true, Ordering::SeqCst);
    let r = f.manager.start(id("w"), None).await.unwrap();
    assert!(r.changed);
    assert_eq!(r.resumed, Some(false), "it didn't come back from memory");
    assert_eq!(r.workload.state, WorkloadState::Running);
    assert_ne!(r.workload.started_at, frozen_since, "a new run");
    assert_eq!(r.workload.restart_count, 0, "the kill was blocklyd's, not a failure");
    assert_eq!(f.fake.count_calls("kill "), 1);
    assert_eq!(f.fake.count_calls("start "), 2);
}

#[tokio::test]
async fn a_spec_sent_while_it_sleeps_waits_for_its_next_boot() {
    let f = suspended("").await;
    let r = f.manager.ensure(id("w"), upgraded(), Precondition::None, None).await.unwrap();
    assert_eq!(r.outcome, EnsureOutcome::Deferred);
    assert!(!r.restarted);
    assert_eq!(r.workload.state, WorkloadState::Suspended, "still asleep, memory kept");
    assert_eq!(r.workload.spec_digest, digest(&sleepy()), "what runs is unchanged");
    assert_eq!(r.workload.pending_spec_digest, Some(digest(&upgraded())));
    assert_eq!(f.fake.count_calls("create "), 1, "nothing made yet");
    // A resume wakes what is in memory; the deferred spec still waits.
    let r = f.manager.start(id("w"), None).await.unwrap();
    assert_eq!(r.resumed, Some(true));
    assert_eq!(r.workload.spec_digest, digest(&sleepy()));
    assert_eq!(r.workload.pending_spec_digest, Some(digest(&upgraded())));
    // The next boot puts it in place, with the secret it was sent with.
    f.manager.stop(id("w"), None, None).await.unwrap();
    let r = f.manager.start(id("w"), None).await.unwrap();
    assert!(r.resumed.is_none());
    assert_eq!(r.workload.state, WorkloadState::Running);
    assert_eq!(r.workload.spec_digest, digest(&upgraded()));
    assert_eq!(r.workload.pending_spec_digest, None);
    assert_eq!(r.workload.generation, 2);
    let env = &f.fake.container(NAME).unwrap().spec.env;
    assert!(env.contains(&("VERSION".into(), "1.21.9".into())));
    assert!(env.contains(&("RCON_PASSWORD".into(), "first-secret".into())));
}

#[tokio::test]
async fn a_cold_start_after_a_hung_resume_boots_the_deferred_spec() {
    let f = suspended(QUICK_RESUME).await;
    f.manager.ensure(id("w"), upgraded(), Precondition::None, None).await.unwrap();
    f.fake.hang_unpause.store(true, Ordering::SeqCst);
    let r = f.manager.start(id("w"), None).await.unwrap();
    assert_eq!(r.resumed, Some(false));
    assert_eq!(r.workload.state, WorkloadState::Running);
    assert_eq!(r.workload.spec_digest, digest(&upgraded()));
    assert_eq!(r.workload.pending_spec_digest, None);
}

#[tokio::test]
async fn any_other_spec_replaces_a_sleeping_workload_as_it_always_did() {
    let f = suspended("").await;
    // No resumesOn, as a control plane that never suspends sends it.
    let plain = spec_with(|v| v["env"]["VERSION"] = "1.21.9".into());
    let r = f.manager.ensure(id("w"), plain.clone(), Precondition::None, None).await.unwrap();
    assert_eq!(r.outcome, EnsureOutcome::Replaced);
    assert!(!r.restarted, "what it kept in memory can't come over");
    assert_eq!(r.workload.state, WorkloadState::Created);
    assert_eq!(r.workload.spec_digest, digest(&plain));
    assert_eq!(f.fake.count_calls("stop "), 1, "stopped first, so it saved");
    // Nor does one naming another resumesOn defer.
    let f = suspended("").await;
    let other = spec_with(|v| {
        v["resumesOn"] = "1.22".into();
        v["env"]["VERSION"] = "1.22".into();
    });
    let r = f.manager.ensure(id("w"), other, Precondition::None, None).await.unwrap();
    assert_eq!(r.outcome, EnsureOutcome::Replaced);
    // The spec it runs again changes nothing, and drops a deferred one.
    let f = suspended("").await;
    f.manager.ensure(id("w"), upgraded(), Precondition::None, None).await.unwrap();
    let r = f.manager.ensure(id("w"), sleepy(), Precondition::None, None).await.unwrap();
    assert_eq!(r.outcome, EnsureOutcome::Unchanged);
    assert_eq!(r.workload.state, WorkloadState::Suspended);
    assert_eq!(r.workload.pending_spec_digest, None);
}

#[tokio::test]
async fn stop_kill_fence_and_delete_each_reach_a_suspended_copy() {
    let f = suspended("").await;
    let r = f.manager.stop(id("w"), None, None).await.unwrap();
    assert!(r.changed && !r.forced);
    assert_eq!(r.workload.state, WorkloadState::Stopped);
    // Started again it is a plain start, not a resume.
    assert!(f.manager.start(id("w"), None).await.unwrap().resumed.is_none());

    let f = suspended("").await;
    let r = f.manager.kill(id("w"), None).await.unwrap();
    assert!(r.changed);
    assert_eq!(r.workload.state, WorkloadState::Stopped);

    let f = fixture("").await;
    f.manager.ensure(id("w"), sleepy(), Precondition::None, Some(1)).await.unwrap();
    f.manager.start(id("w"), Some(1)).await.unwrap();
    f.manager.suspend(id("w"), Some(1)).await.unwrap();
    let r = f.manager.fence(id("w"), 2).await.unwrap();
    assert!(r.changed && r.stopped);
    assert_eq!(r.workload.state, WorkloadState::Fenced);
    assert!(f.manager.start(id("w"), Some(1)).await.is_err(), "never started again");

    let f = suspended("").await;
    let r = f.manager.delete(id("w"), DataDisposition::Keep, None, None).await.unwrap();
    assert!(r.removed_container);
    assert_eq!(f.fake.count_calls("stop "), 1, "a graceful stop first, so it saved");
    assert_eq!(f.manager.view(&id("w")).unwrap().state, WorkloadState::Retained);
    // Brought back, it starts as a new workload would.
    f.manager.ensure(id("w"), sleepy(), Precondition::None, None).await.unwrap();
    let r = f.manager.start(id("w"), None).await.unwrap();
    assert!(r.resumed.is_none());
    assert_eq!(r.workload.state, WorkloadState::Running);
}

/// blocklyd on `dir`, in one boot of the host.
fn daemon(dir: &Path, fake: &Arc<FakeRuntime>, boot: &str) -> Arc<Manager> {
    let config = support::config(dir, "");
    let store = Store::open(&config.state_dir).unwrap();
    let probe: blocklyd::ports::Probe = Arc::new(|_, _| true);
    Manager::with_boot(Arc::new(config), fake.clone(), store, Arc::new(Metrics::new()), probe, Some(boot.into()))
}

#[tokio::test]
async fn a_restart_of_the_host_ends_the_sleep_and_resumes_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeRuntime::new());
    let first = daemon(dir.path(), &fake, "boot-1");
    first.reconcile(Records::FromDisk).await;
    first.ensure(id("w"), sleepy(), Precondition::None, None).await.unwrap();
    first.start(id("w"), None).await.unwrap();
    first.suspend(id("w"), None).await.unwrap();
    first.ensure(id("w"), upgraded(), Precondition::None, None).await.unwrap();
    // blocklyd alone restarts: it still knows the workload sleeps, and what waits for its boot.
    let again = daemon(dir.path(), &fake, "boot-1");
    again.reconcile(Records::FromDisk).await;
    let view = again.view(&id("w")).unwrap();
    assert_eq!(view.state, WorkloadState::Suspended);
    assert_eq!(view.pending_spec_digest, Some(digest(&upgraded())));
    // The host goes down: the frozen process with it, killed as Docker kills it.
    fake.crash(NAME, blocklyd::runtime::exit_by(libc::SIGKILL), false);
    let second = daemon(dir.path(), &fake, "boot-2");
    assert!(second.reconcile(Records::FromDisk).await.error.is_none());
    tokio::spawn(second.clone().restart_supervisor(CancellationToken::new()));
    tokio::time::sleep(Duration::from_secs(1)).await;
    let view = second.view(&id("w")).unwrap();
    assert_eq!(view.state, WorkloadState::Stopped, "not crashed, not restarting");
    assert_eq!(fake.count_calls("start "), 1, "never resumed by itself");
    // A start boots the deferred spec, with the secret the old container ran with.
    let r = second.start(id("w"), None).await.unwrap();
    assert!(r.resumed.is_none(), "a start of a stopped workload");
    assert_eq!(r.workload.state, WorkloadState::Running);
    assert_eq!(r.workload.spec_digest, digest(&upgraded()));
    assert!(fake.container(NAME).unwrap().spec.env.contains(&("RCON_PASSWORD".into(), "first-secret".into())));
}

#[tokio::test]
async fn over_http_suspend_takes_the_epoch_and_old_answers_stay_as_they_were() {
    let f = fixture("").await;
    let pki = pki(f.dir.path());
    let server = serve(f.manager.clone()).await;
    let me = client(&pki.ca, CLIENT);
    let call = async |method: &str, path: &str, headers: &[(&str, &str)], body: Option<serde_json::Value>| {
        let body = body.map(|b| serde_json::to_vec(&b).unwrap());
        request(server.addr, &pki.ca.pem, Some(&me), method, path, headers, body).await.expect("request")
    };
    let epoch = [("blocklyd-epoch", "3")];
    let r = call("PUT", "/v1/workloads/w", &epoch, Some(support::spec_json())).await;
    assert_eq!(r.status, StatusCode::CREATED);
    assert!(r.json()["workload"].get("pendingSpecDigest").is_none(), "absent unless something waits");
    let r = call("POST", "/v1/workloads/w/suspend", &epoch, None).await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert_eq!(r.code(), "not_running");
    let r = call("POST", "/v1/workloads/w/start", &epoch, None).await;
    assert!(r.json().get("resumed").is_none(), "absent on a start that resumes nothing");
    let r = call("POST", "/v1/workloads/w/suspend", &[("blocklyd-epoch", "2")], None).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "an older epoch acts on nothing");
    let r = call("POST", "/v1/workloads/w/suspend", &epoch, None).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["workload"]["state"], "suspended");
    let r = call("POST", "/v1/workloads/w/start", &epoch, None).await;
    assert_eq!(r.json()["resumed"], true);
    let r = call("GET", "/v1/health", &[], None).await;
    assert!(r.json()["features"].as_array().unwrap().contains(&"suspend".into()));
    server.cancel.cancel();
}
