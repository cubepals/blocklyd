//! Lifecycle and idempotency against the in-memory runtime: every verb repeated, raced and
//! refused, without Docker. Docker-backed versions of the important ones are in docker.rs.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use blocklyd::fleet::heartbeat::heartbeat_report;
use blocklyd::manager::{NodeError, Precondition};
use blocklyd::protocol::{DataDisposition, DataOutcome, EnsureOutcome, ExecRequest, WorkloadState};
use blocklyd::store::Phase;
use support::{fixture, id, spec, spec_with};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn duplicate_create_makes_one_workload() {
    let f = fixture("").await;
    let first = f.manager.ensure(id("w1"), spec(), Precondition::None, None).await.unwrap();
    assert_eq!(first.outcome, EnsureOutcome::Created);
    assert_eq!(first.workload.state, WorkloadState::Created);
    let second = f.manager.ensure(id("w1"), spec(), Precondition::None, None).await.unwrap();
    assert_eq!(second.outcome, EnsureOutcome::Unchanged);
    assert_eq!(f.fake.count_calls("create "), 1, "one container, however often it's asked for");
    assert_eq!(first.workload.ports, second.workload.ports);
}

#[tokio::test]
async fn a_create_below_the_free_disk_floor_is_refused_and_leaves_nothing() {
    // A floor no disk has room above.
    let f = fixture("[capacity]\nmin_free_disk_mb = 1000000000\n").await;
    let refused = f.manager.ensure(id("w1"), spec(), Precondition::None, None).await.unwrap_err();
    assert!(matches!(refused, NodeError::InsufficientDisk(_)), "{refused}");
    assert_eq!(f.fake.count_calls("create "), 0);
    assert!(matches!(f.manager.view(&id("w1")), Err(NodeError::NotFound(_))));
}

#[tokio::test]
async fn concurrent_duplicate_creates_are_serialized() {
    let f = fixture("").await;
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let m = f.manager.clone();
            tokio::spawn(async move { m.ensure(id("race"), spec(), Precondition::None, None).await.unwrap().outcome })
        })
        .collect();
    let mut outcomes = Vec::new();
    for t in tasks {
        outcomes.push(t.await.unwrap());
    }
    assert_eq!(outcomes.iter().filter(|o| **o == EnsureOutcome::Created).count(), 1);
    assert_eq!(f.fake.count_calls("create "), 1);
}

#[tokio::test]
async fn conditional_puts_follow_http_semantics() {
    let f = fixture("").await;
    let err = f.manager.ensure(id("w"), spec(), Precondition::IfMatch("*".into()), None).await.unwrap_err();
    assert!(matches!(err, NodeError::PreconditionFailed { current: None }));
    let made = f.manager.ensure(id("w"), spec(), Precondition::IfNoneMatch, None).await.unwrap();
    assert_eq!(made.outcome, EnsureOutcome::Created);
    let err = f.manager.ensure(id("w"), spec(), Precondition::IfNoneMatch, None).await.unwrap_err();
    assert!(matches!(err, NodeError::PreconditionFailed { current: Some(_) }));
    let changed = spec_with(|v| v["env"]["VERSION"] = "1.21.9".into());
    let err = f
        .manager
        .ensure(id("w"), changed.clone(), Precondition::IfMatch("sha256:stale".into()), None)
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::PreconditionFailed { .. }), "a stale apply can't overwrite a newer spec");
    let digest = made.workload.spec_digest.clone();
    let applied = f.manager.ensure(id("w"), changed, Precondition::IfMatch(digest), None).await.unwrap();
    assert_eq!(applied.outcome, EnsureOutcome::Replaced);
}

#[tokio::test]
async fn apply_keeps_ports_and_data_and_restarts_a_running_workload() {
    let f = fixture("").await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    let changed = spec_with(|v| v["env"]["VERSION"] = "1.21.9".into());
    let applied = f.manager.ensure(id("w"), changed, Precondition::None, None).await.unwrap();
    assert_eq!(applied.outcome, EnsureOutcome::Replaced);
    assert!(applied.restarted);
    assert_eq!(applied.workload.state, WorkloadState::Running);
    assert_eq!(applied.workload.generation, 2);
    assert_eq!(applied.workload.ports, made.workload.ports, "an apply never moves the address");
    assert_eq!(f.fake.count_calls("stop "), 1, "the old generation got its graceful stop");
}

#[tokio::test]
async fn a_changed_secret_is_a_new_spec_though_the_digest_ignores_it() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    let rotated = spec_with(|v| v["secrets"]["RCON_PASSWORD"] = "second-secret".into());
    let result = f.manager.ensure(id("w"), rotated, Precondition::None, None).await.unwrap();
    assert_eq!(result.outcome, EnsureOutcome::Replaced);
    let env = f.fake.container("blockly-test-w").unwrap().spec.env;
    assert!(env.contains(&("RCON_PASSWORD".into(), "second-secret".into())));
}

#[tokio::test]
async fn start_and_stop_are_idempotent() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    assert!(f.manager.start(id("w"), None).await.unwrap().changed);
    assert!(!f.manager.start(id("w"), None).await.unwrap().changed, "a second start is a no-op");
    assert_eq!(f.fake.count_calls("start "), 1, "never two servers");
    let stopped = f.manager.stop(id("w"), None, None).await.unwrap();
    assert!(stopped.changed && !stopped.forced);
    assert_eq!(stopped.workload.state, WorkloadState::Stopped);
    let again = f.manager.stop(id("w"), None, None).await.unwrap();
    assert!(!again.changed, "stop while stopped is a no-op");
    assert_eq!(f.fake.count_calls("stop "), 1);
}

#[tokio::test]
async fn a_stop_that_needed_a_kill_says_so_and_is_still_a_stop() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    f.fake.make_stubborn("blockly-test-w");
    let stopped = f.manager.stop(id("w"), Some(1), None).await.unwrap();
    assert!(stopped.forced);
    assert_eq!(stopped.workload.state, WorkloadState::Stopped, "requested, so not a crash");
    assert_eq!(stopped.workload.exit.unwrap().code, 137);
}

#[tokio::test]
async fn crashes_and_oom_kills_are_reported_as_such() {
    let f = fixture("").await;
    // No retries: with retries left a failure reads as restarting (see the restart tests).
    let no_retries =
        support::spec_with(|v| v["restart"] = serde_json::json!({ "policy": "on-failure", "maxRetries": 0 }));
    f.manager.ensure(id("w"), no_retries, Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    f.fake.crash("blockly-test-w", 1, false);
    let view = f.manager.stats(&id("w")).await.map(|_| f.manager.view(&id("w")).unwrap()).unwrap();
    assert_eq!(view.state, WorkloadState::Crashed);
    assert_eq!(view.exit.as_ref().unwrap().code, 1);
    f.manager.start(id("w"), None).await.unwrap();
    f.fake.crash("blockly-test-w", 137, true);
    f.manager.stats(&id("w")).await.unwrap();
    let view = f.manager.view(&id("w")).unwrap();
    assert_eq!(view.state, WorkloadState::Crashed);
    assert!(view.exit.unwrap().oom_killed);
}

#[tokio::test]
async fn delete_while_running_stops_gracefully_and_keeps_data_by_default() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    let data = f.manager.store.data_dir(&id("w"));
    std::fs::write(data.join("level.dat"), b"a world").unwrap();
    let deleted = f.manager.delete(id("w"), DataDisposition::Keep, None, None).await.unwrap();
    assert!(deleted.existed && deleted.removed_container);
    assert_eq!(deleted.data, DataOutcome::Kept);
    assert_eq!(f.fake.count_calls("stop "), 1, "graceful stop before removal");
    assert!(data.join("level.dat").exists());
    assert_eq!(f.manager.view(&id("w")).unwrap().state, WorkloadState::Retained);
    // A retained workload has no compute until the control plane asks for it again.
    assert!(matches!(f.manager.start(id("w"), None).await, Err(NodeError::Conflict { code: "no_compute", .. })));
    let back = f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    assert_eq!(back.outcome, EnsureOutcome::Replaced);
    assert!(data.join("level.dat").exists(), "the world came back with it");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_record_written_from_two_sides_at_once_ends_as_the_newest() {
    let f = fixture("").await;
    // A delete writes the record under the workload's lock while the container's events are
    // observed, and written, from another thread under none. Over and over, to meet the race.
    for n in 0..20 {
        let w = format!("w{n}");
        f.manager.ensure(id(&w), spec(), Precondition::None, None).await.unwrap();
        f.manager.start(id(&w), None).await.unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let events = tokio::spawn({
            let (m, done, name) = (f.manager.clone(), done.clone(), format!("blockly-test-{w}"));
            async move {
                while !done.load(Ordering::SeqCst) {
                    m.on_event(&name, "die", Some(143), time::OffsetDateTime::now_utc()).await;
                    tokio::task::yield_now().await;
                }
            }
        });
        f.manager.delete(id(&w), DataDisposition::Keep, None, None).await.unwrap();
        done.store(true, Ordering::SeqCst);
        events.await.unwrap();
        // What a restarted blocklyd would read.
        let (records, unreadable) = f.manager.store.load_all().unwrap();
        assert!(unreadable.is_empty(), "{w}: {unreadable:?}");
        let record = records.iter().find(|r| r.id == id(&w)).unwrap();
        assert_eq!((record.phase, record.ports.len()), (Phase::Retained, 0), "{w}");
    }
}

#[tokio::test]
async fn deleting_data_needs_confirmation_and_goes_to_trash() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    std::fs::write(f.manager.store.data_dir(&id("w")).join("level.dat"), b"a world").unwrap();
    for wrong in [None, Some("other".to_owned())] {
        let err = f.manager.delete(id("w"), DataDisposition::Delete, wrong, None).await.unwrap_err();
        assert!(matches!(err, NodeError::ConfirmationRequired));
    }
    let deleted = f.manager.delete(id("w"), DataDisposition::Delete, Some("w".into()), None).await.unwrap();
    assert_eq!(deleted.data, DataOutcome::Trashed);
    let trash = std::path::PathBuf::from(deleted.trash_path.unwrap());
    assert!(trash.join("data/level.dat").exists(), "recoverable until the trash is purged");
    let again = f.manager.delete(id("w"), DataDisposition::Delete, Some("w".into()), None).await.unwrap();
    assert!(!again.existed, "a repeated delete is a no-op");
    assert!(matches!(f.manager.view(&id("w")), Err(NodeError::NotFound(_))));
}

#[tokio::test]
async fn delete_while_stopped_removes_without_stopping() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    let deleted = f.manager.delete(id("w"), DataDisposition::Keep, None, None).await.unwrap();
    assert!(deleted.removed_container);
    assert_eq!(f.fake.count_calls("stop "), 0);
}

#[tokio::test]
async fn a_container_lost_by_the_runtime_is_reported_and_made_again_on_request() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.fake.vanish("blockly-test-w");
    let err = f.manager.start(id("w"), None).await.unwrap_err();
    assert!(matches!(err, NodeError::Conflict { code: "container_missing", .. }));
    let view = f.manager.view(&id("w")).unwrap();
    assert_eq!(view.state, WorkloadState::Missing);
    assert!(view.issues.iter().any(|i| i.code == "container_missing"));
    let back = f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    assert_eq!(back.outcome, EnsureOutcome::Replaced);
    assert!(back.workload.issues.is_empty());
}

#[tokio::test]
async fn an_unreachable_runtime_is_an_answer_not_a_hang() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.fake.set_available(false);
    assert!(matches!(f.manager.start(id("w"), None).await, Err(NodeError::RuntimeUnavailable(_))));
    assert!(matches!(
        f.manager.ensure(id("x"), spec(), Precondition::None, None).await,
        Err(NodeError::RuntimeUnavailable(_))
    ));
    assert!(!f.manager.docker_up());
    assert_eq!(f.manager.health().await.status, "degraded");
    assert!(f.manager.view(&id("x")).is_err(), "a failed create leaves nothing behind");
    f.fake.set_available(true);
    assert!(f.manager.start(id("w"), None).await.unwrap().changed);
    assert!(f.manager.docker_up());
}

#[tokio::test]
async fn memory_admission_refuses_a_start_that_would_overfill_the_host() {
    let f = fixture("[capacity]\nallocatable_memory_mb = 1536\nreserved_memory_mb = 0\n").await;
    f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    f.manager.ensure(id("b"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("a"), None).await.unwrap();
    let err = f.manager.start(id("b"), None).await.unwrap_err();
    assert!(matches!(err, NodeError::InsufficientCapacity(_)), "{err:?}");
    f.manager.stop(id("a"), None, None).await.unwrap();
    assert!(f.manager.start(id("b"), None).await.unwrap().changed, "room again once a stopped");
}

#[tokio::test]
async fn ports_are_never_shared_and_run_out_cleanly() {
    let f = fixture("[network]\nport_range = [43000, 43004]\nport_quarantine_seconds = 0\n").await;
    let a = f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    let b = f.manager.ensure(id("b"), spec(), Precondition::None, None).await.unwrap();
    let mut all: Vec<u16> = a.workload.ports.iter().chain(&b.workload.ports).map(|p| p.host_port).collect();
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 4);
    let err = f.manager.ensure(id("c"), spec(), Precondition::None, None).await.unwrap_err();
    assert!(matches!(err, NodeError::NoFreePorts(_)), "{err:?}");
    assert!(f.manager.view(&id("c")).is_err(), "nothing half-made");
    assert_eq!(f.manager.ports_usage().0, 4, "the failed create gave its one port back");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ports_workloads_let_go_of_at_once_all_rest() {
    let f = fixture("").await;
    let mut released = std::collections::BTreeSet::new();
    // Over and over, to meet the race between the writes of the quarantine.
    for n in 0..50 {
        let group: Vec<String> = (0..4).map(|k| format!("w{n}x{k}")).collect();
        for w in &group {
            let made = f.manager.ensure(id(w), spec(), Precondition::None, None).await.unwrap();
            released.extend(made.workload.ports.iter().map(|p| p.host_port));
        }
        let deletes: Vec<_> = group
            .into_iter()
            .map(|w| {
                let m = f.manager.clone();
                tokio::spawn(async move { m.delete(id(&w), DataDisposition::Keep, None, None).await })
            })
            .collect();
        for d in deletes {
            d.await.unwrap().unwrap();
        }
        let file = std::fs::read(f.dir.path().join("state/ports.json")).unwrap();
        let resting: Vec<blocklyd::store::RestingPort> = serde_json::from_slice(&file).expect("ports.json parses");
        let resting: std::collections::BTreeSet<u16> = resting.iter().map(|p| p.port).collect();
        assert_eq!(resting, released, "after round {n}, every released port is still resting");
    }
}

#[tokio::test]
async fn a_port_a_failed_create_held_for_a_moment_is_free_at_once() {
    // Quarantine on, as on a host: it is for ports a route may still know, and this one never ran.
    let f = fixture("[network]\nport_range = [43000, 43001]\nport_quarantine_seconds = 600\n").await;
    let one_port = || spec_with(|v| v["ports"].as_array_mut().unwrap().truncate(1));
    let a = f.manager.ensure(id("a"), one_port(), Precondition::None, None).await.unwrap();
    assert_eq!(a.workload.ports[0].host_port, 43000);
    let err = f.manager.ensure(id("b"), spec(), Precondition::None, None).await.unwrap_err();
    assert!(matches!(err, NodeError::NoFreePorts(_)), "two ports asked, one left: {err:?}");
    let c = f.manager.ensure(id("c"), one_port(), Precondition::None, None).await.unwrap();
    assert_eq!(c.workload.ports[0].host_port, 43001, "b's port went back without resting");
}

#[tokio::test]
async fn invalid_ids_and_specs_are_refused_before_anything_happens() {
    let f = fixture("").await;
    assert!(blocklyd::ids::WorkloadId::parse("../../etc").is_err());
    let bad = spec_with(|v| {
        v["image"] = "attacker/miner:latest".into();
        v["storage"]["mountPath"] = "/".into();
    });
    let calls_before = f.fake.calls.lock().unwrap().len();
    let err = f.manager.ensure(id("w"), bad, Precondition::None, None).await.unwrap_err();
    let NodeError::Invalid(fields) = err else { panic!("{err:?}") };
    assert_eq!(fields.len(), 2);
    assert_eq!(f.fake.calls.lock().unwrap().len(), calls_before, "the runtime was never touched");
}

#[tokio::test]
async fn exec_runs_once_per_idempotency_key() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    let req = || ExecRequest { command: vec!["echo".into(), "hi".into()], timeout_seconds: 5 };
    assert!(matches!(
        f.manager.exec(id("w"), req(), None, None).await,
        Err(NodeError::Conflict { code: "not_running", .. })
    ));
    f.manager.start(id("w"), None).await.unwrap();
    let first = f.manager.exec(id("w"), req(), Some("k1".into()), None).await.unwrap();
    let second = f.manager.exec(id("w"), req(), Some("k1".into()), None).await.unwrap();
    assert_eq!(first, second);
    assert_eq!(f.fake.count_calls("exec "), 1, "the retry got the first answer");
    let other = ExecRequest { command: vec!["rm".into(), "-rf".into(), "/data".into()], timeout_seconds: 5 };
    assert!(matches!(
        f.manager.exec(id("w"), other, Some("k1".into()), None).await,
        Err(NodeError::IdempotencyMismatch)
    ));
}

#[tokio::test]
async fn concurrent_execs_with_one_key_run_once() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    let m = Arc::clone(&f.manager);
    let tasks: Vec<_> = (0..5)
        .map(|_| {
            let m = m.clone();
            tokio::spawn(async move {
                let req = ExecRequest { command: vec!["sleep".into(), "0.2".into()], timeout_seconds: 5 };
                m.exec(id("w"), req, Some("same".into()), None).await.unwrap()
            })
        })
        .collect();
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(f.fake.count_calls("exec "), 1);
}

#[tokio::test]
async fn logs_stream_what_the_workload_printed() {
    use futures_util::StreamExt;
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    f.fake.log("blockly-test-w", blocklyd::runtime::LogStream::Stdout, "Done (9.9s)!");
    let records: Vec<_> = f.manager.logs(&id("w"), Some(10), None, false).unwrap().collect().await;
    let text = serde_json::to_string(&records).unwrap();
    assert!(text.contains("Done (9.9s)!"));
    assert!(text.contains(r#""event":"end""#));
}

#[tokio::test]
async fn a_follow_ends_when_its_reader_goes_away_however_quiet_the_workload() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    f.fake.hold_follows.store(true, std::sync::atomic::Ordering::SeqCst);
    let open = || Arc::strong_count(&f.fake.follows) - 1;
    let records = f.manager.logs(&id("w"), None, None, true).unwrap();
    support::eventually("the follow to open", async || (open() == 1, open())).await;
    // The workload prints nothing, and the console that followed it is closed: the follow ends
    // now, not at the workload's next line.
    drop(records);
    support::eventually("the follow to end", async || (open() == 0, open())).await;
}

#[tokio::test]
async fn while_the_runtime_is_down_nothing_stale_is_passed_off_as_the_state() {
    let f = fixture("").await;
    f.manager.reconcile(true).await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    assert_eq!(f.manager.view(&id("w")).unwrap().state, WorkloadState::Running);
    // The daemon goes away, and (as a daemon stop does without live-restore) takes the workload.
    f.fake.set_available(false);
    let health = f.manager.health().await;
    assert!(!health.docker && !health.reconciled, "an outage forgets the last reconciliation");
    assert_eq!(f.manager.view(&id("w")).unwrap().state, WorkloadState::Unknown, "not the last thing it saw");
    f.fake.set_available(true);
    f.fake.crash("blockly-test-w", 0, false);
    assert_eq!(f.manager.health().await.status, "degraded", "back, but not looked at yet");
    f.manager.reconcile(false).await;
    assert_eq!(f.manager.health().await.status, "ok");
    assert_eq!(f.manager.view(&id("w")).unwrap().state, WorkloadState::Stopped, "the truth, once it has looked");
}

#[tokio::test]
async fn a_crash_blocklyd_sees_is_restarted_within_the_policy_and_nothing_else_is() {
    let f = fixture("").await;
    tokio::spawn(f.manager.clone().restart_supervisor(tokio_util::sync::CancellationToken::new()));
    let two_retries =
        support::spec_with(|v| v["restart"] = serde_json::json!({ "policy": "on-failure", "maxRetries": 2 }));
    f.manager.ensure(id("w"), two_retries, Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();

    for expected in 1..=2 {
        f.fake.crash("blockly-test-w", 3, false);
        f.manager.stats(&id("w")).await.unwrap(); // blocklyd looks, and sees it go
        support::until(&f.manager, "w", "a restart", |v| {
            v.restart_count == expected && v.state == WorkloadState::Running
        })
        .await;
    }
    f.fake.crash("blockly-test-w", 3, false);
    f.manager.stats(&id("w")).await.unwrap();
    // Time let pass on purpose, for a restart that must not come: twice the longest backoff the
    // policy could still use (2 s, before a third try).
    tokio::time::sleep(Duration::from_secs(4)).await;
    let view = f.manager.view(&id("w")).unwrap();
    assert_eq!((view.state, view.restart_count), (WorkloadState::Crashed, 2), "the policy ran out");
    assert!(view.last_failure_at.is_some());
    // A requested start begins a fresh run.
    f.manager.start(id("w"), None).await.unwrap();
    assert_eq!(f.manager.view(&id("w")).unwrap().restart_count, 0);
}

#[tokio::test]
async fn a_workload_found_dead_at_startup_is_not_restarted() {
    // As after blocklyd itself restarted, in the same boot of the host: Docker says the container
    // exited with 255 while nobody watched. (A host that restarted is tests/resume.rs.)
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    f.fake.crash("blockly-test-w", 255, false);
    let restarted = support::manager_on(f.dir.path(), f.fake.clone(), "");
    tokio::spawn(restarted.clone().restart_supervisor(tokio_util::sync::CancellationToken::new()));
    restarted.reconcile(true).await;
    restarted.stats(&id("w")).await.ok();
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let view = restarted.view(&id("w")).unwrap();
    assert_eq!(view.state, WorkloadState::Crashed, "reported, and left for the control plane");
    assert_eq!(f.fake.count_calls("start "), 1, "only the requested start");
}

#[tokio::test]
async fn a_restart_of_blocklyd_doesnt_give_a_crash_loop_its_retries_again() {
    let f = fixture("").await;
    let two_retries =
        support::spec_with(|v| v["restart"] = serde_json::json!({ "policy": "on-failure", "maxRetries": 2 }));
    f.manager.ensure(id("w"), two_retries, Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    let first_run = CancellationToken::new();
    tokio::spawn(f.manager.clone().restart_supervisor(first_run.clone()));
    f.fake.crash("blockly-test-w", 3, false);
    support::until(&f.manager, "w", "the first restart", |v| v.restart_count == 1 && v.state == WorkloadState::Running)
        .await;
    first_run.cancel();

    // blocklyd restarts, in the same boot, and finds the workload running again.
    let again = support::manager_on(f.dir.path(), f.fake.clone(), "");
    again.reconcile(true).await;
    tokio::spawn(again.clone().restart_supervisor(CancellationToken::new()));
    assert_eq!(again.view(&id("w")).unwrap().restart_count, 1, "remembered");
    let beat = heartbeat_report(&again, "test-node", "session", None, 1).await;
    assert_eq!(beat.workloads[0].restart_count, 1, "and reported");

    f.fake.crash("blockly-test-w", 3, false);
    support::until(&again, "w", "the second restart", |v| v.restart_count == 2 && v.state == WorkloadState::Running)
        .await;
    f.fake.crash("blockly-test-w", 3, false);
    again.stats(&id("w")).await.unwrap();
    let view = again.view(&id("w")).unwrap();
    // A restart on its way reads as restarting: this one isn't coming.
    assert_eq!((view.state, view.restart_count), (WorkloadState::Crashed, 2), "two retries in all, not two more");
    assert_eq!(f.fake.count_calls("start "), 3, "the requested start and two restarts");
}

#[tokio::test]
async fn a_restart_that_wouldnt_fit_is_refused_and_says_why() {
    // Room for one of the two at a time.
    let f = fixture("[capacity]\nallocatable_memory_mb = 1536\nreserved_memory_mb = 0\n").await;
    f.manager.ensure(id("a"), spec(), Precondition::None, None).await.unwrap();
    f.manager.ensure(id("b"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("a"), None).await.unwrap();
    f.fake.crash("blockly-test-a", 3, false);
    f.manager.stats(&id("a")).await.unwrap();
    assert_eq!(f.manager.view(&id("a")).unwrap().state, WorkloadState::Restarting, "a restart is due");
    // Before it comes round, b is started in the room a left.
    f.manager.start(id("b"), None).await.unwrap();
    tokio::spawn(f.manager.clone().restart_supervisor(CancellationToken::new()));
    let view = support::until(&f.manager, "a", "the restart to be refused", |v| {
        v.state == WorkloadState::Crashed && v.issues.iter().any(|i| i.code == "insufficient_capacity")
    })
    .await;
    assert_eq!(view.restart_count, 0, "no retry was spent");
    assert_eq!(f.fake.count_calls("start blockly-test-a"), 1, "only the requested start");
    // The reason stays through reconciliation, until a start that fits, and the control plane is
    // told it with the workload.
    f.manager.reconcile(false).await;
    assert!(f.manager.view(&id("a")).unwrap().issues.iter().any(|i| i.code == "insufficient_capacity"));
    let beat = heartbeat_report(&f.manager, "test-node", "session", None, 1).await;
    let reported = |w: &str| beat.workloads.iter().find(|r| r.id == w).map(|r| r.issues.clone()).unwrap();
    assert_eq!(reported("a").iter().map(|i| i.code.as_str()).collect::<Vec<_>>(), ["insufficient_capacity"]);
    assert!(reported("b").is_empty());
    let wire = serde_json::to_value(&beat).unwrap();
    assert!(wire["workloads"].as_array().unwrap().iter().any(|w| w.get("issues").is_none()), "none is left out");
    f.manager.stop(id("b"), None, None).await.unwrap();
    f.manager.start(id("a"), None).await.unwrap();
    assert!(f.manager.view(&id("a")).unwrap().issues.is_empty());
}

/// An object store that takes an upload and doesn't answer until told to, then drops it. Says
/// when it holds one.
async fn stalling_store() -> (String, tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/world.tar.gz", listener.local_addr().unwrap());
    let (holding, held) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = socket.read(&mut [0u8; 4096]).await;
        let _ = holding.send(());
        let _ = released.await;
    });
    (url, held, release)
}

#[tokio::test]
async fn a_workload_busy_for_long_holds_up_no_other_workloads_restart() {
    let f = fixture("").await;
    tokio::spawn(f.manager.clone().restart_supervisor(CancellationToken::new()));
    for w in ["x", "y"] {
        f.manager.ensure(id(w), spec(), Precondition::None, None).await.unwrap();
        f.manager.start(id(w), None).await.unwrap();
    }
    // An export of x holds x's lock for as long as its upload takes, and this one never ends.
    let (url, held, release) = stalling_store().await;
    let request: blocklyd::protocol::ExportRequest =
        serde_json::from_value(serde_json::json!({ "url": url, "quiesced": true })).unwrap();
    let export = tokio::spawn({
        let m = f.manager.clone();
        async move { m.export(id("x"), request, None).await }
    });
    held.await.unwrap();
    // x crashes meanwhile, and its restart comes due while x is busy.
    f.fake.crash("blockly-test-x", 3, false);
    f.manager.stats(&id("x")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    // y's crash is restarted all the same, within its backoff (half a second; the rest is margin).
    f.fake.crash("blockly-test-y", 3, false);
    f.manager.stats(&id("y")).await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        f.manager.stats(&id("y")).await.unwrap();
        let y = f.manager.view(&id("y")).unwrap();
        if (y.state, y.restart_count) == (WorkloadState::Running, 1) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "y wasn't restarted while x was busy: {y:#?}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(f.fake.count_calls("start blockly-test-x"), 1, "x waits its turn");
    assert_eq!(f.manager.view(&id("x")).unwrap().state, WorkloadState::Restarting);
    // Once the export ends, x gets its restart too.
    release.send(()).unwrap();
    assert!(export.await.unwrap().is_err(), "the store dropped the upload");
    support::until(&f.manager, "x", "x's restart", |v| v.state == WorkloadState::Running && v.restart_count == 1).await;
}

#[tokio::test]
async fn an_exec_runs_in_the_container_it_was_checked_against_or_not_at_all() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, None).await.unwrap();
    f.manager.start(id("w"), None).await.unwrap();
    // The exec is checked and on its way into the runtime when the workload is replaced.
    let gate = f.fake.exec_gate.write().await;
    let exec = {
        let m = f.manager.clone();
        let request = ExecRequest { command: vec!["echo".into(), "hi".into()], timeout_seconds: 5 };
        tokio::spawn(async move { m.exec(id("w"), request, None, None).await })
    };
    support::eventually("the exec to reach the runtime", async || {
        let calls = f.fake.count_calls("exec ");
        (calls == 1, calls)
    })
    .await;
    // A replace takes the workload's lock: the exec doesn't hold it while it runs.
    let changed = spec_with(|v| v["env"]["VERSION"] = "1.21.9".into());
    let replace = f.manager.ensure(id("w"), changed, Precondition::None, None);
    let replaced = tokio::time::timeout(Duration::from_secs(30), replace).await.expect("not held up").unwrap();
    assert!(replaced.restarted, "another container runs under the same name");
    drop(gate);
    let err = exec.await.unwrap().unwrap_err();
    assert!(matches!(err, NodeError::Conflict { code: "not_running", .. }), "{err:?}");
    let calls = f.fake.calls.lock().unwrap().clone();
    assert!(calls.iter().any(|c| c.starts_with("exec ") && !c.contains("blockly-test-w")), "by id: {calls:?}");
}

#[tokio::test]
async fn node_totals_count_what_runs_now() {
    let f = fixture("").await;
    for w in ["a", "b", "c"] {
        f.manager.ensure(id(w), spec(), Precondition::None, None).await.unwrap();
    }
    let used = || {
        let capacity = f.manager.capacity();
        (capacity.used_memory_bytes, capacity.used_cpu_cores)
    };
    assert_eq!(used(), (Some(0), Some(0.0)), "nothing runs, so nothing is used");
    f.manager.start(id("a"), None).await.unwrap();
    f.manager.start(id("b"), None).await.unwrap();
    assert_eq!(used().0, None, "running, and not sampled yet");
    f.manager.sample_all().await;
    // The fake runtime reports a working set of 448 MiB for each running workload.
    let one = 448 * 1024 * 1024;
    assert_eq!(used().0, Some(2 * one), "c never ran and has no sample: the total is known all the same");
    f.manager.stop(id("b"), None, None).await.unwrap();
    assert_eq!(used().0, Some(one), "b's last sample stopped counting when b stopped");
    // A CPU rate needs two samples at least half a second apart: time let pass on purpose.
    assert_eq!(used().1, None, "one sample of a is no rate yet");
    tokio::time::sleep(Duration::from_millis(600)).await;
    f.manager.sample_all().await;
    assert_eq!(used().1, Some(0.0));
}
