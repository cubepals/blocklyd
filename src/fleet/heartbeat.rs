// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! The heartbeat: every few seconds the node tells the control plane everything it holds. The
//! answer fences the copies a newer placement superseded, and grants the execution lease. It runs
//! until shutdown, whatever the control plane does: a control plane that is down changes nothing
//! on the node. Workloads keep running, and the next beat that gets through reports the whole
//! state again.
//!
//! A beat's body is `heartbeat_report`, put together from what the manager holds. Between beats
//! the loop looks at the workloads a few times a second, and beats early once a workload's state,
//! epoch or ports differ from the last beat's, though never sooner than `QUICKEST` after it: a
//! stop frees its room in the control plane's ledger about a second later, not a whole interval.
//! A beat that failed waits out the interval as before.
//!
//! The node never stops a workload because it lost contact: self-fencing on a lease would make a
//! control-plane outage stop every server in the fleet (docs/fleet.md, "Epochs and fencing"). The
//! lease only governs what the node does unasked, restarting a failed workload or resuming one the
//! host went down under, so a node cut off from the control plane, which may have been replaced,
//! brings nothing back on its own.
//!
//! An answer may also offer a newer blocklyd, which the node installs and restarts into
//! (upgrade.rs); a beat accepted once reconciled ends the trial of one just installed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hyper::Method;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use super::identity::Credentials;
use crate::http_client as client;
use crate::ids::{NodeId, WorkloadId};
use crate::manager::{Lifecycle, Manager};
use crate::protocol::ProtocolVersions;
use crate::protocol::WorkloadState;
use crate::protocol::wire::FLEET_API;
use crate::protocol::wire::{Fence, HeartbeatRequest, HeartbeatResponse, WorkloadReport};
use crate::upgrade::Upgrader;

/// The soonest a beat follows the one before when what it reports changed.
const QUICKEST: Duration = Duration::from_secs(1);
/// How often the loop looks at the workloads between beats.
const LOOK: Duration = Duration::from_millis(200);

/// Everything this node holds, for a heartbeat.
pub fn heartbeat_report(
    manager: &Manager,
    node_id: &NodeId,
    session: &str,
    boot_id: Option<String>,
    seq: u64,
) -> HeartbeatRequest {
    let workloads = workload_reports(manager);
    let issues = manager.host_issues();
    HeartbeatRequest {
        node_id: node_id.to_string(),
        session_id: session.to_owned(),
        boot_id,
        seq,
        daemon_version: crate::upgrade::daemon_version(),
        protocol: ProtocolVersions::ours(),
        features: crate::protocol::features(),
        runtime_up: manager.docker_up(),
        reconciled: manager.reconciled(),
        capacity: manager.capacity(),
        workloads,
        issues,
        addresses: manager.addresses(),
        upgrade_failed: None,
    }
}

fn workload_reports(manager: &Manager) -> Vec<WorkloadReport> {
    manager
        .list(None)
        .into_iter()
        .map(|v| WorkloadReport {
            id: v.id.to_string(),
            epoch: v.epoch,
            superseded_by: v.superseded_by,
            state: v.state,
            spec_digest: v.spec_digest,
            generation: v.generation,
            memory_mb: v.resources.memory_mb,
            restart_count: v.restart_count,
            exit: v.exit,
            last_failure_at: v.last_failure_at,
            changed_at: v.changed_at,
            ports: v.ports.iter().map(|p| (p.name.clone(), p.host_port)).collect(),
            issues: v.issues,
        })
        .collect()
}

/// What of a report the control plane acts on at once: each workload's state, epoch and ports.
#[derive(Debug, PartialEq)]
struct Gist(Vec<Seen>);

#[derive(Debug, PartialEq)]
struct Seen {
    id: String,
    epoch: Option<u64>,
    state: WorkloadState,
    ports: BTreeMap<String, u16>,
}

impl Gist {
    fn of(workloads: &[WorkloadReport]) -> Self {
        let seen =
            |w: &WorkloadReport| Seen { id: w.id.clone(), epoch: w.epoch, state: w.state, ports: w.ports.clone() };
        Self(workloads.iter().map(seen).collect())
    }
}

/// Waits for the next beat after one sent at `sent` that reported `last`: `interval` from now, or
/// sooner once `look` finds something else, though not before `QUICKEST` after `sent`. A beat that
/// failed (`!delivered`) waits out the interval. False once cancelled.
async fn next_beat(
    sent: Instant,
    delivered: bool,
    interval: Duration,
    last: &Gist,
    look: impl Fn() -> Gist,
    cancel: &CancellationToken,
) -> bool {
    let due = Instant::now() + interval;
    loop {
        let now = Instant::now();
        if now >= due {
            return true;
        }
        let wake = if delivered { due.min(now + LOOK).max(sent + QUICKEST) } else { due };
        tokio::select! {
            () = tokio::time::sleep_until(wake.into()) => {}
            () = cancel.cancelled() => return false,
        }
        if delivered && look() != *last {
            return true;
        }
    }
}

/// Applies an answer's fences without waiting for any copy to stop: the loop goes on beating for
/// the node's other workloads meanwhile. Each is noted at once, before the lease is granted, and
/// carried out in its own task.
fn apply_fences(manager: &Arc<Manager>, fences: Vec<Fence>) {
    for fence in fences {
        let Ok(id) = WorkloadId::parse(&fence.workload) else { continue };
        manager.fence_soon(id, fence.current_epoch);
    }
}

pub async fn run(
    manager: Arc<Manager>,
    credentials: Arc<Credentials>,
    upgrader: Arc<Upgrader>,
    control_plane: String,
    every: u64,
    cancel: CancellationToken,
) {
    let node_id = credentials.node_id();
    let url = format!("{}{FLEET_API}/nodes/{node_id}/heartbeat", control_plane.trim_end_matches('/'));
    let session = uuid::Uuid::new_v4().to_string();
    let boot = crate::host::boot_id();
    let mut interval = Duration::from_secs(every);
    let mut seq: u64 = 0;
    let mut failing_since: Option<u64> = None;
    loop {
        seq += 1;
        let mut request: HeartbeatRequest = heartbeat_report(&manager, &node_id, &session, boot.clone(), seq);
        request.upgrade_failed = upgrader.failure();
        let reported = Gist::of(&request.workloads);
        let timeout = interval.max(Duration::from_secs(2)).min(Duration::from_secs(10));
        let sent = Instant::now();
        let result =
            client::json::<_, HeartbeatResponse>(Method::POST, &url, &request, credentials.client(), timeout).await;
        let delivered = result.is_ok();
        match result {
            Ok(answer) => {
                if let Some(since) = failing_since.take() {
                    info!(missed = seq - since, "heartbeat: the control plane answers again");
                }
                manager.fleet_contact(Ok(Lifecycle::from(answer.lifecycle.as_str())), sent.elapsed());
                // Fences first: the lease granted below lets waiting resumes go ahead at once,
                // and none of them may be a copy this answer supersedes.
                apply_fences(&manager, answer.fences);
                manager.grant_lease(answer.lease_seconds, sent);
                if answer.renew {
                    credentials.renew_soon(&control_plane, manager.clone());
                }
                if let Some(s) = answer.heartbeat_seconds.filter(|s| (1..=60).contains(s)) {
                    interval = Duration::from_secs(s);
                }
                upgrader.accepted(request.reconciled);
                if let Some(offer) = answer.upgrade {
                    upgrader.offered(offer, credentials.client());
                }
            }
            Err(e) => {
                manager.fleet_contact(Err(&e.to_string()), sent.elapsed());
                // Loud once, then every minute or so: a long outage shouldn't flood the journal.
                match failing_since {
                    None => {
                        failing_since = Some(seq);
                        warn!(error = %e, "heartbeat: the control plane doesn't answer; workloads carry on");
                    }
                    Some(since) if (seq - since).is_multiple_of(12) => {
                        warn!(error = %e, missed = seq - since, "heartbeat: still no answer")
                    }
                    Some(_) => debug!(error = %e, "heartbeat failed"),
                }
            }
        }
        let look = || Gist::of(&workload_reports(&manager));
        if !next_beat(sent, delivered, interval, &reported, look, &cancel).await {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::config::Config;
    use crate::manager::{Precondition, Records};
    use crate::metrics::Metrics;
    use crate::runtime::fake::FakeRuntime;
    use crate::store::Store;

    fn gist(epoch: u64) -> Gist {
        Gist(vec![Seen { id: "w".into(), epoch: Some(epoch), state: WorkloadState::Running, ports: BTreeMap::new() }])
    }

    /// Waits as the loop does after a beat sent now, and how long it took.
    async fn waited(delivered: bool, interval: Duration, last: &Gist, look: impl Fn() -> Gist) -> Duration {
        let sent = Instant::now();
        assert!(next_beat(sent, delivered, interval, last, look, &CancellationToken::new()).await);
        sent.elapsed()
    }

    #[tokio::test]
    async fn nothing_changed_waits_out_the_interval() {
        let took = waited(true, Duration::from_secs(2), &gist(1), || gist(1)).await;
        assert!(took >= Duration::from_secs(2), "{took:?}");
    }

    #[tokio::test]
    async fn under_steady_churn_beats_come_once_a_second() {
        let epoch = AtomicU64::new(0);
        let interval = Duration::from_secs(5);
        for _ in 0..3 {
            let last = gist(epoch.load(Ordering::SeqCst));
            let took = waited(true, interval, &last, || gist(epoch.fetch_add(1, Ordering::SeqCst))).await;
            assert!((QUICKEST..interval).contains(&took), "{took:?}");
        }
    }

    #[tokio::test]
    async fn a_beat_that_failed_is_not_retried_sooner_for_a_change() {
        let took = waited(false, Duration::from_secs(2), &gist(1), || gist(2)).await;
        assert!(took >= Duration::from_secs(2), "{took:?}");
    }

    /// blocklyd over the in-memory runtime, with nothing on it yet.
    async fn daemon() -> (tempfile::TempDir, Arc<FakeRuntime>, Arc<Manager>) {
        let dir = tempfile::tempdir().unwrap();
        let (uid, gid) = (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw());
        let config: Config = toml::from_str(&format!(
            "deployment_id = \"test\"\nnode_id = \"n\"\nstate_dir = \"{}\"\n[api]\nlisten = \"127.0.0.1:0\"\n\
             [workloads]\nallowed_images = [\"alpine:\"]\ndata_owner = \"{uid}:{gid}\"\n\
             [capacity]\nmin_free_disk_mb = 1\n",
            dir.path().display()
        ))
        .unwrap();
        let store = Store::open(&config.state_dir).unwrap();
        let fake = Arc::new(FakeRuntime::new());
        let m = Manager::new(Arc::new(config), fake.clone(), store, Arc::new(Metrics::new()), Arc::new(|_, _| true));
        assert!(m.reconcile(Records::FromDisk).await.error.is_none());
        (dir, fake, m)
    }

    /// `name` running at `epoch`, taking `grace` seconds to stop.
    async fn running(m: &Arc<Manager>, name: &str, epoch: Option<u64>, grace: u32) -> WorkloadId {
        let spec = serde_json::from_value(serde_json::json!({
            "image": "alpine:3.22",
            "resources": { "memoryMb": 256 },
            "storage": { "mountPath": "/data", "sizeGb": 1 },
            "stop": { "timeoutSeconds": grace },
        }))
        .unwrap();
        let w = WorkloadId::parse(name).unwrap();
        m.ensure(w.clone(), spec, Precondition::None, epoch).await.unwrap();
        m.start(w.clone(), epoch).await.unwrap();
        w
    }

    #[tokio::test]
    async fn a_stop_is_reported_within_about_a_second() {
        let (_dir, _fake, m) = daemon().await;
        let w = running(&m, "w", None, 90).await;

        // A beat goes out with heartbeat_seconds = 5 just before the stop.
        let look = || Gist::of(&workload_reports(&m));
        let reported = look();
        let sent = Instant::now();
        m.stop(w, None, None).await.unwrap();
        assert!(next_beat(sent, true, Duration::from_secs(5), &reported, look, &CancellationToken::new()).await);
        assert!(sent.elapsed() < Duration::from_millis(1500), "{:?}", sent.elapsed());
        let next = workload_reports(&m);
        assert_eq!(next[0].state, WorkloadState::Stopped, "{:?}", next[0].state);
    }

    #[tokio::test]
    async fn an_answers_fences_are_applied_without_holding_up_the_next_beat() {
        let (_dir, fake, m) = daemon().await;
        let mut fences = Vec::new();
        for name in ["a", "b", "c"] {
            let w = running(&m, name, Some(1), 2).await;
            // Each copy takes its whole grace to stop.
            fake.make_stubborn(&format!("blockly-test-{name}"));
            fences.push(Fence { workload: w.to_string(), current_epoch: 2 });
        }
        let applying = Instant::now();
        apply_fences(&m, fences);
        assert!(applying.elapsed() < QUICKEST, "applying the fences took {:?}", applying.elapsed());
        // Each copy is recorded superseded before its stop ends, and is then fenced.
        let reports = |m: &Manager| workload_reports(m);
        let deadline = Instant::now() + Duration::from_secs(1);
        while !reports(&m).iter().all(|r| r.superseded_by == Some(2)) {
            assert!(Instant::now() < deadline, "not all recorded superseded: {:?}", reports(&m));
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        while !reports(&m).iter().all(|r| r.state == WorkloadState::Fenced) {
            assert!(Instant::now() < deadline, "not all fenced: {:?}", reports(&m));
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
