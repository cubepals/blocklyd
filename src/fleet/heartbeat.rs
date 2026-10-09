//! The heartbeat: every few seconds the node tells the control plane everything it holds. The
//! answer fences the copies a newer placement superseded, and grants the execution lease. It runs
//! until shutdown, whatever the control plane does: a control plane that is down changes nothing
//! on the node. Workloads keep running, and the next beat that gets through reports the whole
//! state again.
//!
//! A beat's body is `heartbeat_report`, put together from what the manager holds.
//!
//! The node never stops a workload because it lost contact: self-fencing on a lease would make a
//! control-plane outage stop every server in the fleet (docs/fleet.md, "Epochs and fencing"). The
//! lease only governs what the node does unasked, restarting a failed workload or resuming one the
//! host went down under, so a node cut off from the control plane, which may have been replaced,
//! brings nothing back on its own.
//!
//! An answer may also offer a newer blocklyd, which the node installs and restarts into
//! (upgrade.rs); a beat accepted once reconciled ends the trial of one just installed.

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
use crate::protocol::wire::{HeartbeatRequest, HeartbeatResponse, WorkloadReport};
use crate::upgrade::Upgrader;

/// Everything this node holds, for a heartbeat.
pub fn heartbeat_report(
    manager: &Manager,
    node_id: &NodeId,
    session: &str,
    boot_id: Option<String>,
    seq: u64,
) -> HeartbeatRequest {
    let workloads = manager
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
        .collect();
    let issues = manager.host_issues();
    HeartbeatRequest {
        node_id: node_id.to_string(),
        session_id: session.to_owned(),
        boot_id,
        seq,
        daemon_version: crate::fleet::daemon_version(),
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

pub async fn run(
    manager: Arc<Manager>,
    credentials: Arc<Credentials>,
    upgrader: Arc<Upgrader>,
    control_plane: String,
    every: u64,
    cancel: CancellationToken,
) {
    let node_id = credentials.node_id();
    let url = format!("{}/fleet/v1/nodes/{node_id}/heartbeat", control_plane.trim_end_matches('/'));
    let session = uuid::Uuid::new_v4().to_string();
    let boot = crate::host::boot_id();
    let mut interval = Duration::from_secs(every);
    let mut seq: u64 = 0;
    let mut failing_since: Option<u64> = None;
    loop {
        seq += 1;
        let mut request: HeartbeatRequest = heartbeat_report(&manager, &node_id, &session, boot.clone(), seq);
        request.upgrade_failed = upgrader.failure();
        let timeout = interval.max(Duration::from_secs(2)).min(Duration::from_secs(10));
        let sent = Instant::now();
        let result =
            client::json::<_, HeartbeatResponse>(Method::POST, &url, &request, credentials.client(), timeout).await;
        match result {
            Ok(answer) => {
                if let Some(since) = failing_since.take() {
                    info!(missed = seq - since, "heartbeat: the control plane answers again");
                }
                manager.fleet_contact(Ok(Lifecycle::from(answer.lifecycle.as_str())), sent.elapsed());
                // Fences first: the lease granted below lets waiting resumes go ahead at once,
                // and none of them may be a copy this answer supersedes.
                for fence in answer.fences {
                    let Ok(id) = WorkloadId::parse(&fence.workload) else { continue };
                    match manager.fence(id.clone(), fence.current_epoch).await {
                        Ok(r) if r.changed => {
                            warn!(workload = %id, current_epoch = fence.current_epoch, "heartbeat: fenced a superseded copy")
                        }
                        Ok(_) => {}
                        Err(e) => warn!(workload = %id, error = %e, "heartbeat: couldn't fence"),
                    }
                }
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
        tokio::select! {
            () = tokio::time::sleep(interval) => {}
            () = cancel.cancelled() => return,
        }
    }
}
