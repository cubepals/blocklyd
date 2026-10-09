//! What the node reports about itself: its health, and its status (host, capacity, Docker,
//! fleet contact, last reconciliation). Nothing here is about one workload; that is
//! `workload.rs`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::Issue;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct NodeHealth {
    pub status: String,
    pub docker: bool,
    pub reconciled: bool,
    /// blocklyd's version: what a control plane reads before relying on a feature.
    pub(crate) daemon_version: String,
    pub(crate) protocol: ProtocolVersions,
    pub(crate) features: Vec<String>,
    pub(crate) node_id: String,
    pub(crate) deployment_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ProtocolVersions {
    pub current: u32,
    pub supported: Vec<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct NodeStatus {
    pub(crate) node_id: String,
    pub(crate) deployment_id: String,
    /// In fleet mode: the control plane this node reports to, and when it last answered.
    pub(crate) fleet: Option<FleetView>,
    pub(crate) daemon: DaemonView,
    pub(crate) hostname: String,
    pub(crate) kernel: String,
    pub(crate) cpus: u32,
    pub(crate) load_average: Option<[f64; 3]>,
    pub(crate) memory_total_bytes: Option<u64>,
    pub(crate) memory_available_bytes: Option<u64>,
    pub(crate) disk_path: String,
    pub(crate) disk_total_bytes: Option<u64>,
    pub(crate) disk_available_bytes: Option<u64>,
    pub(crate) trash_bytes: Option<u64>,
    pub(crate) capacity: CapacityView,
    pub(crate) workloads_by_state: BTreeMap<String, u64>,
    pub(crate) docker: DockerView,
    pub(crate) reconcile: Option<ReconcileView>,
    pub issues: Vec<Issue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub(crate) struct FleetView {
    pub(crate) node_id: String,
    pub(crate) control_plane: String,
    /// active, draining, lost or retired, as the control plane last said.
    pub(crate) lifecycle: Option<String>,
    pub(crate) last_contact_at: Option<String>,
    pub(crate) last_contact_age_seconds: Option<u64>,
    pub(crate) last_error: Option<String>,
    pub(crate) last_latency_ms: Option<u64>,
    pub(crate) heartbeats_ok: u64,
    pub(crate) heartbeats_failed: u64,
    /// How long this node may still restart a failed workload on its own (see
    /// `fleet::heartbeat`). None until the control plane has granted a lease.
    pub(crate) lease_remaining_seconds: Option<u64>,
    /// When the node last renewed its certificates, in this run.
    pub(crate) certificate_renewed_at: Option<String>,
}

/// The blocklyd process itself, as distinct from the node it runs on.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub(crate) struct DaemonView {
    pub(crate) version: String,
    pub(crate) started_at: String,
    pub(crate) uptime_seconds: u64,
    pub(crate) rss_bytes: Option<u64>,
    pub(crate) cpu_seconds: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub(crate) struct CapacityView {
    /// Memory workloads may be given: total minus what the host keeps for itself.
    pub(crate) allocatable_memory_mb: u64,
    /// Memory of running workloads: what a start is admitted against.
    pub(crate) running_memory_mb: u64,
    /// Memory of every workload with compute, running or not.
    pub(crate) provisioned_memory_mb: u64,
    pub(crate) ports_total: u32,
    pub(crate) ports_allocated: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub(crate) struct DockerView {
    pub(crate) reachable: bool,
    pub(crate) version: Option<String>,
    pub(crate) api_version: Option<String>,
    pub(crate) cgroup_version: Option<String>,
    pub(crate) cgroup_driver: Option<String>,
    pub(crate) storage_driver: Option<String>,
    pub(crate) security_options: Vec<String>,
    pub(crate) live_restore: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReconcileView {
    pub(crate) finished_at: String,
    pub duration_ms: u64,
    pub workloads: u64,
    pub adopted: u64,
    pub issues: u64,
    pub error: Option<String>,
}
