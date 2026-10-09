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
    pub daemon_version: String,
    pub protocol: ProtocolVersions,
    pub features: Vec<String>,
    pub node_id: String,
    pub deployment_id: String,
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
    pub node_id: String,
    pub deployment_id: String,
    /// In fleet mode: the control plane this node reports to, and when it last answered.
    pub fleet: Option<FleetView>,
    pub daemon: DaemonView,
    pub hostname: String,
    pub kernel: String,
    pub cpus: u32,
    pub load_average: Option<[f64; 3]>,
    pub memory_total_bytes: Option<u64>,
    pub memory_available_bytes: Option<u64>,
    pub disk_path: String,
    pub disk_total_bytes: Option<u64>,
    pub disk_available_bytes: Option<u64>,
    pub trash_bytes: Option<u64>,
    pub capacity: CapacityView,
    pub workloads_by_state: BTreeMap<String, u64>,
    pub docker: DockerView,
    pub reconcile: Option<ReconcileView>,
    pub issues: Vec<Issue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FleetView {
    pub node_id: String,
    pub control_plane: String,
    /// active, draining, lost or retired, as the control plane last said.
    pub lifecycle: Option<String>,
    pub last_contact_at: Option<String>,
    pub last_contact_age_seconds: Option<u64>,
    pub last_error: Option<String>,
    pub last_latency_ms: Option<u64>,
    pub heartbeats_ok: u64,
    pub heartbeats_failed: u64,
    /// How long this node may still restart a failed workload on its own (see
    /// `fleet::heartbeat`). None until the control plane has granted a lease.
    pub lease_remaining_seconds: Option<u64>,
    /// When the node last renewed its certificates, in this run.
    pub certificate_renewed_at: Option<String>,
}

/// The blocklyd process itself, as distinct from the node it runs on.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DaemonView {
    pub version: String,
    pub started_at: String,
    pub uptime_seconds: u64,
    pub rss_bytes: Option<u64>,
    pub cpu_seconds: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct CapacityView {
    /// Memory workloads may be given: total minus what the host keeps for itself.
    pub allocatable_memory_mb: u64,
    /// Memory of running workloads: what a start is admitted against.
    pub running_memory_mb: u64,
    /// Memory of every workload with compute, running or not.
    pub provisioned_memory_mb: u64,
    pub ports_total: u32,
    pub ports_allocated: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DockerView {
    pub reachable: bool,
    pub version: Option<String>,
    pub api_version: Option<String>,
    pub cgroup_version: Option<String>,
    pub cgroup_driver: Option<String>,
    pub storage_driver: Option<String>,
    pub security_options: Vec<String>,
    pub live_restore: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ReconcileView {
    pub finished_at: String,
    pub duration_ms: u64,
    pub workloads: u64,
    pub adopted: u64,
    pub issues: u64,
    pub error: Option<String>,
}
