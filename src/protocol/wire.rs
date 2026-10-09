//! What a node sends the control plane, and what comes back: enrollment once, then a heartbeat
//! every few seconds. JSON, camelCase, like the node's own API.
//!
//! A heartbeat carries everything the node holds, every time. A node that was cut off, restarted
//! or rebooted therefore reports its whole state on its first beat back, and the control plane
//! never has to ask what it missed. The answer fences, and grants the execution lease: a
//! heartbeat never makes a node create or start anything it was asked to, only allows it to
//! keep its own restart policy.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use serde::{Deserialize, Serialize};

use super::{ExitInfo, Issue, ProtocolVersions, WorkloadState};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct EnrollRequest {
    pub token: String,
    /// PKCS#10, PEM. The key never leaves the node.
    pub csr_pem: String,
    pub(crate) facts: NodeFacts,
}

/// What the node says about itself when it joins. The control plane records it; the token, not
/// these facts, decides whether the node may join.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct NodeFacts {
    pub hostname: String,
    /// The kernel's boot id: a new one means the host rebooted.
    pub boot_id: Option<String>,
    /// sha256 of /etc/machine-id. Two nodes with the same one are clones of one image.
    pub machine_id_sha256: Option<String>,
    pub daemon_version: String,
    pub protocol: ProtocolVersions,
    pub features: Vec<String>,
    /// Where the control plane dials this node's API.
    pub api_address: SocketAddr,
    /// Where the edge reaches game ports, and the control plane console and status ports.
    pub edge_ips: Vec<IpAddr>,
    pub control_ips: Vec<IpAddr>,
    pub capacity: NodeCapacity,
    pub labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct EnrollResponse {
    /// The node's durable identity, issued by the control plane.
    pub node_id: String,
    pub deployment_id: String,
    /// serverAuth, for the node's API; SAN `<nodeId>.nodes.<deployment>.fleet`.
    pub server_cert_pem: String,
    /// clientAuth, for heartbeats; the same key.
    pub client_cert_pem: String,
    pub ca_pem: String,
    /// Client names allowed to call the node's API.
    pub allowed_clients: Vec<String>,
    pub heartbeat_seconds: u64,
}

/// Physical, reserved and allocated capacity, and what is actually used. Placement decides on
/// the control plane's own ledger; this is what the node sees, to check it against.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct NodeCapacity {
    pub memory_total_mb: u64,
    pub reserved_memory_mb: u64,
    pub allocatable_memory_mb: u64,
    /// Memory of every workload that holds compute here, running or not.
    pub provisioned_memory_mb: u64,
    /// Memory of running workloads: what a start is admitted against.
    pub running_memory_mb: u64,
    /// The working set of running workloads, as the kernel counts it, from each one's latest
    /// sample. A workload that runs but hasn't been sampled yet is left out; null only while
    /// workloads run and none has been.
    pub used_memory_bytes: Option<u64>,
    pub cpus: u32,
    pub reserved_cpu_millis: u64,
    /// Cores running workloads use, likewise.
    pub used_cpu_cores: Option<f64>,
    pub load_average: Option<[f64; 3]>,
    pub disk_total_bytes: Option<u64>,
    pub disk_available_bytes: Option<u64>,
    pub min_free_disk_mb: u64,
    /// What local snapshots hold on the data disk, as their files count it (shared blocks are
    /// counted in full, so it overstates what reflinked snapshots really take).
    #[serde(default)]
    pub snapshot_bytes: Option<u64>,
    /// The data's filesystem shares blocks between copies: snapshots are nearly free.
    #[serde(default)]
    pub reflink: Option<bool>,
    pub ports_total: u32,
    pub ports_allocated: u32,
}

/// Where the node is reached, as its configuration says now.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub(crate) struct NodeAddresses {
    /// blocklyd's API, `host:port`.
    pub(crate) api: String,
    /// Where the edge reaches game ports.
    pub(crate) edge: String,
    /// Where the control plane reaches console and status ports.
    pub(crate) control: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatRequest {
    pub(crate) node_id: String,
    /// New each time blocklyd starts. Two sessions beating for one node id at once are two
    /// hosts with one identity.
    pub(crate) session_id: String,
    pub(crate) boot_id: Option<String>,
    /// Counts up within a session, so a late or replayed beat can be told apart.
    pub(crate) seq: u64,
    pub(crate) daemon_version: String,
    pub(crate) protocol: ProtocolVersions,
    pub(crate) features: Vec<String>,
    /// Docker answers.
    pub(crate) runtime_up: bool,
    /// A full pass has looked since the runtime last didn't answer: the states below are current.
    pub(crate) reconciled: bool,
    pub(crate) capacity: NodeCapacity,
    pub workloads: Vec<WorkloadReport>,
    pub(crate) issues: Vec<Issue>,
    /// Where the node is reached now; the control plane follows a change.
    #[serde(default)]
    pub(crate) addresses: Option<NodeAddresses>,
    /// An upgrade this node gave up on, going back to the binary it had (cli/upgrade.rs).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) upgrade_failed: Option<UpgradeFailure>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UpgradeFailure {
    pub version: String,
    pub reason: String,
}

/// The blocklyd the control plane wants the node to run, at `GET /fleet/v1/blocklyd`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct UpgradeOffer {
    pub(crate) version: String,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct WorkloadReport {
    pub id: String,
    pub(crate) epoch: Option<u64>,
    pub(crate) superseded_by: Option<u64>,
    pub(crate) state: WorkloadState,
    pub(crate) spec_digest: String,
    pub(crate) generation: u64,
    pub(crate) memory_mb: u32,
    pub restart_count: u32,
    pub(crate) exit: Option<ExitInfo>,
    pub(crate) last_failure_at: Option<String>,
    pub(crate) changed_at: String,
    /// Port name → host port.
    pub(crate) ports: BTreeMap<String, u16>,
    /// What the node found wrong with the workload and can't mend itself: a restart refused for
    /// want of room, data over the size its spec promised. Left out when there is none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<Issue>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct HeartbeatResponse {
    /// The node as the control plane holds it: active, draining, lost or retired.
    pub lifecycle: String,
    /// Copies this node holds that a newer placement superseded. The node stops each and never
    /// starts it again.
    #[serde(default)]
    pub fences: Vec<Fence>,
    /// A new interval, if the control plane wants one.
    #[serde(default)]
    pub heartbeat_seconds: Option<u64>,
    /// The execution lease: for this long after sending the heartbeat, the node may restart a
    /// workload that failed on its own, or resume one the host went down under. 0 for a node the
    /// control plane holds lost.
    #[serde(default)]
    pub lease_seconds: Option<u64>,
    /// The node's certificates end soon: it should renew them.
    #[serde(default)]
    pub renew: bool,
    /// A newer blocklyd to upgrade to: offered to one node of a region at a time.
    #[serde(default)]
    pub upgrade: Option<UpgradeOffer>,
}

/// `POST /fleet/v1/nodes/{id}/renew`, over the node's current client certificate: a CSR for a new
/// key. The node keeps its id; only its certificates and key change.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RenewRequest {
    pub node_id: String,
    pub csr_pem: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RenewResponse {
    pub server_cert_pem: String,
    pub client_cert_pem: String,
    pub ca_pem: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Fence {
    pub(crate) workload: String,
    pub(crate) current_epoch: u64,
}
