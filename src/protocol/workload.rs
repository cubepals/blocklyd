//! What blocklyd reports about one workload: its view (state, health, ports, storage, issues),
//! the list of every view, its resource stats, and the lines of its log stream.
//! It holds no verb's request: those are in `lifecycle.rs`, `transfer.rs` and `exec.rs`. What the
//! node reports about itself is in `node.rs`.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::{Proto, Resources};
use crate::ids::WorkloadId;

/// What a workload is doing, normalized. How it maps onto Blockly's `ObservedState`:
/// created/stopped → stopped · running → running · stopping → stopping · crashed → crashed ·
/// restarting → starting (with `lastFailureAt`) · missing/retained → absent · creating → stopped
/// · unknown → unknown · fenced → stopped (this copy lost its placement; the current one is
/// elsewhere).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum WorkloadState {
    /// Accepted; its container is being made.
    Creating,
    /// Made and never started, or started again since.
    Created,
    Running,
    /// A requested stop is in progress.
    Stopping,
    /// Exited after a requested stop, or on its own with code 0.
    Stopped,
    /// Exited on its own with a failure, or killed by the kernel for memory.
    Crashed,
    /// The host is starting it again after a failure (restart policy).
    Restarting,
    /// blocklyd holds a record but the container is gone (the runtime lost it).
    Missing,
    /// Compute removed on request, data kept (`DELETE ?data=keep`).
    Retained,
    /// The runtime can't be asked (Docker down).
    Unknown,
    /// Superseded: the control plane placed this workload anew (a newer epoch, usually on another
    /// node). This copy is stopped, never started again, and its data kept until deleted.
    Fenced,
}

impl WorkloadState {
    pub(crate) const ALL: [WorkloadState; 11] = [
        Self::Creating,
        Self::Created,
        Self::Running,
        Self::Stopping,
        Self::Stopped,
        Self::Crashed,
        Self::Restarting,
        Self::Missing,
        Self::Retained,
        Self::Unknown,
        Self::Fenced,
    ];
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Created => "created",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Crashed => "crashed",
            Self::Restarting => "restarting",
            Self::Missing => "missing",
            Self::Retained => "retained",
            Self::Unknown => "unknown",
            Self::Fenced => "fenced",
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub(crate) enum Health {
    Starting,
    Healthy,
    Unhealthy,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ExitInfo {
    pub code: i64,
    pub oom_killed: bool,
    pub(crate) at: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PortView {
    pub(crate) name: String,
    pub(crate) protocol: Proto,
    pub(crate) container_port: u16,
    pub host_port: u16,
    /// `host:port` per audience, `[v6]:port` for IPv6: where the edge and the control plane
    /// reach this port. Blockly's `endpoint(handle, port, audience)`.
    pub(crate) endpoints: Endpoints,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Default)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub(crate) struct Endpoints {
    pub(crate) edge: Vec<String>,
    pub(crate) control: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub(crate) struct StorageView {
    pub(crate) mount_path: String,
    pub(crate) size_gb: u32,
    pub(crate) used_bytes: Option<u64>,
    pub(crate) measured_at: Option<String>,
}

/// Something reconciliation or an operation found that the control plane should know about.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Issue {
    #[cfg_attr(test, schemars(with = "String"))]
    pub code: IssueCode,
    pub detail: String,
}

impl Issue {
    pub(crate) fn new(code: IssueCode, detail: impl Into<String>) -> Self {
        Self { code, detail: detail.into() }
    }
}

/// What an `Issue` is about, as its code says it on the wire.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum IssueCode {
    ContainerMissing,
    CreateIncomplete,
    DigestMismatch,
    DuplicateContainer,
    InsufficientCapacity,
    LiveRestoreOff,
    NetworkNotIsolated,
    OrphanData,
    OverStorage,
    PortConflict,
    PortMismatch,
    PortOutOfRange,
    RecordRebuilt,
    RestoreFinished,
    UnexpectedContainer,
    UnlabelledContainer,
    UnreadableRecord,
    UnreadableSnapshot,
    UnrecoverableContainer,
}

/// Where an operator finds it on the host. Informational; never a contract.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct Locate {
    pub container_name: String,
    pub container_id: Option<String>,
    pub data_dir: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct WorkloadView {
    pub(crate) id: WorkloadId,
    /// Bumped each time the container is made again for a new spec.
    pub generation: u64,
    /// The placement epoch this copy belongs to, if the control plane gave one.
    pub epoch: Option<u64>,
    /// Set once a newer placement exists: the epoch that superseded this copy.
    pub superseded_by: Option<u64>,
    pub spec_digest: String,
    pub(crate) image: String,
    pub state: WorkloadState,
    pub(crate) health: Option<Health>,
    pub exit: Option<ExitInfo>,
    /// Restarts the host made after failures since the last requested start or new spec. Kept
    /// across restarts of blocklyd.
    pub restart_count: u32,
    /// When it last failed and the host started it again (Blockly's `failedAt`).
    pub last_failure_at: Option<String>,
    pub started_at: Option<String>,
    pub(crate) finished_at: Option<String>,
    /// When the state last changed, as the runtime recorded it (survives blocklyd restarts).
    pub(crate) changed_at: String,
    pub ports: Vec<PortView>,
    pub(crate) resources: Resources,
    pub(crate) storage: StorageView,
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) secret_names: Vec<String>,
    pub issues: Vec<Issue>,
    pub locate: Locate,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct StatsView {
    pub(crate) at: String,
    pub(crate) state: WorkloadState,
    pub(crate) uptime_seconds: Option<u64>,
    /// Cores in use over the last sampling interval (1.0 = one full core).
    pub(crate) cpu_cores: Option<f64>,
    pub(crate) cpu_seconds_total: Option<f64>,
    pub(crate) cpu_throttled_periods: Option<u64>,
    pub(crate) cpu_limit_millis: Option<u32>,
    pub(crate) memory_bytes: Option<u64>,
    /// Usage minus inactive page cache: what the kernel would have to reclaim to fit.
    pub(crate) memory_working_set_bytes: Option<u64>,
    pub(crate) memory_limit_bytes: Option<u64>,
    pub(crate) pids: Option<u64>,
    pub(crate) pids_limit: Option<u64>,
    pub(crate) network_rx_bytes: Option<u64>,
    pub(crate) network_tx_bytes: Option<u64>,
    pub(crate) data_used_bytes: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub(crate) struct ListResponse {
    pub(crate) workloads: Vec<WorkloadView>,
    pub(crate) observed_at: String,
}

/// One line of `GET …/logs` (NDJSON): a log line, or the event that ends a stream.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(untagged)]
pub enum LogRecord {
    Line { ts: Option<String>, stream: String, line: String },
    Event { event: String, reason: String },
}
