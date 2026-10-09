//! The verbs that decide whether a workload exists and runs: ensure, start and stop, fence and
//! delete, each with its request and its answer. The workload view these answers carry is in
//! `workload.rs`. Moving a workload's data is in `transfer.rs`.

use serde::{Deserialize, Serialize};

use super::WorkloadView;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum EnsureOutcome {
    /// Nothing existed; now it does (stopped).
    Created,
    /// It already matched; nothing was touched.
    Unchanged,
    /// Made again for the new spec, keeping data and ports; started again if it was running.
    Replaced,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct EnsureResponse {
    pub outcome: EnsureOutcome,
    pub restarted: bool,
    pub workload: WorkloadView,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PowerResponse {
    /// False when it was already in the asked-for state: a repeat is a no-op, not an error.
    pub changed: bool,
    /// For stop: the workload didn't exit within the grace and was killed.
    #[serde(default)]
    pub forced: bool,
    pub workload: WorkloadView,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct StopRequest {
    /// Overrides the spec's grace for this stop.
    #[serde(default)]
    pub(crate) timeout_seconds: Option<u32>,
}

/// `POST /v1/workloads/{id}/fence`: the control plane says which epoch is current. A copy
/// older than it is stopped and never started again; a copy at or past it is left alone, so a
/// late or repeated fence is harmless.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FenceRequest {
    pub(crate) current_epoch: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct FenceResponse {
    /// False when this copy wasn't older than the current epoch, or was already fenced.
    pub changed: bool,
    /// The copy was running and has been stopped.
    pub stopped: bool,
    pub workload: WorkloadView,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataDisposition {
    Keep,
    Delete,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum DataOutcome {
    /// Left in place; the workload is `retained` and a PUT brings its compute back.
    Kept,
    /// Moved to the host's trash; purged after the retention (the last chance to undo).
    Trashed,
    /// There was no data.
    Absent,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct DeleteResponse {
    /// False when nothing by this id existed: a repeated delete is a no-op.
    pub existed: bool,
    pub removed_container: bool,
    pub data: DataOutcome,
    pub trash_path: Option<String>,
}
