//! Protocol v1: what the control plane sends and what blocklyd answers.
//!
//! The vocabulary is workloads, not containers. A workload is a spec (image, environment,
//! resources, storage, ports, stop) blocklyd keeps running or stopped on this host. Nothing
//! here names Docker, so blocklyd could move to containerd or microVMs under the same
//! protocol. It mirrors Blockly's `RuntimeSpec` (apps/control/src/app/ports/runtime.ts) closely
//! on purpose: an `infra/fleet` adapter would translate field for field.
//!
//! Requests are strict (`deny_unknown_fields`): a node daemon that silently ignored a field a newer
//! control plane relies on — a limit, say — would be worse than one that refuses. Responses only
//! ever grow; clients must ignore fields they don't know. New request fields are announced in
//! `NodeHealth::features` before anyone sends them.
//!
//! Parts (`protocol/`):
//! - `spec.rs`: what a workload is, as the control plane sends it and as blocklyd keeps it.
//! - `workload.rs`: what blocklyd reports about one workload: its view, stats and log lines.
//! - `lifecycle.rs`: the verbs that create, start, stop, fence and delete a workload.
//! - `transfer.rs`: moving a workload's data: export, restore, snapshots and their upload.
//! - `exec.rs`: running one command inside a workload.
//! - `node.rs`: what the node reports about itself.
//! - `wire.rs`: what a node and its control plane send each other: enrollment, heartbeats, renewal.
//! - `schema.rs`: tests only; these types as the OpenAPI documents the control plane's TypeScript
//!   types are generated from.

use serde::{Deserialize, Serialize};

mod exec;
mod lifecycle;
mod node;
#[cfg(test)]
mod schema;
mod spec;
mod transfer;
pub mod wire;
mod workload;

pub use exec::*;
pub use lifecycle::*;
pub use node::*;
pub use spec::*;
pub use transfer::*;
pub use workload::*;

pub const PROTOCOL_VERSION: u32 = 1;
/// Every protocol version this blocklyd speaks, PROTOCOL_VERSION among them.
pub const SUPPORTED_VERSIONS: &[u32] = &[1];
/// Sent on every response so a client can tell which protocol answered.
pub const PROTOCOL_HEADER: &str = "blocklyd-protocol";
/// Must echo the workload id for `DELETE …?data=delete`: data never goes on a single typo.
pub const CONFIRM_DELETE_HEADER: &str = "x-blockly-confirm-delete-data";
/// Features a client may rely on, beyond protocol v1's base. Only ever added to (`features_only_grow`).
pub const FEATURES: &[&str] = &[
    "exec-idempotency-key",
    "logs-follow-across-restarts",
    "conditional-put",
    "placement-epochs",
    "data-transfer",
    "local-snapshots",
    "export-exclude",
    "execution-lease",
    "certificate-renewal",
    "resume-after-reboot",
    "multipart-upload",
    "self-upgrade",
];

impl ProtocolVersions {
    /// This build's, as enrollment, heartbeats and `/v1/health` say them.
    pub fn ours() -> Self {
        ProtocolVersions { current: PROTOCOL_VERSION, supported: SUPPORTED_VERSIONS.to_vec() }
    }
}

/// FEATURES, in order, as enrollment, heartbeats and `/v1/health` say them.
pub fn features() -> Vec<String> {
    FEATURES.iter().map(|f| (*f).to_owned()).collect()
}

/// The placement epoch a mutating request acts for: a fencing token the control plane issues,
/// bumped each time a workload is placed anew (moved, recovered, or brought back). A node refuses
/// a request older than the copy it holds, so a delayed or stale caller can't act on it.
pub const EPOCH_HEADER: &str = "blocklyd-epoch";

/// Label keys blocklyd writes itself; a spec may not set anything under this prefix, so a
/// request can never forge ownership of a container.
pub const RESERVED_LABEL_PREFIX: &str = "blocklyd.";

/// One thing wrong with a request, by the JSON path of the field.
#[derive(Clone, Debug, Serialize, PartialEq, Eq)]
pub struct FieldError {
    pub field: String,
    pub problem: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ErrorBody {
    pub error: ErrorDetail,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
pub struct ErrorDetail {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn this_build_speaks_its_own_protocol_version() {
        let ours = ProtocolVersions::ours();
        assert_eq!(ours.current, PROTOCOL_VERSION);
        assert!(ours.supported.contains(&PROTOCOL_VERSION));
        // What enrollment, heartbeats and /v1/health have always sent.
        assert_eq!(serde_json::to_value(&ours).unwrap(), serde_json::json!({ "current": 1, "supported": [1] }));
        assert_eq!(features(), FEATURES);
    }

    /// The control plane stops placing on, or upgrading, a node without a feature it needs, so a
    /// feature once shipped stays: these are all the ones shipped so far.
    #[test]
    fn features_only_grow() {
        let shipped = [
            "exec-idempotency-key",
            "logs-follow-across-restarts",
            "conditional-put",
            "placement-epochs",
            "data-transfer",
            "local-snapshots",
            "export-exclude",
            "execution-lease",
            "certificate-renewal",
            "resume-after-reboot",
            "multipart-upload",
            "self-upgrade",
        ];
        for feature in shipped {
            assert!(FEATURES.contains(&feature), "{feature} shipped: control planes rely on it, so it stays");
        }
    }
}
