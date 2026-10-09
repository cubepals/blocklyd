//! The record as a container's `LABEL_RECORD` label carries it: enough to rebuild a workload's
//! record from the runtime alone when the state directory is lost.

use crate::protocol::SpecRecord;
use crate::store::{AllocatedPort, RECORD_VERSION, WorkloadRecord};

/// The record as a label carries it.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct LabelRecord {
    pub version: u32,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    pub spec_digest: String,
    pub ports: Vec<AllocatedPort>,
    pub spec: SpecRecord,
    pub created_at: String,
}

impl LabelRecord {
    pub(super) fn of(record: &WorkloadRecord) -> Self {
        Self {
            version: RECORD_VERSION,
            generation: record.generation,
            epoch: record.epoch,
            spec_digest: record.spec_digest.clone(),
            ports: record.ports.clone(),
            spec: record.spec.clone(),
            created_at: record.created_at.clone(),
        }
    }
}
