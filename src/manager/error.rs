//! What an operation on a workload fails with (`NodeError`), which the API turns into its error
//! answers, and the fencing check every mutating verb makes first (`check_epoch`).

use crate::ids::WorkloadId;
use crate::protocol::FieldError;
use crate::runtime::RuntimeError;
use crate::store::WorkloadRecord;

#[derive(Debug, Clone, thiserror::Error)]
pub enum NodeError {
    #[error("the request is not valid")]
    Invalid(Vec<FieldError>),
    #[error("no workload {0}")]
    NotFound(WorkloadId),
    #[error("no snapshot {0} of this workload on this node")]
    SnapshotNotFound(String),
    #[error("the workload's current spec is not the one the request expected")]
    PreconditionFailed { current: Option<String> },
    #[error("{message}")]
    Conflict { code: ConflictCode, message: String },
    #[error("{0}")]
    InsufficientCapacity(String),
    #[error("{0}")]
    InsufficientDisk(String),
    #[error("{0}")]
    NoFreePorts(String),
    #[error("deleting data needs the {} header set to the workload id", crate::protocol::CONFIRM_DELETE_HEADER)]
    ConfirmationRequired,
    #[error("this idempotency key was used for a different request")]
    IdempotencyMismatch,
    #[error("{0}")]
    RuntimeUnavailable(String),
    #[error("{0}")]
    Timeout(String),
    #[error("the container runtime refused: {0}")]
    Runtime(String),
    #[error("{0}")]
    Internal(String),
    #[error("this request acts for epoch {asked}; this copy belongs to epoch {current:?}, which is newer")]
    StaleEpoch { asked: u64, current: Option<u64> },
    #[error("this copy belongs to epoch {current:?}; PUT the spec with epoch {asked} first")]
    EpochAhead { asked: u64, current: Option<u64> },
    #[error("this copy was superseded by epoch {by}; it will not run again")]
    Superseded { by: u64 },
    #[error("this workload belongs to epoch {current}; send it in the {} header", crate::protocol::EPOCH_HEADER)]
    EpochRequired { current: u64 },
    #[error("{0}")]
    InvalidArchive(String),
    #[error("the archive's sha256 is {actual}, not {expected}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("{0}")]
    Transfer(String),
    #[error("the archive is {size_bytes} bytes; one upload to the store carries at most {limit_bytes}")]
    ArchiveTooLarge { size_bytes: u64, limit_bytes: u64 },
}

/// Why a verb can't act on the workload as it is: the `code` of its 409 answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConflictCode {
    ContainerMissing,
    NameTaken,
    NoCompute,
    NotCreated,
    NotQuiesced,
    NotRunning,
    NotStopped,
    PortConflict,
    SnapshotBusy,
}

impl ConflictCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ContainerMissing => "container_missing",
            Self::NameTaken => "name_taken",
            Self::NoCompute => "no_compute",
            Self::NotCreated => "not_created",
            Self::NotQuiesced => "not_quiesced",
            Self::NotRunning => "not_running",
            Self::NotStopped => "not_stopped",
            Self::PortConflict => "port_conflict",
            Self::SnapshotBusy => "snapshot_busy",
        }
    }
}

/// How a verb treats the placement epoch it is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochRule {
    /// Runs the copy (start, exec): only for exactly its epoch, and never once superseded.
    Exact,
    /// Tears the copy down (stop, kill, delete): its epoch or any newer one, since a newer
    /// placement may always clean up an older copy.
    Teardown,
    /// Places the workload here (ensure): its epoch, or a newer one that takes the copy over.
    Place,
}

/// The fencing check every mutating verb makes before touching the runtime. Workloads made
/// without an epoch keep the single-node protocol: none is asked for and none is checked.
pub fn check_epoch(record: &WorkloadRecord, asked: Option<u64>, rule: EpochRule) -> Result<(), NodeError> {
    match (asked, record.epoch) {
        (None, None) => {}
        (None, Some(current)) => return Err(NodeError::EpochRequired { current }),
        (Some(asked), current) => {
            let floor = current.unwrap_or(0);
            if asked < floor {
                return Err(NodeError::StaleEpoch { asked, current });
            }
            if asked > floor && rule == EpochRule::Exact {
                return Err(NodeError::EpochAhead { asked, current });
            }
        }
    }
    if let Some(by) = record.superseded_by {
        let revived = rule == EpochRule::Place && asked.is_some_and(|a| a > by);
        if rule != EpochRule::Teardown && !revived {
            return Err(NodeError::Superseded { by });
        }
    }
    Ok(())
}

impl From<RuntimeError> for NodeError {
    fn from(e: RuntimeError) -> Self {
        match e {
            RuntimeError::Unavailable(m) => NodeError::RuntimeUnavailable(m),
            RuntimeError::Timeout(m) => NodeError::Timeout(m),
            other => NodeError::Runtime(other.to_string()),
        }
    }
}

impl From<crate::store::StoreError> for NodeError {
    fn from(e: crate::store::StoreError) -> Self {
        NodeError::Internal(e.to_string())
    }
}
