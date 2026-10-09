//! Identifiers that cross the protocol boundary and end up in paths, container names and labels.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

/// What the control plane calls a workload: Blockly's server id (a UUID) in practice, but any
/// lowercase DNS label will do. The character set is the whole defence against path traversal
/// and injection: a valid id is always one safe path component, one safe container-name suffix
/// and one safe label value, so nothing downstream needs to escape it.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct WorkloadId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a workload id is 1-63 characters of a-z, 0-9 and '-', starting and ending with a letter or digit")]
pub struct InvalidWorkloadId;

impl WorkloadId {
    pub const MAX_LEN: usize = 63;

    pub fn parse(value: &str) -> Result<Self, InvalidWorkloadId> {
        let bytes = value.as_bytes();
        let edge_ok = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
        let valid = !bytes.is_empty()
            && bytes.len() <= Self::MAX_LEN
            && bytes.iter().all(|&b| edge_ok(b) || b == b'-')
            && edge_ok(bytes[0])
            && edge_ok(bytes[bytes.len() - 1]);
        if valid { Ok(Self(value.to_owned())) } else { Err(InvalidWorkloadId) }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for WorkloadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for WorkloadId {
    type Err = InvalidWorkloadId;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl<'de> Deserialize<'de> for WorkloadId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// A local snapshot's id: the control plane's archive id (a UUID). It names a directory, so it is
/// held to a workload id's rules.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(transparent)]
pub struct SnapshotId(String);

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("a snapshot id is 1-63 characters of a-z, 0-9 and '-', starting and ending with a letter or digit")]
pub struct InvalidSnapshotId;

impl SnapshotId {
    pub fn parse(value: &str) -> Result<Self, InvalidSnapshotId> {
        WorkloadId::parse(value).map(|w| Self(w.0)).map_err(|_| InvalidSnapshotId)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for SnapshotId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_uuids_and_labels() {
        for ok in ["0b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10", "a", "mc-1", "x9"] {
            assert!(WorkloadId::parse(ok).is_ok(), "{ok}");
        }
    }

    #[test]
    fn refuses_anything_that_could_escape_a_path_or_a_name() {
        for bad in [
            "",
            "-a",
            "a-",
            "A",
            "../etc",
            "..",
            ".",
            "a/b",
            "a b",
            "a\0b",
            "a;rm -rf /",
            "$(id)",
            "a.b",
            "a_b",
            "é",
            &"a".repeat(64),
        ] {
            assert!(WorkloadId::parse(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn deserializing_validates() {
        assert!(serde_json::from_str::<WorkloadId>("\"ok-1\"").is_ok());
        assert!(serde_json::from_str::<WorkloadId>("\"../x\"").is_err());
    }
}
