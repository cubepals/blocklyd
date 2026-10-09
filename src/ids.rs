//! Identifiers that cross the protocol boundary and end up in paths, container names and labels.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize};

/// The longest id: a DNS label's limit.
const MAX_LEN: usize = 63;

/// Whether `value` is an id: 1-63 of a-z, 0-9 and '-', starting and ending with a letter or digit.
fn valid(value: &str) -> bool {
    let bytes = value.as_bytes();
    let edge_ok = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    !bytes.is_empty()
        && bytes.len() <= MAX_LEN
        && bytes.iter().all(|&b| edge_ok(b) || b == b'-')
        && edge_ok(bytes[0])
        && edge_ok(bytes[bytes.len() - 1])
}

/// An id type held to `valid`: parsed once, then a plain string everywhere it goes. `$what` names
/// it in the error a bad one gets.
macro_rules! id_type {
    ($(#[$meta:meta])* $name:ident, $invalid:ident, $what:literal) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
        #[cfg_attr(test, derive(schemars::JsonSchema))]
        #[serde(transparent)]
        pub struct $name(String);

        #[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
        #[error("a {} id is 1-63 characters of a-z, 0-9 and '-', starting and ending with a letter or digit", $what)]
        pub struct $invalid;

        impl $name {
            pub fn parse(value: &str) -> Result<Self, $invalid> {
                if valid(value) { Ok(Self(value.to_owned())) } else { Err($invalid) }
            }

            pub(crate) fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = $invalid;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Self::parse(s)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let raw = String::deserialize(deserializer)?;
                Self::parse(&raw).map_err(serde::de::Error::custom)
            }
        }
    };
}

id_type!(
    /// What the control plane calls a workload: Blockly's server id (a UUID) in practice, but any
    /// lowercase DNS label will do. The character set is the whole defence against path traversal
    /// and injection: a valid id is always one safe path component, one safe container-name suffix
    /// and one safe label value, so nothing downstream needs to escape it.
    WorkloadId,
    InvalidWorkloadId,
    "workload"
);

id_type!(
    /// A local snapshot's id: the control plane's archive id (a UUID). It names a directory, so it is
    /// held to a workload id's rules.
    SnapshotId,
    InvalidSnapshotId,
    "snapshot"
);

/// What this node is called: `node_id` in the config, or in fleet mode the id the control plane
/// gave it at enrollment. It names the node in labels, metrics and the control plane's URLs. Not
/// held to a workload id's rules: an identity enrolled before is never refused for its id, and the
/// config holds its own to `config.rs`'s.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(String);

impl NodeId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for NodeId {
    fn from(id: String) -> Self {
        Self(id)
    }
}

impl From<&str> for NodeId {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
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
