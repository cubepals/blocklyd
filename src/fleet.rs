//! Fleet mode: how a node joins a control plane and reports to it.
//!
//! Parts (`fleet/`):
//! - `token.rs`: the enrollment token's two forms, the pasted `bk1.` one and the bare secret.
//! - `enroll.rs`: a one-time token plus a certificate request gives a durable identity.
//! - `identity.rs`: that identity on disk, and its renewal; the TLS settings it gives.
//! - `heartbeat.rs`: the node's whole state, every few seconds; the answer can only fence.
//!
//! What a node and its control plane send each other is `protocol::wire`.
//!
//! Upgrading blocklyd itself when a heartbeat's answer offers it is `crate::upgrade`, which also
//! says which version this one is.
//!
//! The control plane keeps the durable state (nodes, placements, epochs, backups). The node keeps
//! only what it needs to act without it: its identity, its workloads' records, their data.

pub mod enroll;
pub mod heartbeat;
pub mod identity;
pub(crate) mod token;
pub use crate::protocol::wire;
