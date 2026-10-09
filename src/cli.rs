//! What blocklyd does as a one-off command rather than as the daemon: `serve` and `check-config`
//! stay in main.rs, beside the setup they share.
//!
//! Parts (`cli/`):
//! - `join.rs`: makes a host a node of the fleet a pasted token names, from the token alone.
//! - `reenroll.rs`: swaps an enrolled node's identity, and its fleet CA, for one under the same id.
//! - `service.rs`: what join asks of systemd, and of the blocklyd it starts.
//! - `upgrade.rs`: blocklyd replacing itself with its control plane's, and going back if it fails.

pub mod join;
pub mod reenroll;
pub mod service;
pub mod upgrade;
