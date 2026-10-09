//! What blocklyd does as a one-off command rather than as the daemon: `serve` and `check-config`
//! stay in main.rs, beside the setup they share.
//!
//! Parts (`cli/`):
//! - `join.rs`: makes a host a node of the fleet a pasted token names, from the token alone.
//! - `reenroll.rs`: swaps an enrolled node's identity, and its fleet CA, for one under the same id.
//! - `service.rs`: what join asks of systemd, and of the blocklyd it starts.
//! - `upgrade.rs`: `blocklyd upgrade`, the control plane's blocklyd at once; the upgrade itself is
//!   `crate::upgrade`.

pub mod join;
pub(crate) mod reenroll;
pub(crate) mod service;
pub mod upgrade;
