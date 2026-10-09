//! blocklyd: Blockly's node daemon. It runs Blockly's Minecraft servers on a Linux host through
//! Docker, for a control plane that places them: alone on one host, or as one node of many. See
//! README.md, and docs/protocol.md for the API.

pub mod api;
pub mod certs;
pub mod cli;
pub mod config;
pub mod doctor;
pub mod durable;
pub mod fleet;
pub mod host;
pub mod ids;
pub mod infer;
pub mod manager;
pub mod metrics;
pub mod ports;
pub mod protocol;
pub mod reconcile;
pub mod runtime;
pub mod store;
pub mod transfer;
pub mod tree;
