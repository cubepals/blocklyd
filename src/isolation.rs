// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Keeps workloads from dialling what only the edge and the control plane may. Docker's
//! inter-container switch (`enable_icc=false`, runtime/docker.rs) stops a workload reaching another
//! by its own address, but not by its published port: on this host Docker's proxy takes the
//! connection on the host's address and hands it on, and on another node it arrives as the edge's
//! does. A server's own code could then join another server past the edge, which is the one place
//! that takes out what only the edge may send: a forwarded identity (Floodgate's, BungeeCord's) and
//! a PROXY header naming the player.
//!
//! The rules are one nftables table, put in place whole by `nft -f`, so each run replaces what the
//! last left. Of the new connections from the workloads' bridge, it drops:
//! - any to this host (the input hook): every port it publishes, its API, anything else it serves;
//! - any to the link-local range, where clouds serve instance metadata;
//! - one to a private address on the ports blocklyd hands out or on its API's port: another node's
//!   servers and its blocklyd.
//!
//! Replies, and what the edge and the control plane dial, never come from the bridge, so they pass.
//! What a server is documented to dial (the artifact endpoint, the archive store, Mojang, the mod
//! hosts) passes too, unless it sits on one of those ports of a private address.
//!
//! `blocklyd isolate` applies it as root, before every start of the unit (`ExecStartPre=+`): the
//! daemon's own sandbox has no netlink to do it with. Not for making the network
//! (runtime/docker.rs), which names its bridge for this (`bridge_name`).

use std::io::Write;
use std::ops::RangeInclusive;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, anyhow, ensure};
use tracing::warn;

use crate::config::Config;
use crate::runtime::docker::{DockerRuntime, bridge_name};

/// The table blocklyd owns. Nothing else of the host's firewall is touched.
pub const TABLE: &str = "blocklyd";

/// Private IPv4: RFC 1918's ranges, and RFC 6598's shared space, where WireGuard and Tailscale
/// overlays live (as infer.rs counts them).
const PRIVATE_V4: &str = "10.0.0.0/8, 100.64.0.0/10, 172.16.0.0/12, 192.168.0.0/16";
/// Private IPv6: unique local addresses (RFC 4193) and link-local ones (RFC 4291).
const PRIVATE_V6: &str = "fc00::/7, fe80::/10";
/// IPv4 link-local (RFC 3927), where 169.254.169.254 serves instance metadata on most clouds.
const LINK_LOCAL_V4: &str = "169.254.0.0/16";

/// The rules for a bridge, the ports blocklyd hands out and its API's port, as `nft -f` reads them.
/// The first two lines make the table if it is missing, so the third can always remove it.
pub fn ruleset(table: &str, bridge: &str, ports: &RangeInclusive<u16>, api_port: u16) -> String {
    let served = if ports.contains(&api_port) { span(ports) } else { format!("{}, {api_port}", span(ports)) };
    let from = format!("iifname \"{bridge}\"");
    let to_served = format!("meta l4proto {{ tcp, udp }} th dport {{ {served} }} ct state new drop");
    format!(
        "table inet {table}\n\
         delete table inet {table}\n\
         table inet {table} {{\n\
         \tchain input {{\n\
         \t\ttype filter hook input priority filter - 10; policy accept;\n\
         \t\t{from} ct state new drop\n\
         \t}}\n\
         \tchain forward {{\n\
         \t\ttype filter hook forward priority filter - 10; policy accept;\n\
         \t\t{from} ip daddr {LINK_LOCAL_V4} ct state new drop\n\
         \t\t{from} ip daddr {{ {PRIVATE_V4} }} {to_served}\n\
         \t\t{from} ip6 daddr {{ {PRIVATE_V6} }} {to_served}\n\
         \t}}\n\
         }}\n"
    )
}

/// A range of ports as nftables writes one: a single port alone, since a range whose ends are the
/// same is refused.
fn span(ports: &RangeInclusive<u16>) -> String {
    if ports.start() == ports.end() { ports.start().to_string() } else { format!("{}-{}", ports.start(), ports.end()) }
}

/// Puts rules from `ruleset` in place, in one transaction: they all hold, or nothing changed.
pub fn apply(rules: &str) -> anyhow::Result<()> {
    let mut nft = Command::new("nft")
        .args(["-f", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| match e.kind() {
            std::io::ErrorKind::NotFound => anyhow!("nft isn't installed: apt-get install nftables"),
            _ => anyhow::Error::new(e).context("starting nft"),
        })?;
    nft.stdin.take().context("nft's input")?.write_all(rules.as_bytes()).context("writing to nft")?;
    let done = nft.wait_with_output().context("waiting for nft")?;
    ensure!(done.status.success(), "nft refused the rules: {}", String::from_utf8_lossy(&done.stderr).trim());
    Ok(())
}

/// `blocklyd isolate`: the rules for the configuration at `path`, in place. Says what it did.
pub async fn isolate(path: &Path) -> anyhow::Result<String> {
    let config = Config::load(path)?;
    let network = &config.docker.network;
    // A network made before its bridge was named for it keeps the name Docker gave it. Docker down
    // (it may still be starting) is no reason to start without rules: the network blocklyd makes
    // once it is up gets the name these name.
    let bridge = match DockerRuntime::new(&config.docker.socket)?.bridge_of(network).await {
        Ok(Some(bridge)) => bridge,
        Ok(None) => bridge_name(network),
        Err(e) => {
            warn!(error = %e, network, "couldn't ask Docker for the network's bridge; naming the one blocklyd makes");
            bridge_name(network)
        }
    };
    ensure!(
        bridge.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.')),
        "the bridge of {network} has a name nftables can't be given as is: {bridge:?}"
    );
    let [low, high] = config.network.port_range;
    apply(&ruleset(TABLE, &bridge, &(low..=high), config.api.listen.port()))?;
    Ok(format!(
        "Workloads on {network} ({bridge}) can't open connections to this host, to ports {low}-{high} or {} on \
         private addresses, or to the metadata address (nft list table inet {TABLE}).\n",
        config.api.listen.port()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rules_drop_new_connections_from_the_bridge_only() {
        assert_eq!(
            ruleset("blocklyd", "bly0123456789ab", &(42000..=42999), 7443),
            "table inet blocklyd\n\
             delete table inet blocklyd\n\
             table inet blocklyd {\n\
             \tchain input {\n\
             \t\ttype filter hook input priority filter - 10; policy accept;\n\
             \t\tiifname \"bly0123456789ab\" ct state new drop\n\
             \t}\n\
             \tchain forward {\n\
             \t\ttype filter hook forward priority filter - 10; policy accept;\n\
             \t\tiifname \"bly0123456789ab\" ip daddr 169.254.0.0/16 ct state new drop\n\
             \t\tiifname \"bly0123456789ab\" ip daddr { 10.0.0.0/8, 100.64.0.0/10, 172.16.0.0/12, 192.168.0.0/16 } \
             meta l4proto { tcp, udp } th dport { 42000-42999, 7443 } ct state new drop\n\
             \t\tiifname \"bly0123456789ab\" ip6 daddr { fc00::/7, fe80::/10 } \
             meta l4proto { tcp, udp } th dport { 42000-42999, 7443 } ct state new drop\n\
             \t}\n\
             }\n"
        );
    }

    #[test]
    fn ports_nftables_would_refuse_are_written_another_way() {
        let rules = ruleset("t", "b", &(42000..=42999), 42500);
        assert!(rules.contains("th dport { 42000-42999 }"), "an API port inside the range isn't listed twice: {rules}");
        let rules = ruleset("t", "b", &(42000..=42000), 7443);
        assert!(rules.contains("th dport { 42000, 7443 }"), "a range of one is one port: {rules}");
    }
}
