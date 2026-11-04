// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! What a host's configuration is held to before blocklyd runs on it: the ranges, addresses and
//! ids it may name, and the floor of the policy every workload is held to. Not for what the
//! configuration holds or leaves out (`config.rs`), or for working out what it leaves out
//! (`crate::infer`).

use std::net::IpAddr;

use super::{Config, ConfigError, MAX_PUT_MB, Mode, fleet_url_ok, private_or_loopback};

/// The first port an unprivileged process may bind: Linux's default
/// `net.ipv4.ip_unprivileged_port_start`, and the end of the ports IANA calls well-known.
const UNPRIVILEGED_PORTS: u16 = 1024;
/// What /proc/<pid>/oom_score_adj takes: `OOM_SCORE_ADJ_MIN` and `OOM_SCORE_ADJ_MAX` in the
/// kernel's include/uapi/linux/oom.h, which libc doesn't carry.
const OOM_SCORE_ADJ: std::ops::RangeInclusive<i32> = -1000..=1000;

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let bad = |m: String| Err(ConfigError::Invalid(m));
        if !(1..=MAX_PUT_MB).contains(&self.transfer.max_put_mb) {
            return bad(format!(
                "transfer.max_put_mb is the most one PUT carries, 1 to {MAX_PUT_MB}: no store takes more"
            ));
        }
        if self.transfer.idle_seconds == 0 {
            return bad("transfer.idle_seconds is at least 1".into());
        }
        if self.workloads.resume_timeout_seconds == 0 {
            return bad("workloads.resume_timeout_seconds is at least 1".into());
        }
        if self.fleet.is_none() && !id_ok(self.node_id.as_str()) {
            return bad("node_id is 1-63 of a-z, 0-9 and '-'".into());
        }
        match self.mode()? {
            Mode::Standalone(tls) => {
                if tls.allowed_clients.is_empty() {
                    return bad("api.tls.allowed_clients needs at least one client name".into());
                }
            }
            Mode::Fleet(fleet) => {
                if !self.node_id.as_str().is_empty() {
                    return bad("in fleet mode node_id comes from enrollment; leave it out".into());
                }
                if self.api.tls.is_some() {
                    return bad("in fleet mode TLS material comes from enrollment; leave api.tls out".into());
                }
                if !fleet_url_ok(&fleet.url) {
                    return bad("fleet.url must be https://host[:port]".into());
                }
                if !(1..=60).contains(&fleet.heartbeat_seconds) {
                    return bad("fleet.heartbeat_seconds is between 1 and 60".into());
                }
                // The control plane dials what the node reports: 0.0.0.0 would send it to itself.
                if fleet.api_address.is_none() && self.api.listen.ip().is_unspecified() {
                    return bad(format!(
                        "api.listen is {}, which the control plane can't dial; set fleet.api_address to this \
                         host's address on the private network",
                        self.api.listen
                    ));
                }
                if fleet.api_address.is_some_and(|a| a.ip().is_unspecified()) {
                    return bad("fleet.api_address is where the control plane dials this node; 0.0.0.0 isn't".into());
                }
            }
        }
        if !name_ok(&self.deployment_id) {
            return bad("deployment_id is 1-32 of a-z, 0-9 and '-'".into());
        }
        if !self.state_dir.is_absolute() {
            return bad("state_dir must be absolute".into());
        }
        let [low, high] = self.network.port_range;
        if low < UNPRIVILEGED_PORTS || low > high {
            return bad(format!("network.port_range is [low, high] with {UNPRIVILEGED_PORTS} <= low <= high"));
        }
        if self.network.edge_ips.is_empty() || self.network.control_ips.is_empty() {
            return bad("network.edge_ips and network.control_ips each need an address".into());
        }
        // Servers are published on these and reached through them: 0.0.0.0 would publish them on
        // every interface, past the edge, and route to nowhere.
        if self.network.edge_ips.iter().chain(&self.network.control_ips).any(IpAddr::is_unspecified) {
            return bad("network.edge_ips and network.control_ips are this host's own addresses, not 0.0.0.0".into());
        }
        // Anyone who reaches the ops listener reads it: it asks for no certificate, and its metrics
        // name every workload. 0.0.0.0 would put it on the public interfaces too.
        if !private_or_loopback(self.ops.listen.ip()) {
            return bad(format!(
                "ops.listen is {}, but the ops listener has no authentication: bind it to loopback or a private \
                 address (127.0.0.0/8, ::1, 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, 100.64.0.0/10, fc00::/7, \
                 fe80::/10), never 0.0.0.0, :: or a public one",
                self.ops.listen
            ));
        }
        // Root and its group neither run a workload nor own its data: uid 0 in a container is a
        // short step from root on the host, and files the host's root owns are the host's.
        let owner = self.workloads.data_owner.map(|owner| ("workloads.data_owner", owner));
        for (key, ids) in std::iter::once(("workloads.user", self.workloads.user)).chain(owner) {
            if ids.uid == 0 || ids.gid == 0 {
                return bad(format!(
                    "{key} is {ids}, which is root: workloads never run as, or own data as, uid 0 or gid 0"
                ));
            }
        }
        if self.workloads.allowed_images.is_empty() {
            return bad("workloads.allowed_images needs at least one prefix".into());
        }
        if !(0.5..=4.0).contains(&self.capacity.memory_overcommit) {
            return bad("capacity.memory_overcommit is between 0.5 and 4.0".into());
        }
        if !OOM_SCORE_ADJ.contains(&self.workloads.oom_score_adj) {
            return bad(format!(
                "workloads.oom_score_adj is between {} and {}",
                OOM_SCORE_ADJ.start(),
                OOM_SCORE_ADJ.end()
            ));
        }
        Ok(())
    }
}

/// A node id: what the control plane issues (a UUID) or an operator picks.
fn id_ok(value: &str) -> bool {
    let b = value.as_bytes();
    !b.is_empty() && b.len() <= 63 && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn name_ok(value: &str) -> bool {
    let b = value.as_bytes();
    !b.is_empty() && b.len() <= 32 && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}
