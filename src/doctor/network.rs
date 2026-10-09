//! Whether the game port range is free on the addresses blocklyd publishes on, and whether its
//! listeners stay off public addresses.

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};

use super::check::Check;
use crate::config::{Config, private_or_loopback};
use crate::ports::{Probe, bind_probe};
use crate::protocol::Proto;

pub fn ports(config: &Config, held: &HashSet<(Proto, u16)>) -> Check {
    let mut addresses: Vec<IpAddr> = config.network.edge_ips.clone();
    addresses.extend(config.network.control_ips.iter().copied());
    addresses.sort();
    addresses.dedup();
    verdict(config.network.port_range, &addresses, held, bind_probe(addresses.clone()))
}

/// Ports blocklyd's own workloads hold are counted as free: they are where they should be.
fn verdict(range: [u16; 2], addresses: &[IpAddr], held: &HashSet<(Proto, u16)>, probe: Probe) -> Check {
    let [low, high] = range;
    let total = usize::from(high - low) + 1;
    let busy: Vec<u16> = (low..=high)
        .filter(|port| {
            [Proto::Tcp, Proto::Udp].into_iter().any(|proto| !held.contains(&(proto, *port)) && !probe(proto, *port))
        })
        .collect();
    let on = addresses.iter().map(IpAddr::to_string).collect::<Vec<_>>().join(", ");
    match busy.len() {
        0 => Check::pass("ports", format!("{low}-{high} are free on {on}")),
        n if n == total => Check::warn(
            "ports",
            format!("none of {low}-{high} can be bound on {on}: no server can be reached"),
            "Set network.edge_ips and network.control_ips to this host's own addresses, or free the range",
        ),
        n => Check::warn(
            "ports",
            format!(
                "{n} of {total} ports in {low}-{high} are taken by something else (first {}); blocklyd skips them",
                busy[0]
            ),
            format!(
                "Stop what holds them (ss -tulpn 'sport >= :{low} and sport <= :{high}'), or move network.port_range"
            ),
        ),
    }
}

/// The API asks for a client certificate, the ops listener for nothing: neither belongs on a
/// public address. A public `ops.listen` is already refused by the config check.
pub fn listeners(config: &Config) -> Vec<Check> {
    let dialed = config.fleet.as_ref().and_then(|f| f.api_address).unwrap_or(config.api.listen);
    vec![
        listener("listen.api", "api.listen", config.api.listen, dialed),
        listener("listen.ops", "ops.listen", config.ops.listen, config.ops.listen),
    ]
}

/// `bound` is what the socket binds; `reached` the address it is reached on, which for 0.0.0.0 in
/// fleet mode is `fleet.api_address`.
fn listener(name: &'static str, key: &str, bound: SocketAddr, reached: SocketAddr) -> Check {
    if bound.ip().is_unspecified() {
        let fix = if reached.ip().is_unspecified() {
            format!("Set {key} to this host's private or WireGuard address")
        } else {
            format!("Set {key} to this host's private address, {}", reached.ip())
        };
        return Check::warn(name, format!("{key} is {bound}: every interface, public ones included"), fix);
    }
    if private_or_loopback(bound.ip()) {
        return Check::pass(name, format!("{key} is {bound}, off the public internet"));
    }
    Check::fail(
        name,
        format!("{key} is {bound}, a public address"),
        format!("Set {key} to this host's private or WireGuard address"),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::doctor::Status;

    fn ips() -> Vec<IpAddr> {
        vec![IpAddr::from([127, 0, 0, 1])]
    }

    #[test]
    fn free_ports_pass_and_taken_ones_warn() {
        let free: Probe = Arc::new(|_, _| true);
        assert_eq!(verdict([42000, 42009], &ips(), &HashSet::new(), free).status, Status::Pass);

        let one_taken: Probe = Arc::new(|proto, port| !(proto == Proto::Tcp && port == 42003));
        let check = verdict([42000, 42009], &ips(), &HashSet::new(), one_taken.clone());
        assert_eq!(check.status, Status::Warn);
        assert!(check.detail.starts_with("1 of 10 ports"), "{}", check.detail);

        let ours = HashSet::from([(Proto::Tcp, 42003)]);
        assert_eq!(verdict([42000, 42009], &ips(), &ours, one_taken).status, Status::Pass, "blocklyd's own server");

        let none: Probe = Arc::new(|_, _| false);
        let check = verdict([42000, 42009], &ips(), &HashSet::new(), none);
        assert_eq!(check.status, Status::Warn);
        assert!(check.fix.unwrap().contains("edge_ips"));
    }

    #[test]
    fn listeners_stay_off_public_addresses() {
        let at = |a: &str| a.parse::<SocketAddr>().unwrap();
        assert_eq!(listener("listen.api", "api.listen", at("10.0.0.5:7443"), at("10.0.0.5:7443")).status, Status::Pass);
        assert_eq!(
            listener("listen.api", "api.listen", at("100.64.1.2:7443"), at("100.64.1.2:7443")).status,
            Status::Pass
        );
        let public = listener("listen.api", "api.listen", at("203.0.113.9:7443"), at("203.0.113.9:7443"));
        assert_eq!((public.status, public.gates_start), (Status::Fail, false));
        let every = listener("listen.api", "api.listen", at("0.0.0.0:7443"), at("10.0.0.5:7443"));
        assert_eq!(every.status, Status::Warn);
        assert!(every.fix.unwrap().ends_with("10.0.0.5"));
    }
}
