//! What blocklyd works out about its host when the configuration leaves it out, so that a node's
//! configuration holds only what the host can't say about itself. `Config::load` fills these in
//! before reading the file; a key the file sets is never touched, and every value filled in is
//! listed in `Config::inferred`, for `serve` to log and `join` to print.
//!
//! - In fleet mode, `api.listen` (port 7443), `network.edge_ips` and `network.control_ips` are one
//!   address: the one the file gives in `api.listen` or `fleet.api_address`, or else the host's one
//!   private address (RFC 1918, 100.64.0.0/10, or any address on a WireGuard interface). Joining a
//!   fleet is the decision to serve on it. Alone, blocklyd keeps loopback (config.rs).
//! - `capacity.reserved_memory_mb` grows with the host's memory (`reserved_memory_mb` below).
//!
//! It reads under a root directory, `/` on a host and a fixture tree in tests: the kernel's local
//! addresses from /proc/net/fib_trie, their interfaces from /proc/net/route, what each interface
//! is from /sys/class/net, and /proc/meminfo. Not getifaddrs(3) (if-addrs, nix::ifaddrs, netdev):
//! it opens a netlink socket, and the unit's RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
//! refuses AF_NETLINK. Units are written once by join, never by upgrade. It doesn't infer the
//! deployment: that comes from the join token (cli/join.rs).

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

use toml::{Table, Value};

/// The API's port when `api.listen` is inferred.
pub(crate) const API_PORT: u16 = 7443;

/// One value filled in, as the configuration would write it, and where it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inferred {
    pub key: &'static str,
    pub value: String,
    pub from: String,
}

/// A local address the host could serve the fleet on.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Candidate {
    pub(crate) ip: Ipv4Addr,
    pub(crate) interface: Option<String>,
}

impl std::fmt::Display for Candidate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.interface {
            Some(interface) => write!(f, "{} ({interface})", self.ip),
            None => write!(f, "{}", self.ip),
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) enum InferError {
    NoAddress,
    Several(Vec<Candidate>),
}

const CHOOSE: &str = "Name the one the control plane and the edge reach this host on: add `--address <ip>` to \
                      `blocklyd join` (or to the end of the pasted line), or set api.listen, network.edge_ips and \
                      network.control_ips in the configuration.";

impl std::fmt::Display for InferError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoAddress => write!(
                f,
                "this host has no private address to serve the fleet on (10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16, \
                 100.64.0.0/10, or a WireGuard interface). Join it to the private network first, or: {CHOOSE}"
            ),
            Self::Several(found) => {
                let listed: Vec<String> = found.iter().map(ToString::to_string).collect();
                write!(f, "this host has {} private addresses: {}. {CHOOSE}", found.len(), listed.join(", "))
            }
        }
    }
}

impl std::error::Error for InferError {}

/// Fills in what `table`, a configuration as written, leaves out.
pub(crate) fn fill(table: &mut Table, root: &Path) -> Result<Vec<Inferred>, InferError> {
    let mut inferred = Vec::new();
    if table.contains_key("fleet") {
        addresses(table, root, &mut inferred)?;
    }
    if let Some(total) = memory_total_mb(root)
        && let Some(capacity) = section(table, "capacity")
        && !capacity.contains_key("reserved_memory_mb")
    {
        let reserved = reserved_memory_mb(total);
        capacity.insert("reserved_memory_mb".into(), Value::Integer(reserved as i64));
        inferred.push(Inferred {
            key: "capacity.reserved_memory_mb",
            value: reserved.to_string(),
            from: format!("a sixteenth of the host's {total} MB, and at least 2048"),
        });
    }
    Ok(inferred)
}

/// Memory kept for the host: a sixteenth of it, and never less than 2048 MB, today's fixed
/// default. The kernel, the page cache worlds are read through, Docker and blocklyd need more on a
/// bigger host: 2048 MB up to 32 GB, 4096 on 64 GB, 8192 on 128 GB.
pub(crate) fn reserved_memory_mb(total_mb: u64) -> u64 {
    (total_mb / 16).max(2048)
}

fn memory_total_mb(root: &Path) -> Option<u64> {
    let text = std::fs::read_to_string(root.join("proc/meminfo")).ok()?;
    let kb = text.lines().find_map(|line| line.strip_prefix("MemTotal:"))?.split_whitespace().next()?;
    kb.parse::<u64>().ok().map(|kb| kb / 1024)
}

/// The table `name`, made if absent. None when the file gives `name` as something else, which
/// reading the configuration then refuses by name.
fn section<'a>(table: &'a mut Table, name: &str) -> Option<&'a mut Table> {
    table.entry(name).or_insert_with(|| Value::Table(Table::new())).as_table_mut()
}

fn sets(table: &Table, name: &str, key: &str) -> bool {
    table.get(name).and_then(Value::as_table).is_some_and(|t| t.contains_key(key))
}

fn addresses(table: &mut Table, root: &Path, inferred: &mut Vec<Inferred>) -> Result<(), InferError> {
    let keys = [("api", "listen"), ("network", "edge_ips"), ("network", "control_ips")];
    if keys.iter().all(|(name, key)| sets(table, name, key)) {
        return Ok(());
    }
    // The address the file already gives, where the control plane dials this node, else the host's.
    let given = |name: &str, key: &str| {
        let text = table.get(name)?.get(key)?.as_str()?;
        text.parse::<SocketAddr>().ok().map(|a| a.ip()).filter(|ip| !ip.is_unspecified()).map(|ip| (ip, text))
    };
    let (ip, from) = match given("api", "listen").or_else(|| given("fleet", "api_address")) {
        Some((ip, text)) => (ip, format!("the address the configuration gives, {text}")),
        None => {
            let found = address(root)?;
            let on = found.interface.map(|i| format!(", on {i}")).unwrap_or_default();
            (IpAddr::V4(found.ip), format!("the host's private address{on}"))
        }
    };
    for (name, key) in keys {
        if sets(table, name, key) {
            continue;
        }
        let (value, shown) = if key == "listen" {
            let listen = SocketAddr::new(ip, API_PORT).to_string();
            (Value::String(listen.clone()), format!("\"{listen}\""))
        } else {
            (Value::Array(vec![Value::String(ip.to_string())]), format!("[\"{ip}\"]"))
        };
        let Some(section) = section(table, name) else { continue };
        section.insert(key.into(), value);
        let key = match key {
            "listen" => "api.listen",
            "edge_ips" => "network.edge_ips",
            _ => "network.control_ips",
        };
        inferred.push(Inferred { key, value: shown, from: from.clone() });
    }
    Ok(())
}

/// The host's one private address, or why there isn't one.
pub(crate) fn address(root: &Path) -> Result<Candidate, InferError> {
    let mut found = candidates(root);
    match found.len() {
        0 => Err(InferError::NoAddress),
        1 => Ok(found.remove(0)),
        _ => Err(InferError::Several(found)),
    }
}

/// Every local IPv4 address the fleet could be served on: private, or on a WireGuard interface.
/// Never loopback, and never a bridge's, such as Docker's own `docker0`.
pub(crate) fn candidates(root: &Path) -> Vec<Candidate> {
    let read = |path: &str| std::fs::read_to_string(root.join(path)).unwrap_or_default();
    let routes = read("proc/net/route");
    let mut found: Vec<Candidate> = Vec::new();
    for ip in local_addresses(&read("proc/net/fib_trie")) {
        if ip.is_loopback() || found.iter().any(|c| c.ip == ip) {
            continue;
        }
        let interface = interface_of(ip, &routes);
        let net = interface.as_deref().filter(|i| !i.contains('/')).map(|i| root.join("sys/class/net").join(i));
        let take = match net {
            Some(net) if net.join("bridge").exists() => false,
            Some(net) if std::fs::read_to_string(net.join("uevent")).is_ok_and(|u| u.contains("DEVTYPE=wireguard")) => {
                true
            }
            _ => ip.is_private() || (ip.octets()[0] == 100 && (64..128).contains(&ip.octets()[1])),
        };
        if take {
            found.push(Candidate { ip, interface });
        }
    }
    found
}

/// The addresses /proc/net/fib_trie lists as this host's own: a leaf followed by `/32 host LOCAL`.
fn local_addresses(trie: &str) -> Vec<Ipv4Addr> {
    let (mut leaf, mut local) = (None, Vec::new());
    for line in trie.lines().map(str::trim) {
        if let Some(ip) = line.strip_prefix("|-- ") {
            leaf = ip.parse::<Ipv4Addr>().ok();
        } else if line == "/32 host LOCAL"
            && let Some(ip) = leaf
        {
            local.push(ip);
        }
    }
    local
}

/// The interface of the narrowest directly connected route (no gateway) holding `ip`, from
/// /proc/net/route, where addresses are hex in the kernel's byte order.
fn interface_of(ip: Ipv4Addr, routes: &str) -> Option<String> {
    let hex = |field: &str| u32::from_str_radix(field, 16).ok().map(|v| u32::from_be_bytes(v.to_ne_bytes()));
    let ip = u32::from(ip);
    routes
        .lines()
        .skip(1)
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let (destination, gateway, mask) = (hex(fields.get(1)?)?, hex(fields.get(2)?)?, hex(fields.get(7)?)?);
            (gateway == 0 && ip & mask == destination).then(|| (mask.count_ones(), fields[0].to_owned()))
        })
        .max_by_key(|(bits, _)| *bits)
        .map(|(_, interface)| interface)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A host's /proc and /sys as the kernel writes them, for the addresses given as
    /// (interface, address, prefix length, what the interface is).
    fn host(addresses: &[(&str, &str, u32, &str)], memory_kb: u64) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        let net = root.path().join("proc/net");
        std::fs::create_dir_all(&net).unwrap();
        let mut trie = String::from("Main:\n  +-- 0.0.0.0/0 3 0 5\n     |-- 127.0.0.1\n        /32 host LOCAL\n");
        let mut route =
            String::from("Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n");
        let raw = |v: u32| format!("{:08X}", u32::from_ne_bytes(v.to_be_bytes()));
        for (interface, ip, prefix, kind) in addresses {
            let ip: Ipv4Addr = ip.parse().unwrap();
            trie += &format!("     |-- {ip}\n        /32 host LOCAL\n     |-- {ip}\n        /32 link BROADCAST\n");
            let mask = u32::MAX.checked_shl(32 - prefix).unwrap_or(0);
            route += &format!(
                "{interface}\t{}\t00000000\t0001\t0\t0\t0\t{}\t0\t0\t0\n",
                raw(u32::from(ip) & mask),
                raw(mask)
            );
            let sys = root.path().join("sys/class/net").join(interface);
            std::fs::create_dir_all(&sys).unwrap();
            match *kind {
                "bridge" => std::fs::create_dir_all(sys.join("bridge")).unwrap(),
                "wireguard" => std::fs::write(sys.join("uevent"), "DEVTYPE=wireguard\nINTERFACE=wg0\n").unwrap(),
                _ => std::fs::write(sys.join("uevent"), format!("INTERFACE={interface}\n")).unwrap(),
            }
        }
        route += &format!("eth0\t00000000\t{}\t0003\t0\t0\t0\t00000000\t0\t0\t0\n", raw(0xCB00_7101));
        std::fs::write(net.join("fib_trie"), trie).unwrap();
        std::fs::write(net.join("route"), route).unwrap();
        std::fs::write(root.path().join("proc/meminfo"), format!("MemTotal:       {memory_kb} kB\nMemFree: 1 kB\n"))
            .unwrap();
        root
    }

    const FLEET: &str =
        "deployment_id = \"p\"\n[fleet]\nurl = \"https://10.0.0.2:8443\"\nca = \"/etc/blocklyd/fleet-ca.pem\"\n";

    fn filled(toml: &str, root: &Path) -> Result<(Table, Vec<Inferred>), InferError> {
        let mut table: Table = toml::from_str(toml).unwrap();
        fill(&mut table, root).map(|inferred| (table, inferred))
    }

    #[test]
    fn one_private_address_is_where_a_fleet_node_serves() {
        // A public address, the private network, and Docker's bridge: only the private one counts.
        let root = host(
            &[("eth0", "203.0.113.7", 24, ""), ("enp7s0", "10.0.0.5", 16, ""), ("docker0", "172.17.0.1", 16, "bridge")],
            64 * 1024 * 1024,
        );
        let (table, inferred) = filled(FLEET, root.path()).unwrap();
        assert_eq!(table["api"]["listen"].as_str(), Some("10.0.0.5:7443"));
        for key in ["edge_ips", "control_ips"] {
            assert_eq!(table["network"][key].as_array().unwrap(), &vec![Value::String("10.0.0.5".into())]);
        }
        let keys: Vec<_> = inferred.iter().map(|i| (i.key, i.value.as_str())).collect();
        assert_eq!(
            keys,
            [
                ("api.listen", "\"10.0.0.5:7443\""),
                ("network.edge_ips", "[\"10.0.0.5\"]"),
                ("network.control_ips", "[\"10.0.0.5\"]"),
                ("capacity.reserved_memory_mb", "4096"),
            ]
        );
        assert!(inferred[0].from.contains("enp7s0"), "{:?}", inferred[0]);
    }

    #[test]
    fn a_wireguard_address_counts_whatever_its_range() {
        let root = host(&[("eth0", "203.0.113.7", 24, ""), ("wg0", "198.18.0.3", 24, "wireguard")], 8 << 20);
        assert_eq!(
            address(root.path()),
            Ok(Candidate { ip: "198.18.0.3".parse().unwrap(), interface: Some("wg0".into()) })
        );
        let cgnat = host(&[("tailscale0", "100.101.2.3", 32, "")], 8 << 20);
        assert_eq!(address(cgnat.path()).unwrap().ip, "100.101.2.3".parse::<Ipv4Addr>().unwrap());
    }

    #[test]
    fn two_private_addresses_or_none_is_a_question_for_the_operator() {
        let two = host(&[("enp7s0", "10.0.0.5", 16, ""), ("wg0", "100.64.0.9", 24, "wireguard")], 8 << 20);
        let refused = filled(FLEET, two.path()).unwrap_err();
        assert_eq!(refused.to_string().matches("--address").count(), 1);
        assert!(refused.to_string().contains("2 private addresses: 10.0.0.5 (enp7s0), 100.64.0.9 (wg0)"), "{refused}");

        let none = host(&[("eth0", "203.0.113.7", 24, "")], 8 << 20);
        let refused = filled(FLEET, none.path()).unwrap_err();
        assert_eq!(refused, InferError::NoAddress);
        assert!(refused.to_string().contains("--address"), "{refused}");
    }

    #[test]
    fn what_the_file_sets_wins_and_a_single_node_keeps_loopback() {
        let two = host(&[("enp7s0", "10.0.0.5", 16, ""), ("enp8s0", "10.1.0.5", 16, "")], 8 << 20);
        // Every address set: no question asked, though the host has two.
        let set = format!(
            "{FLEET}[api]\nlisten = \"10.1.0.5:9000\"\n[network]\nedge_ips = [\"10.1.0.5\"]\ncontrol_ips = [\"10.1.0.5\"]\n\
             [capacity]\nreserved_memory_mb = 0\n"
        );
        let (table, inferred) = filled(&set, two.path()).unwrap();
        assert!(inferred.is_empty(), "{inferred:?}");
        assert_eq!(table["api"]["listen"].as_str(), Some("10.1.0.5:9000"));
        assert_eq!(table["capacity"]["reserved_memory_mb"].as_integer(), Some(0));

        // The API's address given, the others follow it, as a node with loopback for all does.
        let listen = format!("{FLEET}[api]\nlisten = \"127.0.0.1:7443\"\n");
        let (table, inferred) = filled(&listen, two.path()).unwrap();
        assert_eq!(table["network"]["edge_ips"].as_array().unwrap(), &vec![Value::String("127.0.0.1".into())]);
        assert!(inferred[0].from.contains("127.0.0.1:7443"), "{inferred:?}");
        let dialed = format!("{FLEET}api_address = \"10.1.0.5:7443\"\n[api]\nlisten = \"0.0.0.0:7443\"\n");
        let (table, _) = filled(&dialed, two.path()).unwrap();
        assert_eq!(table["network"]["control_ips"].as_array().unwrap(), &vec![Value::String("10.1.0.5".into())]);

        let alone = "node_id = \"n\"\ndeployment_id = \"p\"\n[api]\nlisten = \"127.0.0.1:7443\"\n";
        let (table, inferred) = filled(alone, two.path()).unwrap();
        assert!(table.get("network").is_none(), "loopback, as config.rs defaults it");
        assert_eq!(inferred.iter().map(|i| i.key).collect::<Vec<_>>(), ["capacity.reserved_memory_mb"]);
    }

    #[test]
    fn the_host_keeps_a_sixteenth_of_its_memory_and_never_less_than_before() {
        assert_eq!(reserved_memory_mb(4 * 1024), 2048);
        assert_eq!(reserved_memory_mb(32 * 1024), 2048);
        assert_eq!(reserved_memory_mb(64 * 1024), 4096);
        assert_eq!(reserved_memory_mb(128 * 1024), 8192);
        // A host whose memory can't be read keeps the default the configuration has.
        let mut table: Table = toml::from_str("node_id = \"n\"").unwrap();
        assert_eq!(fill(&mut table, Path::new("/nonexistent")), Ok(vec![]));
    }
}
