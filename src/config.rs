//! The host's configuration: who this node is, where it keeps state, who may call it, and the
//! policy it holds every workload to. Policy lives here rather than in requests on purpose: a
//! compromised caller can ask for anything the protocol allows, but not for less isolation than
//! the host enforces.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use serde::Deserialize;

use crate::ids::NodeId;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// This host's stable name, written on every container it makes. Two nodes never share one.
    /// In fleet mode it is left out: the control plane issues it at enrollment.
    #[serde(default)]
    pub node_id: NodeId,
    /// The Blockly deployment (staging, production) this host serves. Containers of another
    /// deployment are never adopted or touched.
    pub deployment_id: String,
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    pub api: ApiConfig,
    #[serde(default)]
    pub ops: OpsConfig,
    #[serde(default)]
    pub docker: DockerConfig,
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub(crate) workloads: WorkloadPolicy,
    #[serde(default)]
    pub capacity: CapacityConfig,
    #[serde(default)]
    pub(crate) intervals: Intervals,
    #[serde(default)]
    pub(crate) transfer: TransferConfig,
    /// Fleet mode: this node enrolls with a control plane, takes its identity and TLS material
    /// from it, and reports to it. Without it, blocklyd is the single-node daemon it was.
    #[serde(default)]
    pub fleet: Option<FleetConfig>,
    /// What `load` worked out from the host because the file left it out (infer.rs).
    #[serde(skip)]
    pub inferred: Vec<crate::infer::Inferred>,
}

fn default_state_dir() -> PathBuf {
    PathBuf::from("/var/lib/blocklyd")
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    /// The mTLS API. Bind it to the private or WireGuard address, never a public one. In fleet
    /// mode, left out, it is the host's private address on port 7443 (infer.rs).
    pub listen: SocketAddr,
    /// Left out in fleet mode, where enrollment provides it.
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default = "default_max_body")]
    pub max_body_bytes: usize,
    /// How long shutdown waits for requests in flight before exiting anyway.
    #[serde(default = "default_shutdown_grace")]
    pub shutdown_grace_seconds: u64,
}

fn default_max_body() -> usize {
    2 * 1024 * 1024
}
fn default_shutdown_grace() -> u64 {
    30
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TlsConfig {
    /// This node's certificate chain (PEM), with extended key usage serverAuth.
    pub(crate) cert: PathBuf,
    /// Its private key (PEM, mode 0600).
    pub(crate) key: PathBuf,
    /// The CA that issues control-plane client certificates. Only its certificate lives on a
    /// host; its private key never does.
    pub(crate) client_ca: PathBuf,
    /// DNS names (subject alternative names) of the clients allowed to call this API, e.g.
    /// `control-plane.staging.blockly.internal`. A valid certificate for another name is refused.
    pub allowed_clients: Vec<String>,
}

/// How this node joins a fleet. The control plane is the durable brain; the node reports to it
/// and takes orders only for workloads, never for its own identity after enrollment.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetConfig {
    /// The control plane's node endpoint (enrollment and heartbeats), `https://host:port`.
    pub url: String,
    /// The fleet CA's certificate: the only thing that says the endpoint above is the real
    /// control plane. Shipped with the host's provisioning, like the token.
    pub ca: PathBuf,
    /// A one-time enrollment token, read on first start and removed once enrolled.
    #[serde(default)]
    pub enrollment_token_file: Option<PathBuf>,
    #[serde(default = "default_heartbeat")]
    pub heartbeat_seconds: u64,
    /// Where the control plane dials this node's API; defaults to `api.listen`.
    #[serde(default)]
    pub api_address: Option<SocketAddr>,
    /// Region, zone and such, as the node reports them at enrollment. The token may pin them.
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    /// The execution lease when the control plane's answer names none: a workload that fails is
    /// restarted (per its policy), or one the host went down under resumed, only this long after
    /// sending the last heartbeat the control plane answered. A node cut off from it might have
    /// been replaced.
    #[serde(default = "default_restart_contact")]
    pub restart_requires_contact_seconds: u64,
}

fn default_restart_contact() -> u64 {
    120
}

fn default_heartbeat() -> u64 {
    5
}

// The sections below need nothing, so each is `#[serde(default)]` as a whole: a key left out
// takes its value from the section's `Default`, the one place each default is written.

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct OpsConfig {
    /// Plain HTTP `/healthz`, `/readyz` and `/metrics`, with no authentication. Loopback by
    /// default, and never a public address: it names workloads.
    pub listen: SocketAddr,
}

impl Default for OpsConfig {
    fn default() -> Self {
        Self { listen: SocketAddr::from(([127, 0, 0, 1], 7070)) }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct DockerConfig {
    pub socket: PathBuf,
    /// The bridge network workloads join. Made with inter-container traffic off.
    pub(crate) network: String,
    pub(crate) pull_timeout_seconds: u64,
}

impl Default for DockerConfig {
    fn default() -> Self {
        Self {
            socket: PathBuf::from("/var/run/docker.sock"),
            network: "blockly-workloads".into(),
            pull_timeout_seconds: 20 * 60,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct NetworkConfig {
    /// Host ports handed to workloads, inclusive. Nothing else on the host should use them.
    pub port_range: [u16; 2],
    /// Addresses the edge reaches game ports on. Loopback by default: exposing a port is a
    /// decision, not an accident. In fleet mode joining is that decision, and left out, both
    /// lists are the address `api.listen` gives, or else the host's private address (infer.rs).
    pub edge_ips: Vec<IpAddr>,
    /// Addresses the control plane reaches console and status ports on.
    pub control_ips: Vec<IpAddr>,
    /// A released port isn't handed out again for this long, so a route to the old workload
    /// that is still cached somewhere can't reach a new one.
    pub(crate) port_quarantine_seconds: u64,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            port_range: [42000, 42999],
            edge_ips: vec![IpAddr::from([127, 0, 0, 1])],
            control_ips: vec![IpAddr::from([127, 0, 0, 1])],
            port_quarantine_seconds: 600,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WorkloadPolicy {
    /// Image reference prefixes this host runs. Everything else is refused.
    pub(crate) allowed_images: Vec<String>,
    /// uid:gid every workload runs as. Numeric, never root.
    pub(crate) user: Ids,
    /// Owner of each workload's data directory on the host. The same as `user` unless the
    /// daemon remaps user namespaces, where it is the remapped host id.
    pub(crate) data_owner: Option<Ids>,
    pub(crate) read_only_rootfs: bool,
    /// Size of the writable /tmp a read-only workload gets (tmpfs, counted in its memory).
    pub(crate) tmp_size_mb: u32,
    pub(crate) default_pids_limit: u32,
    pub(crate) log_max_size_mb: u32,
    pub(crate) log_max_files: u32,
    /// The kernel kills the highest score first when the host runs out; workloads go before
    /// blocklyd and Docker.
    pub(crate) oom_score_adj: i32,
    /// How long deleted data stays in the host's trash before it is purged.
    pub(crate) trash_retention_hours: u64,
    pub(crate) min_memory_mb: u32,
}

impl Default for WorkloadPolicy {
    fn default() -> Self {
        Self {
            allowed_images: vec!["itzg/minecraft-server:".into(), "docker.io/itzg/minecraft-server:".into()],
            user: Ids { uid: 1000, gid: 1000 },
            data_owner: None,
            read_only_rootfs: true,
            tmp_size_mb: 256,
            default_pids_limit: 4096,
            log_max_size_mb: 20,
            log_max_files: 5,
            oom_score_adj: 500,
            trash_retention_hours: 24,
            min_memory_mb: 256,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CapacityConfig {
    /// Memory kept for the host: kernel, page cache, Docker, blocklyd. Never given out. Left out,
    /// it grows with the host's memory, from 2048 (infer.rs).
    pub reserved_memory_mb: u64,
    /// A ceiling on what workloads may be given, below total minus reserved: for a host that
    /// runs other things too, or a test that wants a small host.
    pub(crate) allocatable_memory_mb: Option<u64>,
    /// Creating a workload is refused below this much free disk.
    pub min_free_disk_mb: u64,
    /// Running memory may reach allocatable × this. 1.0: no overcommit (the default, since a
    /// JVM with a pre-touched heap really uses what it is given).
    pub(crate) memory_overcommit: f64,
    /// CPU kept for the host (kernel, Docker, blocklyd, network interrupts), in millicores.
    pub reserved_cpu_millis: u64,
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            reserved_memory_mb: 2048,
            allocatable_memory_mb: None,
            min_free_disk_mb: 5 * 1024,
            memory_overcommit: 1.0,
            reserved_cpu_millis: 1000,
        }
    }
}

impl CapacityConfig {
    /// What workloads may be given on a host with `total_mb` of memory: all but what is reserved,
    /// and no more than `allocatable_memory_mb` when it is set.
    pub fn allocatable_mb(&self, total_mb: u64) -> u64 {
        let host = total_mb.saturating_sub(self.reserved_memory_mb);
        self.allocatable_memory_mb.map_or(host, |cap| cap.min(host))
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Intervals {
    /// A full reconciliation against the runtime, besides the event stream.
    pub(crate) resync_seconds: u64,
    pub(crate) stats_seconds: u64,
    pub(crate) disk_usage_seconds: u64,
}

impl Default for Intervals {
    fn default() -> Self {
        Self { resync_seconds: 30, stats_seconds: 15, disk_usage_seconds: 300 }
    }
}

/// How archives reach the object store.
#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct TransferConfig {
    /// The largest archive sent in one PUT, in MiB; a larger one goes in parts when the request
    /// offers them. The default is R2's limit for one PUT, the strictest of the stores Blockly
    /// uses; a store whose limit is lower sets it lower.
    pub(crate) max_put_mb: u64,
    /// How long a transfer may go without receiving anything before it counts as stalled: a
    /// store that hung mid-body, or a path that drops packets without a reset, never ends a body
    /// by itself. Whatever arrives starts it over, so a slow but live download of any size never
    /// trips it.
    pub(crate) idle_seconds: u64,
}

const MIB: u64 = 1024 * 1024;

/// R2's limit for one PUT, in MiB: the default, and the most `max_put_mb` may be.
const MAX_PUT_MB: u64 = crate::protocol::MAX_SINGLE_PUT_BYTES / MIB;

impl Default for TransferConfig {
    fn default() -> Self {
        Self { max_put_mb: MAX_PUT_MB, idle_seconds: 120 }
    }
}

impl TransferConfig {
    pub(crate) fn max_put_bytes(&self) -> u64 {
        self.max_put_mb.saturating_mul(MIB)
    }

    pub(crate) fn idle(&self) -> Duration {
        Duration::from_secs(self.idle_seconds)
    }
}

/// What `Config::mode` makes of `[fleet]` and `[api.tls]`.
#[derive(Clone, Copy, Debug)]
pub enum Mode<'a> {
    Standalone(&'a TlsConfig),
    Fleet(&'a FleetConfig),
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("can't read {path}: {source}")]
    Read { path: PathBuf, source: std::io::Error },
    #[error("{path} is not valid: {source}")]
    Parse { path: PathBuf, source: toml::de::Error },
    #[error("{0}")]
    Invalid(String),
}

/// A numeric `uid:gid`, as the config writes one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Ids {
    pub(crate) uid: u32,
    pub(crate) gid: u32,
}

impl FromStr for Ids {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let numeric = || format!("{value} is not a numeric uid:gid");
        let (uid, gid) = value.split_once(':').ok_or_else(numeric)?;
        Ok(Self { uid: uid.parse().map_err(|_| numeric())?, gid: gid.parse().map_err(|_| numeric())? })
    }
}

impl<'de> Deserialize<'de> for Ids {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?.parse().map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for Ids {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.uid, self.gid)
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

/// Where a listener without authentication may be bound: loopback, or an address the internet
/// can't route to. An IPv4 address written as IPv6 (`::ffff:a.b.c.d`) is judged as itself.
pub(crate) fn private_or_loopback(ip: IpAddr) -> bool {
    match ip.to_canonical() {
        IpAddr::V4(ip) => {
            let [a, b, ..] = ip.octets();
            // 100.64.0.0/10 is shared address space: WireGuard and Tailscale overlays live there.
            ip.is_loopback() || ip.is_private() || (a == 100 && (64..128).contains(&b))
        }
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unique_local() || ip.is_unicast_link_local(),
    }
}

/// Whether the node can call the control plane at `url`: `https://host[:port]`, or a path under
/// it, read as the fleet client reads it (`hyper::Uri`). Paths are added to it as text, so a query
/// or a fragment would swallow them; a port out of range would be dialled as 443.
pub(crate) fn fleet_url_ok(url: &str) -> bool {
    url.starts_with("https://")
        && !url.contains('#')
        && url.parse::<hyper::Uri>().is_ok_and(|uri| {
            uri.query().is_none()
                && uri
                    .authority()
                    .is_some_and(|a| !a.host().is_empty() && (a.port_u16().is_some() || a.as_str().ends_with(a.host())))
        })
}

impl Config {
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read { path: path.into(), source })?;
        Self::parse(&text, path, Path::new("/"))
    }

    /// `text`, read from `path`, with what it leaves out worked out from the host under `root`.
    pub fn parse(text: &str, path: &Path, root: &Path) -> Result<Self, ConfigError> {
        let parse = |source| ConfigError::Parse { path: path.into(), source };
        let mut table: toml::Table = toml::from_str(text).map_err(parse)?;
        let inferred = crate::infer::fill(&mut table, root).map_err(|e| ConfigError::Invalid(e.to_string()))?;
        let mut config: Self = table.try_into().map_err(parse)?;
        config.inferred = inferred;
        config.validate()?;
        Ok(config)
    }

    /// How the node is reached and trusted: on its own, with the TLS material `[api.tls]` names,
    /// or in a fleet, which issues it at enrollment. A config with neither is refused.
    pub fn mode(&self) -> Result<Mode<'_>, ConfigError> {
        match (&self.fleet, &self.api.tls) {
            (Some(fleet), _) => Ok(Mode::Fleet(fleet)),
            (None, Some(tls)) => Ok(Mode::Standalone(tls)),
            (None, None) => Err(ConfigError::Invalid("api.tls is required unless [fleet] provides it".into())),
        }
    }

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
        if low < 1024 || low > high {
            return bad("network.port_range is [low, high] with 1024 <= low <= high".into());
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
        if !(-1000..=1000).contains(&self.workloads.oom_score_adj) {
            return bad("workloads.oom_score_adj is between -1000 and 1000".into());
        }
        Ok(())
    }

    pub fn data_owner_ids(&self) -> (u32, u32) {
        let owner = self.workloads.data_owner.unwrap_or(self.workloads.user);
        (owner.uid, owner.gid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
        node_id = "host-1"
        deployment_id = "staging"
        [api]
        listen = "10.0.0.5:7443"
        [api.tls]
        cert = "/etc/blocklyd/tls/node.pem"
        key = "/etc/blocklyd/tls/node.key"
        client_ca = "/etc/blocklyd/tls/ca.pem"
        allowed_clients = ["control-plane.staging.blockly.internal"]
    "#;

    #[test]
    fn minimal_config_takes_safe_defaults() {
        let config: Config = toml::from_str(MINIMAL).unwrap();
        config.validate().unwrap();
        assert_eq!(config.network.edge_ips, [IpAddr::from([127, 0, 0, 1])], "nothing is exposed unless asked");
        assert!(config.workloads.read_only_rootfs);
        assert_eq!(config.workloads.user, Ids { uid: 1000, gid: 1000 });
        assert_eq!(config.ops.listen.ip(), IpAddr::from([127, 0, 0, 1]));
        assert_eq!(config.transfer.max_put_bytes(), crate::protocol::MAX_SINGLE_PUT_BYTES, "R2's limit");
    }

    #[test]
    fn one_put_carries_at_least_a_mebibyte_and_never_more_than_any_store_takes() {
        let with = |mb: u64| {
            let config: Config = toml::from_str(&format!("{MINIMAL}\n[transfer]\nmax_put_mb = {mb}\n")).unwrap();
            config.validate()
        };
        assert!(with(1).is_ok());
        assert!(with(5115).is_ok());
        for mb in [0, 5116, 1 << 20] {
            let err = with(mb).unwrap_err().to_string();
            assert!(err.contains("transfer.max_put_mb"), "{err}");
        }
    }

    const EXAMPLE: &str = include_str!("../examples/blocklyd.toml");

    #[test]
    fn the_example_config_is_valid() {
        let config: Config = toml::from_str(EXAMPLE).unwrap();
        config.validate().unwrap();
        assert!(config.fleet.is_none());
    }

    /// The example writes out every default of the sections that have nothing required, but for
    /// the addresses and the reserved memory, which it sets as a host would: put back, it reads as
    /// a file that leaves the sections out.
    #[test]
    fn the_example_states_every_default() {
        let minimal: Config = toml::from_str(MINIMAL).unwrap();
        let mut example: Config = toml::from_str(EXAMPLE).unwrap();
        example.network.edge_ips = vec![IpAddr::from([127, 0, 0, 1])];
        example.network.control_ips = vec![IpAddr::from([127, 0, 0, 1])];
        example.capacity.reserved_memory_mb = 2048;
        let sections = |c: &Config| {
            [
                ("ops", format!("{:?}", c.ops)),
                ("docker", format!("{:?}", c.docker)),
                ("network", format!("{:?}", c.network)),
                ("workloads", format!("{:?}", c.workloads)),
                ("capacity", format!("{:?}", c.capacity)),
                ("intervals", format!("{:?}", c.intervals)),
                ("transfer", format!("{:?}", c.transfer)),
            ]
        };
        for ((name, left_out), (_, written)) in sections(&minimal).into_iter().zip(sections(&example)) {
            assert_eq!(left_out, written, "[{name}]");
        }
    }

    #[test]
    fn unknown_keys_in_a_section_are_refused() {
        for section in ["ops", "docker", "network", "workloads", "capacity", "intervals", "transfer"] {
            let unknown = format!("{MINIMAL}\n[{section}]\nfoo = 1\n");
            let refused = toml::from_str::<Config>(&unknown).expect_err(section).to_string();
            assert!(refused.contains("unknown field `foo`"), "{section}: {refused}");
        }
    }

    /// The example's fleet mode, switched on as its comment says: `node_id` and `[api.tls]`
    /// removed, `[fleet]` uncommented.
    #[test]
    fn the_example_fleet_mode_is_valid() {
        let (mut out, mut in_tls, mut in_fleet) = (Vec::new(), false, false);
        for line in EXAMPLE.lines() {
            if line.starts_with("node_id") {
                continue;
            }
            if line.starts_with('[') {
                in_tls = line.trim() == "[api.tls]";
            }
            if in_tls {
                continue;
            }
            in_fleet |= line.trim() == "# [fleet]";
            match line.strip_prefix("# ") {
                Some(rest) if in_fleet && !rest.starts_with('#') => out.push(rest),
                _ if in_fleet => {}
                _ => out.push(line),
            }
        }
        let config: Config = toml::from_str(&out.join("\n")).unwrap();
        config.validate().unwrap();
        let fleet = config.fleet.expect("fleet mode");
        assert_eq!((fleet.heartbeat_seconds, fleet.restart_requires_contact_seconds), (5, 120));
        assert!(config.api.tls.is_none() && config.node_id.as_str().is_empty());
    }

    #[test]
    fn addresses_others_dial_are_never_unspecified() {
        let everywhere = MINIMAL.to_owned() + "\n[network]\nedge_ips = [\"0.0.0.0\"]\n";
        let config: Config = toml::from_str(&everywhere).unwrap();
        assert!(config.validate().unwrap_err().to_string().contains("not 0.0.0.0"));

        let fleet = |listen: &str, api_address: &str| {
            format!(
                "deployment_id = \"staging\"\n[api]\nlisten = \"{listen}\"\n[fleet]\n\
                 url = \"https://control.internal:8443\"\nca = \"/etc/blocklyd/fleet-ca.pem\"\n{api_address}"
            )
        };
        let anywhere: Config = toml::from_str(&fleet("0.0.0.0:7443", "")).unwrap();
        assert!(anywhere.validate().unwrap_err().to_string().contains("fleet.api_address"));
        let told: Config = toml::from_str(&fleet("0.0.0.0:7443", "api_address = \"10.0.0.5:7443\"")).unwrap();
        told.validate().unwrap();
        let mistold: Config = toml::from_str(&fleet("10.0.0.5:7443", "api_address = \"0.0.0.0:7443\"")).unwrap();
        assert!(mistold.validate().is_err());
    }

    #[test]
    fn the_fleet_url_is_one_the_node_can_call() {
        let fleet = |url: &str| {
            let config: Config = toml::from_str(&format!(
                "deployment_id = \"staging\"\n[api]\nlisten = \"10.0.0.5:7443\"\n[fleet]\nurl = \"{url}\"\n\
                 ca = \"/etc/blocklyd/fleet-ca.pem\"\n"
            ))
            .unwrap();
            config.validate()
        };
        for url in
            ["https://fleet.internal.example", "https://10.0.0.2:8443", "https://[fd00::1]:8443/", "https://h/base"]
        {
            fleet(url).unwrap_or_else(|e| panic!("{url}: {e}"));
        }
        // Paths are added to it as text, so a query or a fragment would swallow them.
        for url in [
            "https://",
            "https://:8443",
            "https://control plane.internal:8443",
            "https://h:8443?x=1",
            "https://h:8443#x",
            "https://h:99999",
            "http://h:8443",
            "HTTPS://h:8443",
        ] {
            let refused = fleet(url).expect_err(url).to_string();
            assert!(refused.contains("fleet.url") && refused.contains("https://host[:port]"), "{url}: {refused}");
        }
    }

    #[test]
    fn the_ops_listener_stays_off_public_addresses() {
        let ops = |listen: &str| {
            let config: Config = toml::from_str(&format!("{MINIMAL}\n[ops]\nlisten = \"{listen}\"\n")).unwrap();
            config.validate()
        };
        // Loopback, RFC 1918, the shared range overlays use, IPv6 unique local and link-local.
        for listen in [
            "127.0.0.1:7070",
            "[::1]:7070",
            "10.0.0.5:7070",
            "172.31.255.1:7070",
            "192.168.1.5:7070",
            "100.64.0.1:7070",
            "100.127.255.254:7070",
            "[fd7a:115c:a1e0::1]:7070",
            "[fe80::1]:7070",
            "[::ffff:10.0.0.5]:7070",
        ] {
            ops(listen).unwrap_or_else(|e| panic!("{listen}: {e}"));
        }
        for listen in [
            "0.0.0.0:7070",
            "[::]:7070",
            "203.0.113.7:7070",
            "172.32.0.1:7070",
            "100.128.0.1:7070",
            "169.254.169.254:7070",
            "[2001:db8::5]:7070",
            "[::ffff:203.0.113.7]:7070",
        ] {
            let refused = ops(listen).expect_err(listen).to_string();
            assert!(refused.contains("ops.listen") && refused.contains("100.64.0.0/10"), "{refused}");
        }
    }

    #[test]
    fn root_workloads_and_unknown_keys_are_refused() {
        let workloads = |toml: &str| {
            let config: Config = toml::from_str(&format!("{MINIMAL}\n[workloads]\n{toml}\n")).unwrap();
            config.validate()
        };
        for (toml, key) in [
            ("user = \"0:0\"", "workloads.user"),
            ("user = \"1000:0\"", "workloads.user"),
            ("data_owner = \"0:0\"", "workloads.data_owner"),
            ("data_owner = \"0:1000\"", "workloads.data_owner"),
            ("data_owner = \"101000:0\"", "workloads.data_owner"),
        ] {
            let refused = workloads(toml).expect_err(toml).to_string();
            assert!(refused.contains(key) && refused.contains("root"), "{refused}");
        }
        let named = format!("{MINIMAL}\n[workloads]\ndata_owner = \"root\"\n");
        assert!(toml::from_str::<Config>(&named).unwrap_err().to_string().contains("numeric uid:gid"));
        workloads("data_owner = \"101000:101000\"").unwrap();
        let unknown = MINIMAL.to_owned() + "\nprivileged = true\n";
        assert!(toml::from_str::<Config>(&unknown).is_err());
    }
}
