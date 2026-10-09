//! The seam between blocklyd and whatever runs containers. The manager speaks only these types;
//! Docker is one implementation (docker.rs), an in-memory fake another (fake.rs). A containerd or
//! microVM backend would be a third, and the protocol would not change.
//!
//! Parts (`runtime/`):
//! - `docker.rs`: the Docker Engine API, over the local unix socket.
//! - `fake.rs`: an in-memory runtime for tests, with the Docker behaviour blocklyd depends on.

pub mod docker;
#[cfg(any(test, feature = "fake"))]
pub mod fake;

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use time::OffsetDateTime;

use crate::protocol::{Health, Proto};

/// Everything needed to make one workload's container. Hardening is not optional here: the
/// manager always fills it from host policy.
#[derive(Clone)]
pub struct ContainerSpec {
    pub name: String,
    pub image: String,
    pub entrypoint: Option<Vec<String>>,
    /// Environment, secrets included. Never logged (see `Debug` below).
    pub env: Vec<(String, String)>,
    pub labels: BTreeMap<String, String>,
    pub user: String,
    pub memory_bytes: i64,
    pub nano_cpus: Option<i64>,
    pub cpu_shares: i64,
    pub pids_limit: i64,
    pub read_only_rootfs: bool,
    pub tmp_size_mb: u32,
    pub data_dir: PathBuf,
    pub mount_path: String,
    pub ports: Vec<PortBinding>,
    pub stop_signal: String,
    pub stop_timeout_secs: u32,
    pub network: String,
    pub log_max_size_mb: u32,
    pub log_max_files: u32,
    pub oom_score_adj: i32,
}

/// Names only for the environment: values may be secrets.
impl std::fmt::Debug for ContainerSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContainerSpec")
            .field("name", &self.name)
            .field("image", &self.image)
            .field("env", &self.env.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>())
            .field("memory_bytes", &self.memory_bytes)
            .field("nano_cpus", &self.nano_cpus)
            .field("ports", &self.ports)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PortBinding {
    pub container_port: u16,
    pub protocol: Proto,
    pub host_ips: Vec<IpAddr>,
    pub host_port: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContainerStatus {
    Created,
    Running,
    Paused,
    Restarting,
    Removing,
    Exited,
    Dead,
    Unknown,
}

/// A container as the runtime reports it right now.
#[derive(Clone, Debug)]
pub struct ContainerInfo {
    pub id: String,
    pub name: String,
    pub labels: BTreeMap<String, String>,
    pub env: BTreeMap<String, String>,
    pub status: ContainerStatus,
    pub exit_code: i64,
    pub oom_killed: bool,
    pub started_at: Option<OffsetDateTime>,
    pub finished_at: Option<OffsetDateTime>,
    pub restart_count: u32,
    pub health: Option<Health>,
}

#[derive(Clone, Debug, Default)]
pub struct ExecOutput {
    pub exit_code: Option<i64>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub timed_out: bool,
    pub killed: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogStream {
    Stdout,
    Stderr,
}

#[derive(Clone, Debug)]
pub struct LogChunk {
    pub stream: LogStream,
    /// One or more lines, each prefixed with an RFC 3339 timestamp and a space.
    pub bytes: Bytes,
}

#[derive(Clone, Debug, Default)]
pub struct LogOptions {
    pub tail: Option<u32>,
    pub since_unix: Option<i64>,
    pub follow: bool,
}

#[derive(Clone, Debug, Default)]
pub struct RawStats {
    pub cpu_total_ns: Option<u64>,
    pub cpu_throttled_periods: Option<u64>,
    pub memory_usage: Option<u64>,
    pub memory_inactive_file: Option<u64>,
    pub memory_limit: Option<u64>,
    pub pids: Option<u64>,
    pub pids_limit: Option<u64>,
    pub rx_bytes: Option<u64>,
    pub tx_bytes: Option<u64>,
}

/// Something happened to a container. A hint to look again, never the truth itself.
#[derive(Clone, Debug)]
pub struct RuntimeEvent {
    pub container_id: String,
    pub action: String,
    pub exit_code: Option<i64>,
    pub at: OffsetDateTime,
}

#[derive(Clone, Debug, Default)]
pub struct RuntimeInfo {
    pub version: Option<String>,
    pub api_version: Option<String>,
    pub cgroup_version: Option<String>,
    pub cgroup_driver: Option<String>,
    pub storage_driver: Option<String>,
    pub security_options: Vec<String>,
    pub live_restore: Option<bool>,
    /// The daemon's default log driver, for containers that don't name one (blocklyd's always do).
    pub log_driver: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct NetworkReport {
    /// False when the network lets workloads talk to each other; reported, not fixed, since the
    /// network may predate blocklyd.
    pub isolated: bool,
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum RuntimeError {
    /// The runtime can't be reached (daemon down, socket missing).
    #[error("container runtime unavailable: {0}")]
    Unavailable(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("conflict: {0}")]
    Conflict(String),
    #[error("timed out: {0}")]
    Timeout(String),
    #[error("runtime refused ({status}): {message}")]
    Rejected { status: u16, message: String },
    #[error("{0}")]
    Other(String),
}

impl RuntimeError {
    pub fn is_unavailable(&self) -> bool {
        matches!(self, RuntimeError::Unavailable(_))
    }
}

#[async_trait]
pub trait ContainerRuntime: Send + Sync + 'static {
    async fn info(&self) -> Result<RuntimeInfo, RuntimeError>;
    async fn ensure_network(
        &self,
        name: &str,
        labels: &BTreeMap<String, String>,
    ) -> Result<NetworkReport, RuntimeError>;
    /// Whether the network named `name` keeps containers from reaching each other; None if it
    /// doesn't exist. Looks only: `ensure_network` is what makes it.
    async fn network_isolated(&self, name: &str) -> Result<Option<bool>, RuntimeError>;
    /// Pulls the image if it isn't present. Returns whether it pulled.
    async fn ensure_image(&self, image: &str, timeout: Duration) -> Result<bool, RuntimeError>;
    async fn create(&self, spec: &ContainerSpec) -> Result<String, RuntimeError>;
    async fn start(&self, name: &str) -> Result<(), RuntimeError>;
    /// Sends `signal`, waits up to `grace`, then kills. Returns once it has exited.
    async fn stop(&self, name: &str, signal: &str, grace: Duration) -> Result<(), RuntimeError>;
    async fn kill(&self, name: &str) -> Result<(), RuntimeError>;
    /// Sets the container's restart policy to never, so the runtime can't bring back a copy
    /// blocklyd has fenced.
    async fn disable_restart(&self, name: &str) -> Result<(), RuntimeError>;
    /// Removes the container (not its bind-mounted data). Missing counts as removed.
    async fn remove(&self, name: &str) -> Result<(), RuntimeError>;
    async fn inspect(&self, name: &str) -> Result<Option<ContainerInfo>, RuntimeError>;
    /// Every container matching all `key=value` label filters.
    async fn list(&self, label_filters: &[String]) -> Result<Vec<ContainerInfo>, RuntimeError>;
    /// Runs `command` in the container with this id. blocklyd never names it here: a container
    /// made under the same name meanwhile is another one, and must not be the one it runs in.
    async fn exec(
        &self,
        container: &str,
        command: &[String],
        timeout: Duration,
        max_output: usize,
    ) -> Result<ExecOutput, RuntimeError>;
    fn logs(&self, name: &str, options: LogOptions) -> BoxStream<'static, Result<LogChunk, RuntimeError>>;
    async fn stats(&self, name: &str) -> Result<Option<RawStats>, RuntimeError>;
    fn events(&self, label_filters: &[String]) -> BoxStream<'static, Result<RuntimeEvent, RuntimeError>>;
}

/// RFC 3339 → time, with Docker's "never" (year 1) as None.
pub fn parse_time(value: Option<&str>) -> Option<OffsetDateTime> {
    let value = value?;
    let parsed = OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    (parsed.year() > 1970).then_some(parsed)
}

pub fn format_time(value: OffsetDateTime) -> String {
    value.format(&time::format_description::well_known::Rfc3339).unwrap_or_else(|_| value.unix_timestamp().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_zero_time_is_none() {
        assert!(parse_time(Some("0001-01-01T00:00:00Z")).is_none());
        assert!(parse_time(Some("2026-09-28T02:16:54.572388934Z")).is_some());
        assert!(parse_time(Some("garbage")).is_none());
        assert!(parse_time(None).is_none());
    }
}
