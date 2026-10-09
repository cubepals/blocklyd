//! What `blocklyd join` asks of the host once its files are in place: systemd, to stop and start
//! the unit, and the blocklyd it started, whether the control plane has accepted a heartbeat from
//! it yet. That is read from the ops listener's metrics, which answer without a certificate.
//!
//! A re-enrollment (reenroll.rs) goes through `Service` for both, so its tests stand in for the
//! host. Not here: what join writes, or when.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context;
use hyper::Method;

use crate::http_client as client;

pub(crate) trait Service {
    /// `systemctl` with these arguments; an error when it fails.
    fn systemctl(&self, args: &[&str]) -> anyhow::Result<()>;
    /// Whether the blocklyd whose ops listener is `ops` has had a heartbeat accepted, by `within`
    /// from now.
    fn first_beat(&self, ops: SocketAddr, within: Duration) -> impl Future<Output = bool>;
}

/// The host's own systemd; `program` is another `systemctl` in tests that run blocklyd as a plain
/// process.
pub(crate) struct Systemd {
    pub(crate) program: PathBuf,
}

impl Service for Systemd {
    fn systemctl(&self, args: &[&str]) -> anyhow::Result<()> {
        let status = std::process::Command::new(&self.program)
            .args(args)
            .status()
            .with_context(|| format!("running {}", self.program.display()))?;
        anyhow::ensure!(status.success(), "systemctl {} failed ({status})", args.join(" "));
        Ok(())
    }

    async fn first_beat(&self, ops: SocketAddr, within: Duration) -> bool {
        let ip = match ops.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip => ip,
        };
        let url = format!("http://{}/metrics", SocketAddr::new(ip, ops.port()));
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            let answer = client::send(Method::GET, &url, &[], client::empty(), None, Duration::from_secs(2)).await;
            if let Ok(response) = answer
                && let Ok(metrics) = client::read_body(response, 4 << 20).await
                && beat_accepted(&metrics)
            {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        false
    }
}

/// Whether the metrics count an accepted heartbeat: the counter starts with the process, so this
/// is this blocklyd's, not the one before it.
fn beat_accepted(metrics: &[u8]) -> bool {
    String::from_utf8_lossy(metrics).lines().any(|line| {
        line.strip_prefix(r#"blocklyd_heartbeats_total{outcome="ok"} "#)
            .and_then(|count| count.trim().parse::<f64>().ok())
            .is_some_and(|count| count >= 1.0)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_beat_counts_once_the_control_plane_accepted_one() {
        let metrics = |ok: &str| {
            format!("# TYPE blocklyd_heartbeats counter\n{ok}blocklyd_heartbeats_total{{outcome=\"error\"}} 3\n")
        };
        assert!(!beat_accepted(metrics("").as_bytes()));
        assert!(!beat_accepted(metrics("blocklyd_heartbeats_total{outcome=\"ok\"} 0\n").as_bytes()));
        assert!(beat_accepted(metrics("blocklyd_heartbeats_total{outcome=\"ok\"} 1\n").as_bytes()));
    }
}
