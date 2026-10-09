//! Host ports for workloads.
//!
//! blocklyd is the only allocator on its host, which is the point: Blockly's local
//! DockerRuntime chooses ports by listing containers and serializes choices inside one process,
//! so two control-plane workers provisioning onto the same remote Docker host could both pick
//! the same port, and Docker would only notice at start ("port is already allocated"). Here the
//! allocator lives next to the ports, behind one lock, and its reservations are persisted in
//! workload records (and container labels) before any container exists, so a restart never hands
//! a port out twice.
//!
//! A port is held for as long as its workload has compute, running or not: a stopped server keeps
//! its address. Released ports rest in quarantine before reuse, so a route still cached somewhere
//! (the edge polls every second) can't reach the next workload.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, TcpListener, UdpSocket};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::ids::WorkloadId;
use crate::protocol::Proto;

/// Whether the host could bind this port right now on every address it publishes on; catches
/// ports held by processes that aren't workloads. Injectable for tests.
pub type Probe = Arc<dyn Fn(Proto, u16) -> bool + Send + Sync>;

pub fn bind_probe(addresses: Vec<IpAddr>) -> Probe {
    Arc::new(move |proto, port| {
        addresses.iter().all(|ip| {
            let addr = SocketAddr::new(*ip, port);
            match proto {
                Proto::Tcp => TcpListener::bind(addr).is_ok(),
                Proto::Udp => UdpSocket::bind(addr).is_ok(),
            }
        })
    })
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum PortError {
    #[error("no free {proto} port left in {low}-{high}")]
    Exhausted { proto: &'static str, low: u16, high: u16 },
    #[error("{proto} port {port} is already held by workload {holder}")]
    Conflict { proto: &'static str, port: u16, holder: WorkloadId },
    #[error("port {0} is outside the range this host hands out")]
    OutOfRange(u16),
}

pub(crate) struct PortAllocator {
    range: RangeInclusive<u16>,
    held: HashMap<(Proto, u16), WorkloadId>,
    resting: HashMap<(Proto, u16), Instant>,
    quarantine: Duration,
    probe: Probe,
}

impl PortAllocator {
    pub(crate) fn new(range: RangeInclusive<u16>, quarantine: Duration, probe: Probe) -> Self {
        Self { range, held: HashMap::new(), resting: HashMap::new(), quarantine, probe }
    }

    /// Records a port a workload already holds (reconciliation). Two workloads claiming one port
    /// is a conflict the caller reports; the first claim keeps it.
    pub(crate) fn claim(&mut self, id: &WorkloadId, proto: Proto, port: u16) -> Result<(), PortError> {
        if !self.range.contains(&port) {
            // Still tracked, so nothing else is given it, but reported.
            self.held.entry((proto, port)).or_insert_with(|| id.clone());
            return Err(PortError::OutOfRange(port));
        }
        match self.held.get(&(proto, port)) {
            Some(holder) if holder != id => {
                Err(PortError::Conflict { proto: proto.as_str(), port, holder: holder.clone() })
            }
            _ => {
                self.held.insert((proto, port), id.clone());
                self.resting.remove(&(proto, port));
                Ok(())
            }
        }
    }

    /// The lowest port that is in range, not held, not resting, and bindable now.
    pub(crate) fn allocate(&mut self, id: &WorkloadId, proto: Proto) -> Result<u16, PortError> {
        let now = Instant::now();
        self.resting.retain(|_, since| now.duration_since(*since) < self.quarantine);
        for port in self.range.clone() {
            let key = (proto, port);
            if self.held.contains_key(&key) || self.resting.contains_key(&key) {
                continue;
            }
            if !(self.probe)(proto, port) {
                continue;
            }
            self.held.insert(key, id.clone());
            return Ok(port);
        }
        Err(PortError::Exhausted { proto: proto.as_str(), low: *self.range.start(), high: *self.range.end() })
    }

    /// Gives back one port (a port a replaced spec no longer names).
    pub(crate) fn release(&mut self, id: &WorkloadId, proto: Proto, port: u16) {
        if self.held.get(&(proto, port)) == Some(id) {
            self.held.remove(&(proto, port));
            self.resting.insert((proto, port), Instant::now());
        }
    }

    /// Takes back a port allocated moments ago that nothing used (the rest of the allocation
    /// failed). No workload published it and no route can know it, so it doesn't rest.
    pub(crate) fn unreserve(&mut self, id: &WorkloadId, proto: Proto, port: u16) {
        if self.held.get(&(proto, port)) == Some(id) {
            self.held.remove(&(proto, port));
        }
    }

    /// Gives back every port a workload holds.
    pub(crate) fn release_all(&mut self, id: &WorkloadId) {
        let now = Instant::now();
        let mine: Vec<_> = self.held.iter().filter(|(_, holder)| *holder == id).map(|(k, _)| *k).collect();
        for key in mine {
            self.held.remove(&key);
            self.resting.insert(key, now);
        }
    }

    pub(crate) fn allocated(&self) -> u32 {
        self.held.keys().filter(|(_, port)| self.range.contains(port)).count() as u32
    }

    /// Ports per protocol this host can hand out.
    pub(crate) fn capacity(&self) -> u32 {
        u32::from(*self.range.end() - *self.range.start()) + 1
    }

    #[cfg(test)]
    fn holder(&self, proto: Proto, port: u16) -> Option<&WorkloadId> {
        self.held.get(&(proto, port))
    }

    pub(crate) fn clear(&mut self) {
        self.held.clear();
    }

    /// Resting ports and how long ago each was released, for persisting across restarts.
    pub(crate) fn resting(&self) -> Vec<(Proto, u16, Duration)> {
        let now = Instant::now();
        self.resting
            .iter()
            .map(|((proto, port), since)| (*proto, *port, now.duration_since(*since)))
            .filter(|(_, _, ago)| *ago < self.quarantine)
            .collect()
    }

    /// Puts back ports that were resting before a restart, so quarantine outlives the process.
    pub(crate) fn restore_resting(&mut self, entries: impl IntoIterator<Item = (Proto, u16, Duration)>) {
        let now = Instant::now();
        for (proto, port, ago) in entries {
            if ago < self.quarantine
                && !self.held.contains_key(&(proto, port))
                && let Some(since) = now.checked_sub(ago)
            {
                self.resting.insert((proto, port), since);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn id(s: &str) -> WorkloadId {
        WorkloadId::parse(s).unwrap()
    }

    fn always() -> Probe {
        Arc::new(|_, _| true)
    }

    #[test]
    fn allocates_lowest_free_and_never_twice() {
        let mut ports = PortAllocator::new(42000..=42002, Duration::ZERO, always());
        assert_eq!(ports.allocate(&id("a"), Proto::Tcp), Ok(42000));
        assert_eq!(ports.allocate(&id("b"), Proto::Tcp), Ok(42001));
        assert_eq!(ports.allocate(&id("b"), Proto::Udp), Ok(42000), "protocols are separate spaces");
        assert_eq!(ports.allocate(&id("c"), Proto::Tcp), Ok(42002));
        assert!(matches!(ports.allocate(&id("d"), Proto::Tcp), Err(PortError::Exhausted { .. })));
    }

    #[test]
    fn released_ports_rest_before_reuse() {
        let mut ports = PortAllocator::new(42000..=42001, Duration::from_secs(600), always());
        assert_eq!(ports.allocate(&id("a"), Proto::Tcp), Ok(42000));
        ports.release_all(&id("a"));
        assert_eq!(ports.allocate(&id("b"), Proto::Tcp), Ok(42001), "42000 is resting");
        assert!(ports.allocate(&id("c"), Proto::Tcp).is_err());
    }

    #[test]
    fn an_unreserved_port_is_free_again_at_once() {
        let mut ports = PortAllocator::new(42000..=42001, Duration::from_secs(600), always());
        assert_eq!(ports.allocate(&id("a"), Proto::Tcp), Ok(42000));
        ports.unreserve(&id("b"), Proto::Tcp, 42000);
        assert_eq!(ports.holder(Proto::Tcp, 42000), Some(&id("a")), "only its holder takes it back");
        ports.unreserve(&id("a"), Proto::Tcp, 42000);
        assert!(ports.resting().is_empty(), "never published, so it doesn't rest");
        assert_eq!(ports.allocate(&id("c"), Proto::Tcp), Ok(42000));
    }

    #[test]
    fn without_quarantine_a_port_comes_back() {
        let mut ports = PortAllocator::new(42000..=42000, Duration::ZERO, always());
        ports.allocate(&id("a"), Proto::Tcp).unwrap();
        ports.release_all(&id("a"));
        assert_eq!(ports.allocate(&id("b"), Proto::Tcp), Ok(42000));
    }

    #[test]
    fn ports_held_by_other_processes_are_skipped() {
        let busy = Arc::new(Mutex::new(vec![42000u16]));
        let seen = busy.clone();
        let probe: Probe = Arc::new(move |_, port| !seen.lock().unwrap().contains(&port));
        let mut ports = PortAllocator::new(42000..=42001, Duration::ZERO, probe);
        assert_eq!(ports.allocate(&id("a"), Proto::Tcp), Ok(42001));
    }

    #[test]
    fn claims_rebuild_state_and_catch_conflicts() {
        let mut ports = PortAllocator::new(42000..=42010, Duration::ZERO, always());
        ports.claim(&id("a"), Proto::Tcp, 42003).unwrap();
        ports.claim(&id("a"), Proto::Tcp, 42003).unwrap();
        assert_eq!(
            ports.claim(&id("b"), Proto::Tcp, 42003),
            Err(PortError::Conflict { proto: "tcp", port: 42003, holder: id("a") })
        );
        assert_eq!(ports.holder(Proto::Tcp, 42003), Some(&id("a")), "first claim keeps it");
        assert_ne!(ports.allocate(&id("c"), Proto::Tcp), Ok(42003));
        assert!(matches!(ports.claim(&id("d"), Proto::Tcp, 80), Err(PortError::OutOfRange(80))));
    }

    #[test]
    fn the_real_probe_sees_a_bound_port() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let probe = bind_probe(vec![IpAddr::from([127, 0, 0, 1])]);
        assert!(!probe(Proto::Tcp, port));
        drop(listener);
        assert!(probe(Proto::Tcp, port));
    }

    #[test]
    fn quarantine_survives_a_snapshot_and_restore() {
        let mut before = PortAllocator::new(42000..=42001, Duration::from_secs(600), always());
        before.allocate(&id("a"), Proto::Tcp).unwrap();
        before.release_all(&id("a"));
        let snapshot = before.resting();
        assert_eq!(snapshot.len(), 1);
        let mut after = PortAllocator::new(42000..=42001, Duration::from_secs(600), always());
        after.restore_resting(snapshot);
        assert_eq!(after.allocate(&id("b"), Proto::Tcp), Ok(42001), "42000 still rests after the restart");
        let mut expired = PortAllocator::new(42000..=42001, Duration::from_secs(600), always());
        expired.restore_resting([(Proto::Tcp, 42000, Duration::from_secs(601))]);
        assert_eq!(expired.allocate(&id("c"), Proto::Tcp), Ok(42000), "an old release doesn't rest forever");
    }

    #[test]
    fn release_only_touches_the_holders_port() {
        let mut ports = PortAllocator::new(42000..=42001, Duration::ZERO, always());
        ports.allocate(&id("a"), Proto::Tcp).unwrap();
        ports.release(&id("b"), Proto::Tcp, 42000);
        assert_eq!(ports.holder(Proto::Tcp, 42000), Some(&id("a")));
        assert_eq!(ports.allocated(), 1);
    }
}
