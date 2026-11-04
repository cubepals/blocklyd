// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! The TLS handshake that opens every API connection, bounded: how many may be in progress at
//! once, how long one may take, and how the ones refused are reported.
//!
//! A connection in its handshake holds a file descriptor and has proved nothing, and anyone who
//! reaches the port can open one: a workload reaches the host's private address. So handshakes in
//! progress are capped, overall and per peer address, and a connection past a cap is closed at
//! once. One whose handshake has ended no longer counts: a client that finished it proved who it
//! is, and `api.rs` serves it.
//!
//! Each refusal is counted (`blocklyd_auth_failures_total`) and logged at debug. A warning for each
//! would let anyone flood the journal past its rate limit and silence blocklyd's other warnings,
//! so one warning a minute at most sums them up.

use std::collections::HashMap;
use std::fmt::Display;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use crate::metrics::Metrics;

/// How long a client may take to finish its handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The least time between two warnings that sum up refused handshakes.
const SUMMARY_EVERY: Duration = Duration::from_mins(1);

/// How many TLS handshakes may be in progress at once.
#[derive(Clone, Copy, Debug)]
pub struct HandshakeLimits {
    /// From every peer together: what bounds the file descriptors they can hold.
    pub total: usize,
    /// From one peer address. The control plane is one address and may open many connections at
    /// once, so this leaves it room.
    pub per_peer: usize,
}

impl Default for HandshakeLimits {
    fn default() -> Self {
        Self { total: 512, per_peer: 64 }
    }
}

/// The handshakes of one listener: its TLS, the caps it holds them to, and their refusals.
pub(crate) struct Handshakes {
    acceptor: TlsAcceptor,
    gate: Arc<Gate>,
    refusals: Arc<Refusals>,
    metrics: Arc<Metrics>,
}

impl Handshakes {
    pub(crate) fn new(tls: Arc<rustls::ServerConfig>, limits: HandshakeLimits, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            acceptor: TlsAcceptor::from(tls),
            gate: Gate::new(limits),
            refusals: Refusals::new(SUMMARY_EVERY),
            metrics,
        })
    }

    /// A place in a handshake for a connection from `peer`, or None when a cap is full: the
    /// connection is then dropped, before it costs a task, and its refusal is counted.
    pub(crate) fn admit(&self, peer: SocketAddr) -> Option<Place> {
        match self.gate.enter(peer.ip()) {
            Ok(place) => Some(place),
            Err(full) => {
                self.metrics.auth_failure("tls_handshakes_full");
                self.refusals.refused(peer, &full);
                None
            }
        }
    }

    /// The connection's TLS, once its handshake ends well within `HANDSHAKE_TIMEOUT`. Its place is
    /// given back when the handshake ends, however it ends.
    pub(crate) async fn accept(&self, tcp: TcpStream, peer: SocketAddr, place: Place) -> Option<TlsStream<TcpStream>> {
        let accepted = tokio::time::timeout(HANDSHAKE_TIMEOUT, self.acceptor.accept(tcp)).await;
        drop(place);
        match accepted {
            Ok(Ok(stream)) => Some(stream),
            Ok(Err(e)) => {
                self.metrics.auth_failure("tls_handshake");
                self.refusals.refused(peer, &e);
                None
            }
            Err(_) => {
                self.metrics.auth_failure("tls_handshake_timeout");
                self.refusals.refused(peer, &format_args!("no handshake within {HANDSHAKE_TIMEOUT:?}"));
                None
            }
        }
    }
}

/// Handshakes in progress, held to `HandshakeLimits`.
struct Gate {
    total: Arc<Semaphore>,
    per_peer: usize,
    /// Handshakes in progress per peer address. An address leaves once it has none, so this holds
    /// no more entries than the total cap lets in.
    peers: Mutex<HashMap<IpAddr, usize>>,
}

/// Why a connection found no place in a handshake.
#[derive(Debug, thiserror::Error)]
enum Full {
    #[error("as many TLS handshakes are in progress as are let in at once")]
    Total,
    #[error("as many TLS handshakes are in progress from this address as one address may have")]
    Peer,
}

/// A place in a handshake, given back when dropped.
pub(crate) struct Place {
    _total: OwnedSemaphorePermit,
    peer: IpAddr,
    gate: Arc<Gate>,
}

impl Gate {
    fn new(limits: HandshakeLimits) -> Arc<Self> {
        Arc::new(Self {
            total: Arc::new(Semaphore::new(limits.total)),
            per_peer: limits.per_peer,
            peers: Mutex::default(),
        })
    }

    fn enter(self: &Arc<Self>, peer: IpAddr) -> Result<Place, Full> {
        let total = self.total.clone().try_acquire_owned().map_err(|_| Full::Total)?;
        let mut peers = self.peers.lock().unwrap();
        if peers.get(&peer).copied().unwrap_or(0) >= self.per_peer {
            return Err(Full::Peer);
        }
        *peers.entry(peer).or_insert(0) += 1;
        drop(peers);
        Ok(Place { _total: total, peer, gate: self.clone() })
    }
}

impl Drop for Place {
    fn drop(&mut self) {
        let mut peers = self.gate.peers.lock().unwrap();
        if let Some(held) = peers.get_mut(&self.peer) {
            *held -= 1;
            if *held == 0 {
                peers.remove(&self.peer);
            }
        }
    }
}

/// Refused handshakes, summed up in at most one warning every `every`.
struct Refusals {
    every: Duration,
    window: Mutex<Window>,
}

/// The refusals a warning still has to sum up.
#[derive(Default)]
struct Window {
    /// Whether a warning is on its way: the first refusal after the last warning sends one.
    open: bool,
    refused: u64,
    latest: Option<(IpAddr, String)>,
}

impl Refusals {
    fn new(every: Duration) -> Arc<Self> {
        Arc::new(Self { every, window: Mutex::default() })
    }

    /// One handshake refused, and why: logged at debug now, and summed up `every` after the first
    /// refusal since the last warning.
    fn refused(self: &Arc<Self>, peer: SocketAddr, why: &dyn Display) {
        tracing::debug!(%peer, error = %why, "TLS handshake refused");
        let mut window = self.window.lock().unwrap();
        window.refused += 1;
        window.latest = Some((peer.ip(), why.to_string()));
        if !window.open {
            window.open = true;
            let refusals = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(refusals.every).await;
                refusals.sum_up();
            });
        }
    }

    fn sum_up(&self) {
        let window = std::mem::take(&mut *self.window.lock().unwrap());
        if let Some((peer, why)) = window.latest {
            tracing::warn!(
                refused = window.refused,
                within_seconds = self.every.as_secs(),
                latest_peer = %peer,
                latest_error = %why,
                "TLS handshakes refused"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;

    fn at(last: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, last))
    }

    #[test]
    fn places_are_capped_per_address_and_overall_and_given_back() {
        let gate = Gate::new(HandshakeLimits { total: 3, per_peer: 2 });
        let a1 = gate.enter(at(1)).unwrap();
        let a2 = gate.enter(at(1)).unwrap();
        assert!(matches!(gate.enter(at(1)), Err(Full::Peer)), "the address has as many as it may");
        let b1 = gate.enter(at(2)).unwrap();
        assert!(matches!(gate.enter(at(3)), Err(Full::Total)), "every place is taken");
        drop(a1);
        let a3 = gate.enter(at(1)).expect("a place given back is taken again");
        drop((a2, a3, b1));
        assert!(gate.peers.lock().unwrap().is_empty(), "an address with none in progress is forgotten");
        assert_eq!(gate.total.available_permits(), 3);
    }

    /// What tracing writes, kept to be read.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn refusals_are_summed_up_in_one_warning_a_window() {
        let captured = Captured::default();
        let writer = captured.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::DEBUG)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        let refusals = Refusals::new(Duration::from_millis(200));
        let peer: SocketAddr = "10.0.0.9:4000".parse().unwrap();
        for n in 0..100 {
            refusals.refused(peer, &format_args!("refusal {n}"));
        }
        tokio::time::sleep(Duration::from_millis(600)).await;
        refusals.refused(peer, &"after the window");
        let text = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
        let warnings: Vec<&str> = text.lines().filter(|l| l.contains("WARN")).collect();
        assert_eq!(warnings.len(), 1, "{text}");
        assert!(warnings[0].contains("refused=100") && warnings[0].contains("refusal 99"), "{}", warnings[0]);
        assert_eq!(text.lines().filter(|l| l.contains("DEBUG") && l.contains("TLS handshake refused")).count(), 101);
    }
}
