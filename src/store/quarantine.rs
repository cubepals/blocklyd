//! Released host ports still in quarantine (`RestingPort`), kept in `ports.json` so a restarted
//! blocklyd doesn't hand out a port a route may still point at. Which ports rest, and for how long,
//! is `ports.rs`'s.

use std::fs;

use super::{RestingPort, Store, StoreError};
use crate::durable;

impl Store {
    /// Quarantined ports survive restarts, so a restarted daemon can't hand out a port a route
    /// may still point at. Rewritten whole, atomically, on each release.
    pub(crate) fn save_resting_ports(&self, ports: &[RestingPort]) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec_pretty(ports).expect("serializes");
        Ok(durable::write_atomic(&self.root.join("ports.json"), &bytes, 0o600)?)
    }

    pub(crate) fn load_resting_ports(&self) -> Vec<RestingPort> {
        fs::read(self.root.join("ports.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Proto;

    #[test]
    fn ports_in_quarantine_are_read_back_as_saved() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let resting = [RestingPort { protocol: Proto::Udp, port: 42001, released_at_unix: 1_000 }];
        store.save_resting_ports(&resting).unwrap();
        assert_eq!(store.load_resting_ports(), resting);
    }
}
