//! The node's standing with its control plane: when it last answered, and the execution lease
//! it granted. Sending the heartbeats is `crate::fleet`; what a heartbeat reports is put together
//! in `crate::fleet::heartbeat`.

use super::*;

impl Manager {
    // ─── fleet ─────────────────────────────────────────────────────────────────────────────

    pub fn set_fleet(&self, node_id: &str, control_plane: &str) {
        let mut fleet = self.fleet.lock().unwrap();
        fleet.node_id = Some(node_id.to_owned());
        fleet.control_plane = Some(control_plane.to_owned());
    }

    /// Records a heartbeat's outcome: the lifecycle the control plane holds for this node, or why
    /// it couldn't be reached.
    pub fn fleet_contact(&self, result: Result<&str, &str>, latency: Duration) {
        let mut fleet = self.fleet.lock().unwrap();
        match result {
            Ok(lifecycle) => {
                fleet.lifecycle = Some(lifecycle.to_owned());
                fleet.last_contact = Some(Instant::now());
                fleet.last_contact_at = Some(now());
                fleet.last_error = None;
                fleet.last_latency_ms = Some(latency.as_millis() as u64);
                fleet.beats_ok += 1;
            }
            Err(e) => {
                fleet.last_error = Some(e.to_owned());
                fleet.beats_failed += 1;
            }
        }
        drop(fleet);
        self.metrics.heartbeat(result.is_ok());
    }

    /// The control plane answered a heartbeat sent at `sent`, granting `seconds` of execution
    /// lease (or `restart_requires_contact_seconds` from a control plane that names none). The
    /// answer's fences must be applied before this: a resume waiting for the lease goes ahead at
    /// once.
    pub fn grant_lease(&self, seconds: Option<u64>, sent: Instant) {
        let fallback = self.config.fleet.as_ref().map_or(0, |f| f.restart_requires_contact_seconds);
        let sent = boottime().saturating_sub(sent.elapsed());
        self.fleet.lock().unwrap().lease_until = Some(sent + Duration::from_secs(seconds.unwrap_or(fallback)));
        let mut state = self.state.lock().unwrap();
        if !state.resuming.is_empty() {
            let now = Instant::now();
            let waiting: Vec<WorkloadId> = state.resuming.iter().cloned().collect();
            for id in waiting {
                state.restart_due.insert(id, now);
            }
            drop(state);
            self.restart_wake.notify_one();
        }
    }

    /// What the execution lease allows now. A node restarts nothing on its own without one: a
    /// node cut off from the control plane may have been replaced.
    pub fn lease(&self) -> Lease {
        if self.config.fleet.is_none() {
            return Lease::NotFleet;
        }
        let fleet = self.fleet.lock().unwrap();
        if fleet.lifecycle.as_deref() == Some("lost") {
            return Lease::Revoked;
        }
        match fleet.lease_until {
            Some(until) if boottime() < until => Lease::Held,
            _ => Lease::Lapsed,
        }
    }

    /// What is left of the lease, in fleet mode once one was granted.
    pub fn lease_remaining(&self) -> Option<Duration> {
        self.config.fleet.as_ref()?;
        let until = self.fleet.lock().unwrap().lease_until?;
        Some(until.saturating_sub(boottime()))
    }

    pub fn certificate_renewed(&self) {
        self.fleet.lock().unwrap().renewed_at = Some(now());
    }

    /// Seconds since the control plane last answered a heartbeat, in fleet mode.
    pub fn fleet_contact_age(&self) -> Option<f64> {
        self.fleet.lock().unwrap().last_contact.map(|at| at.elapsed().as_secs_f64())
    }

    pub fn fleet_view(&self) -> Option<crate::protocol::FleetView> {
        let fleet = self.fleet.lock().unwrap().clone();
        let node_id = fleet.node_id?;
        Some(crate::protocol::FleetView {
            node_id,
            control_plane: fleet.control_plane.unwrap_or_default(),
            lifecycle: fleet.lifecycle,
            last_contact_at: fleet.last_contact_at.map(format_time),
            last_contact_age_seconds: fleet.last_contact.map(|at| at.elapsed().as_secs()),
            last_error: fleet.last_error,
            last_latency_ms: fleet.last_latency_ms,
            heartbeats_ok: fleet.beats_ok,
            heartbeats_failed: fleet.beats_failed,
            lease_remaining_seconds: fleet.lease_until.map(|until| until.saturating_sub(boottime()).as_secs()),
            certificate_renewed_at: fleet.renewed_at.map(format_time),
        })
    }
}

/// Time since the host booted, counting time it spent suspended or paused, which `Instant` doesn't:
/// a node that was frozen must not come back believing its lease is as fresh as it was.
fn boottime() -> Duration {
    rustix::time::clock_gettime_dynamic(rustix::time::DynamicClockId::Boottime)
        .map(|t| Duration::new(t.tv_sec.max(0) as u64, t.tv_nsec.clamp(0, 999_999_999) as u32))
        .unwrap_or_default()
}
