//! Brings a workload back without being asked: after a crash, and after the host went down
//! under it. Only if it fits (`room.rs`), and in fleet mode only under the execution lease
//! (`fleet.rs`). Requested starts are in `power.rs`; noticing a crash is `remember` in
//! `manager.rs`.

use super::*;

impl Manager {
    // ─── restarts after failures ───────────────────────────────────────────────────────────

    /// Applies each workload's restart policy: a failure blocklyd saw happen (see `remember`) is
    /// started again after a backoff, up to the spec's `maxRetries` in one run, and like any start
    /// only if it fits in memory. Never a copy that was asked to stop or was fenced. In fleet mode,
    /// only while the control plane was heard from recently: a node cut off from it doesn't bring
    /// anything back on its own.
    pub async fn restart_supervisor(self: Arc<Self>, cancel: tokio_util::sync::CancellationToken) {
        loop {
            let next = self.state.lock().unwrap().restart_due.values().min().copied();
            let wait = next.map_or(Duration::from_secs(3600), |at| at.saturating_duration_since(Instant::now()));
            tokio::select! {
                () = tokio::time::sleep(wait) => {}
                () = self.restart_wake.notified() => continue,
                () = cancel.cancelled() => return,
            }
            let due: Vec<(WorkloadId, Instant)> = {
                let state = self.state.lock().unwrap();
                let now = Instant::now();
                state.restart_due.iter().filter(|(_, at)| **at <= now).map(|(id, at)| (id.clone(), *at)).collect()
            };
            for (id, at) in due {
                let outcome = self.try_restart(&id).await;
                // Only this failure's entry: one that failed again meanwhile keeps its own.
                let mut state = self.state.lock().unwrap();
                if state.restart_due.get(&id) == Some(&at) {
                    match outcome {
                        // Looked at again shortly: a resume waiting for a lease (one granted brings
                        // it forward), or a workload another operation holds.
                        Restart::Waiting => {
                            state.restart_due.insert(id, Instant::now() + RESUME_POLL);
                        }
                        Restart::Done => {
                            state.restart_due.remove(&id);
                        }
                    }
                }
            }
        }
    }

    async fn try_restart(&self, id: &WorkloadId) -> Restart {
        if self.state.lock().unwrap().resuming.contains(id) {
            return self.resume(id).await;
        }
        let lock = self.lock_for(id);
        // Never waits for the workload's lock: an export holds it for hours, and every other
        // workload's restart would wait behind it. A busy one gets its turn at the next look.
        let Ok(_guard) = lock.try_lock() else { return Restart::Waiting };
        let Ok(record) = self.record(id) else { return Restart::Done };
        let policy = &record.spec.restart;
        if record.phase != Phase::Active
            || record.superseded_by.is_some()
            || policy.policy != crate::protocol::RestartPolicy::OnFailure
            || policy.max_retries == 0
        {
            return Restart::Done;
        }
        let Ok(Some(info)) = self.observe(&record).await else { return Restart::Done };
        // Still failed, and not a requested stop (which reads as stopped, not crashed).
        if derive_state(&record, Some(&info), false, true) != WorkloadState::Crashed {
            return Restart::Done;
        }
        match self.lease() {
            Lease::NotFleet | Lease::Held => {}
            lease => {
                tracing::warn!(workload = %id, ?lease, "not restarting after a failure: this node holds no execution lease");
                return Restart::Done;
            }
        }
        let attempt = record.restart_count;
        if attempt >= policy.max_retries {
            tracing::warn!(workload = %id, restarts = attempt, "failed again; its restart policy is spent");
            return Restart::Done;
        }
        // Admitted like any start: memory the host gave out since the failure isn't taken back.
        // Refused, the workload stays crashed, and says why.
        if let Err(e) = self.admit(id, record.spec.resources.memory_mb as u64) {
            tracing::warn!(workload = %id, error = %e, "not restarting after a failure: no room for it now");
            self.set_issue(id, Issue::new("insufficient_capacity", format!("not restarted after a failure: {e}")));
            return Restart::Done;
        }
        self.count_restart(id, attempt + 1);
        match self.note(self.runtime.start(&record.container_name).await) {
            Ok(()) => {
                tracing::warn!(workload = %id, attempt = attempt + 1, exit = info.exit_code, "restarted after a failure")
            }
            Err(e) => tracing::warn!(workload = %id, error = %e, "couldn't restart after a failure"),
        }
        let _ = self.observe(&record).await;
        Restart::Done
    }

    /// Counts a restart after a failure: in memory first, so this run counts it even if the disk
    /// can't take the record, then on disk, so a restart of blocklyd doesn't give a crash loop its
    /// retries again.
    fn count_restart(&self, id: &WorkloadId, count: u32) {
        if let Some(record) = self.state.lock().unwrap().records.get_mut(id) {
            record.restart_count = count;
        }
        if let Err(e) = self.write_record(id) {
            tracing::warn!(workload = %id, error = %e, "couldn't record the restart");
        }
    }

    // ─── resuming after the host restarts ──────────────────────────────────────────────────

    /// After the first full pass in a run, finds the workloads the host went down under: running
    /// in an earlier boot, never asked to stop, and stopped now. Each is resumed as its restart
    /// policy would restart a failure, and reads as restarting until then.
    pub(super) fn schedule_resumes(&self) {
        let Some(boot) = self.boot_id.clone() else { return };
        if self.resume_checked.swap(true, Ordering::SeqCst) {
            return;
        }
        let mut state = self.state.lock().unwrap();
        let ids: Vec<WorkloadId> = state
            .records
            .values()
            .filter(|r| resumable(r, &boot))
            .filter(|r| {
                state.observed.get(&r.id).and_then(|o| o.info.as_ref()).is_some_and(|i| {
                    matches!(i.status, ContainerStatus::Exited | ContainerStatus::Dead | ContainerStatus::Created)
                })
            })
            .map(|r| r.id.clone())
            .collect();
        let now = Instant::now();
        for id in &ids {
            state.resuming.insert(id.clone());
            state.restart_due.insert(id.clone(), now);
        }
        drop(state);
        if !ids.is_empty() {
            tracing::info!(
                workloads = ids.len(),
                waits_for_lease = self.config.fleet.is_some(),
                "the host restarted under running workloads; resuming them"
            );
            self.restart_wake.notify_one();
        }
    }

    /// Starts a workload the host went down under. In fleet mode only under a lease, which the
    /// control plane grants in the same answer that fences any copy placed elsewhere meanwhile:
    /// a node that comes back after its servers moved never starts them here.
    async fn resume(&self, id: &WorkloadId) -> Restart {
        let lock = self.lock_for(id);
        let Ok(_guard) = lock.try_lock() else { return Restart::Waiting };
        let Ok(record) = self.record(id) else {
            self.state.lock().unwrap().resuming.remove(id);
            return Restart::Done;
        };
        let current = self.boot_id.clone().unwrap_or_default();
        if !resumable(&record, &current) {
            self.state.lock().unwrap().resuming.remove(id);
            return Restart::Done;
        }
        match self.lease() {
            Lease::NotFleet | Lease::Held => {}
            Lease::Lapsed => return Restart::Waiting,
            Lease::Revoked => {
                tracing::warn!(workload = %id, "not resuming: the control plane holds this node lost");
                self.give_up_resume(&record);
                return Restart::Done;
            }
        }
        let info = match self.observe(&record).await {
            Ok(info) => info,
            // The runtime doesn't answer: try again once it does.
            Err(_) => return Restart::Waiting,
        };
        let stopped = info.as_ref().is_some_and(|i| {
            matches!(i.status, ContainerStatus::Exited | ContainerStatus::Dead | ContainerStatus::Created)
        });
        if !stopped {
            self.state.lock().unwrap().resuming.remove(id);
            return Restart::Done;
        }
        if let Err(e) = self.admit(id, record.spec.resources.memory_mb as u64) {
            tracing::warn!(workload = %id, error = %e, "not resuming: no room for it now");
            self.give_up_resume(&record);
            return Restart::Done;
        }
        self.state.lock().unwrap().resuming.remove(id);
        match self.note(self.runtime.start(&record.container_name).await) {
            Ok(()) => tracing::info!(workload = %id, "resumed: the host restarted under it"),
            Err(e) => tracing::warn!(workload = %id, error = %e, "couldn't resume after the host restarted"),
        }
        let _ = self.observe(&record).await;
        Restart::Done
    }

    /// A resume that won't happen: forgotten, so a later restart of the host doesn't bring back a
    /// workload that wasn't running before it.
    fn give_up_resume(&self, record: &WorkloadRecord) {
        let mut record = record.clone();
        self.state.lock().unwrap().resuming.remove(&record.id);
        record.running_boot = None;
        if let Err(e) = self.save_record(&record) {
            tracing::warn!(workload = %record.id, error = %e, "couldn't record the abandoned resume");
        }
    }
}

/// How long a resume waiting for a lease, or a restart whose workload was busy, waits before it
/// looks again.
const RESUME_POLL: Duration = Duration::from_secs(5);

/// Whether the host went down under this workload: running in an earlier boot than `boot`, and
/// nothing since says it shouldn't run (a stop asked for, a fence, a policy against restarts).
fn resumable(record: &WorkloadRecord, boot: &str) -> bool {
    record.phase == Phase::Active
        && record.superseded_by.is_none()
        && record.stop_requested_at.is_none()
        && record.spec.restart.policy == crate::protocol::RestartPolicy::OnFailure
        && record.spec.restart.max_retries > 0
        && record.running_boot.as_deref().is_some_and(|b| b != boot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::fake::FakeRuntime;

    fn w() -> WorkloadId {
        WorkloadId::parse("w").unwrap()
    }

    /// blocklyd over the in-memory runtime, with `w` running under the default restart policy.
    async fn running() -> (tempfile::TempDir, Arc<FakeRuntime>, Arc<Manager>) {
        let dir = tempfile::tempdir().unwrap();
        let (uid, gid) = (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw());
        let config: Config = toml::from_str(&format!(
            "deployment_id = \"test\"\nnode_id = \"n\"\nstate_dir = \"{}\"\n[api]\nlisten = \"127.0.0.1:0\"\n\
             [workloads]\nallowed_images = [\"alpine:\"]\ndata_owner = \"{uid}:{gid}\"\n\
             [capacity]\nmin_free_disk_mb = 1\n",
            dir.path().display()
        ))
        .unwrap();
        let fake = Arc::new(FakeRuntime::new());
        let store = Store::open(&config.state_dir).unwrap();
        let m = Manager::new(Arc::new(config), fake.clone(), store, Arc::new(Metrics::new()), Arc::new(|_, _| true));
        assert!(m.reconcile(true).await.error.is_none());
        let spec = serde_json::from_value(serde_json::json!({
            "image": "alpine:3.22",
            "resources": { "memoryMb": 1024 },
            "storage": { "mountPath": "/data", "sizeGb": 1 },
        }))
        .unwrap();
        m.ensure(w(), spec, Precondition::None, None).await.unwrap();
        m.start(w(), None).await.unwrap();
        (dir, fake, m)
    }

    fn restart_due(m: &Manager) -> bool {
        m.state.lock().unwrap().restart_due.contains_key(&w())
    }

    #[tokio::test]
    async fn only_a_crash_is_due_a_restart_never_a_stop_that_ended_badly() {
        let (_dir, fake, m) = running().await;
        // Killed when asked: exit 137, and a stop all the same.
        m.kill(w(), None).await.unwrap();
        assert!(!restart_due(&m), "a requested kill");
        // A graceful stop the workload ignored, ended by SIGKILL.
        m.start(w(), None).await.unwrap();
        fake.make_stubborn("blockly-test-w");
        assert!(m.stop(w(), Some(1), None).await.unwrap().forced);
        assert!(!restart_due(&m), "a stop that needed a kill");
        // The same exit, unasked, is a crash.
        m.start(w(), None).await.unwrap();
        fake.crash("blockly-test-w", 137, false);
        m.stats(&w()).await.unwrap();
        assert!(restart_due(&m), "a crash blocklyd watched happen");
    }
}
