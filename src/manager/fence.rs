//! Fences a workload a newer placement has superseded: restarts off, stopped, never started
//! again, and stopped again should a recorded fence find it running. The epoch check every other
//! verb makes is `check_epoch` in `manager.rs`; removing the copy is `delete.rs`.

use super::*;

impl Manager {
    // ─── fence ─────────────────────────────────────────────────────────────────────────────

    /// The control plane placed this workload anew (`current_epoch`, usually on another node): a
    /// copy older than that is stopped, its restart policy switched off, and it is never started
    /// again. Data is kept for the control plane to delete. A copy at or past the current epoch
    /// is left alone, so a late fence changes nothing. A repeated one fences the copy again
    /// (restarts off, stopped if it runs) and answers `changed: false`.
    pub async fn fence(self: &Arc<Self>, id: WorkloadId, current_epoch: u64) -> Result<FenceResponse, NodeError> {
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.fence_locked(&id, current_epoch).await;
        self.metrics.operation("fence", &result, started.elapsed());
        result
    }

    async fn fence_locked(&self, id: &WorkloadId, current_epoch: u64) -> Result<FenceResponse, NodeError> {
        let mut record = self.record(id)?;
        if record.epoch.is_some_and(|e| e >= current_epoch) {
            return Ok(FenceResponse { changed: false, stopped: false, workload: self.view(id)? });
        }
        let was_fenced = record.superseded_by.is_some();
        self.state.lock().unwrap().restart_due.remove(id);
        // Recorded before anything else: from here on nothing starts this copy, even if the stop
        // below fails or blocklyd dies halfway. In memory first, so a record the disk can't take
        // (full, read-only) still keeps this run from restarting or resuming it; after a restart
        // the control plane fences it again before granting any lease.
        record.superseded_by = Some(record.superseded_by.map_or(current_epoch, |b| b.max(current_epoch)));
        record.updated_at = now_str();
        self.state.lock().unwrap().records.insert(id.clone(), record.clone());
        // Nor does it keep the copy running: it is stopped all the same, and the record that
        // couldn't be written is reported once it is.
        let mut unrecorded = self.save_record(&record).err();
        let mut stopped = false;
        if record.phase == Phase::Active
            && let Some(info) = self.observe(&record).await?
        {
            // Every container blocklyd makes has Docker's restart policy `no` already: this only
            // makes sure, and failing at it mustn't leave the copy running.
            if let Err(e) = self.note(self.runtime.disable_restart(&record.container_name).await) {
                tracing::warn!(workload = %id, error = %e, "couldn't switch the fenced copy's restarts off");
            }
            if matches!(info.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused) {
                record.stop_requested_at = Some(now_str());
                self.state.lock().unwrap().records.insert(id.clone(), record.clone());
                if let Err(e) = self.save_record(&record) {
                    unrecorded.get_or_insert(e);
                }
                let grace = Duration::from_secs(record.spec.stop.timeout_seconds as u64);
                self.state.lock().unwrap().stopping.insert(id.clone());
                let result = self.runtime.stop(&record.container_name, record.spec.stop.signal.as_str(), grace).await;
                self.state.lock().unwrap().stopping.remove(id);
                self.note(result)?;
                stopped = true;
            }
            self.observe(&record).await?;
        }
        tracing::warn!(workload = %id, epoch = ?record.epoch, current_epoch, stopped, "fenced a superseded copy");
        if let Some(e) = unrecorded {
            return Err(e);
        }
        Ok(FenceResponse { changed: !was_fenced, stopped, workload: self.view(id)? })
    }

    /// Fences again each copy recorded as superseded that still runs: one whose fence was recorded
    /// but couldn't be carried out (the runtime didn't answer). The control plane sends a fence
    /// only until the node reports it recorded, so otherwise nothing ever would. Called after a
    /// successful resync, so what it goes by is current. Each fence runs in its own task, since it
    /// takes the workload's lock and a resync takes none; a workload busy with something else (a
    /// stop, a delete, a fence already under way) is looked at again at the next resync.
    pub fn reapply_fences(self: &Arc<Self>) {
        let superseded: Vec<(WorkloadId, u64)> = {
            let state = self.state.lock().unwrap();
            let runs = |id: &WorkloadId| {
                state.observed.get(id).and_then(|o| o.info.as_ref()).is_some_and(|i| {
                    matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused)
                })
            };
            state
                .records
                .values()
                .filter(|r| r.phase == Phase::Active && runs(&r.id))
                .filter_map(|r| r.superseded_by.map(|by| (r.id.clone(), by)))
                .collect()
        };
        for (id, by) in superseded {
            if self.lock_for(&id).try_lock().is_err() {
                continue;
            }
            tracing::warn!(workload = %id, superseded_by = by, "a superseded copy still runs; fencing it again");
            let this = self.clone();
            tokio::spawn(async move {
                if let Err(e) = this.fence(id.clone(), by).await {
                    tracing::warn!(workload = %id, error = %e, "couldn't fence the superseded copy again");
                }
            });
        }
    }
}
