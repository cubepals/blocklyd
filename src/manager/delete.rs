//! Removes a workload's container, keeping its data as a retained record unless told to trash
//! it. Stopping without removing is `power.rs`; deleting a snapshot is `snapshots.rs`.

use super::*;

impl Manager {
    // ─── delete ────────────────────────────────────────────────────────────────────────────

    pub async fn delete(
        self: &Arc<Self>,
        id: WorkloadId,
        data: DataDisposition,
        confirmation: Option<String>,
        epoch: Option<u64>,
    ) -> Result<DeleteResponse, NodeError> {
        if data == DataDisposition::Delete && confirmation.as_deref() != Some(id.as_str()) {
            return Err(NodeError::ConfirmationRequired);
        }
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.delete_locked(&id, data, epoch).await;
        self.metrics.operation("delete", &result, started.elapsed());
        result
    }

    async fn delete_locked(
        &self,
        id: &WorkloadId,
        data: DataDisposition,
        epoch: Option<u64>,
    ) -> Result<DeleteResponse, NodeError> {
        let Some(mut record) = self.state.lock().unwrap().records.get(id).cloned() else {
            return Ok(DeleteResponse {
                existed: false,
                removed_container: false,
                data: DataOutcome::Absent,
                trash_path: None,
            });
        };
        check_epoch(&record, epoch, EpochRule::Teardown)?;
        let mut removed_container = false;
        let info = if record.phase == Phase::Retained { None } else { self.inspect(&record.container_name).await? };
        if let Some(info) = info {
            // Delete while running: the workload still gets its graceful stop first.
            if matches!(info.status, ContainerStatus::Running | ContainerStatus::Restarting) {
                record.stop_requested_at = Some(now_str());
                self.save_record(&record)?;
                let grace = Duration::from_secs(record.spec.stop.timeout_seconds as u64);
                let stopping = self.mark_stopping(id);
                let stopped = self.runtime.stop(&record.container_name, record.spec.stop.signal.as_str(), grace).await;
                drop(stopping);
                // A stop that fails is still followed by a forced removal.
                if let Err(e) = self.note(stopped)
                    && e.is_unavailable()
                {
                    return Err(e.into());
                }
            }
            self.note(self.runtime.remove(&record.container_name).await)?;
            removed_container = true;
        }
        {
            let mut state = self.state.lock().unwrap();
            state.ports.release_all(id);
            state.observed.remove(id);
            state.stats.remove(id);
            state.issues.remove(id);
        }
        self.persist_resting_ports();
        match data {
            DataDisposition::Keep => {
                record.phase = Phase::Retained;
                record.container_id = None;
                record.ports.clear();
                record.updated_at = now_str();
                self.save_record(&record)?;
                Ok(DeleteResponse { existed: true, removed_container, data: DataOutcome::Kept, trash_path: None })
            }
            DataDisposition::Delete => {
                // In turn with the record's other writes: one waiting for its turn finds no record
                // left to write, and doesn't bring back the file just trashed.
                let turn = self.disk_writes.lock().unwrap();
                let path = self.store.trash(id, now().unix_timestamp())?;
                let mut state = self.state.lock().unwrap();
                state.records.remove(id);
                state.disk.remove(id);
                drop(state);
                drop(turn);
                self.locks.lock().unwrap().remove(id);
                Ok(DeleteResponse {
                    existed: true,
                    removed_container,
                    data: if path.is_some() { DataOutcome::Trashed } else { DataOutcome::Absent },
                    trash_path: path.map(|p| p.to_string_lossy().into_owned()),
                })
            }
        }
    }
}
