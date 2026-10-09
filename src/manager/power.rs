//! Changes whether a workload runs when the control plane asks: start, stop, kill. Restarts
//! blocklyd makes on its own are in `restarts.rs`; the stop a fence makes is in `fence.rs`.

use super::*;

impl Manager {
    // ─── power ─────────────────────────────────────────────────────────────────────────────

    pub async fn start(self: &Arc<Self>, id: WorkloadId, epoch: Option<u64>) -> Result<PowerResponse, NodeError> {
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.start_locked(&id, epoch).await;
        self.metrics.operation("start", &result, started.elapsed());
        result
    }

    async fn start_locked(&self, id: &WorkloadId, epoch: Option<u64>) -> Result<PowerResponse, NodeError> {
        let mut record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Exact)?;
        match record.phase {
            Phase::Creating => {
                return Err(NodeError::Conflict {
                    code: "not_created",
                    message: "the workload's creation didn't finish; PUT its spec again".into(),
                });
            }
            Phase::Retained => {
                return Err(NodeError::Conflict {
                    code: "no_compute",
                    message: "the workload was decommissioned; PUT its spec to bring it back".into(),
                });
            }
            Phase::Active => {}
        }
        let Some(info) = self.observe(&record).await? else {
            self.set_issue(id, Issue::new("container_missing", "the runtime no longer has this workload's container"));
            return Err(NodeError::Conflict {
                code: "container_missing",
                message: "the container is gone; PUT the spec to make it again".into(),
            });
        };
        if matches!(info.status, ContainerStatus::Running | ContainerStatus::Restarting) {
            return Ok(PowerResponse { changed: false, forced: false, workload: self.view(id)? });
        }
        self.admit(id, record.spec.resources.memory_mb as u64)?;
        self.state.lock().unwrap().restart_due.remove(id);
        // A requested start begins a new run, with all its retries.
        record.restart_count = 0;
        record.stop_requested_at = None;
        record.updated_at = now_str();
        self.save_record(&record)?;
        self.note(self.runtime.start(&record.container_name).await).map_err(|e| port_clash(id, e))?;
        self.state.lock().unwrap().last_failure.remove(id);
        self.clear_issue(id, "insufficient_capacity");
        self.observe(&record).await?;
        Ok(PowerResponse { changed: true, forced: false, workload: self.view(id)? })
    }

    pub async fn stop(
        self: &Arc<Self>,
        id: WorkloadId,
        grace: Option<u32>,
        epoch: Option<u64>,
    ) -> Result<PowerResponse, NodeError> {
        if let Some(g) = grace
            && !(1..=600).contains(&g)
        {
            return Err(NodeError::Invalid(vec![FieldError {
                field: "timeoutSeconds".into(),
                problem: "must be between 1 and 600".into(),
            }]));
        }
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.stop_locked(&id, grace, false, epoch).await;
        self.metrics.operation("stop", &result, started.elapsed());
        result
    }

    pub async fn kill(self: &Arc<Self>, id: WorkloadId, epoch: Option<u64>) -> Result<PowerResponse, NodeError> {
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.stop_locked(&id, None, true, epoch).await;
        self.metrics.operation("kill", &result, started.elapsed());
        result
    }

    async fn stop_locked(
        &self,
        id: &WorkloadId,
        grace: Option<u32>,
        kill: bool,
        epoch: Option<u64>,
    ) -> Result<PowerResponse, NodeError> {
        let mut record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Teardown)?;
        if record.phase != Phase::Active {
            return Ok(PowerResponse { changed: false, forced: false, workload: self.view(id)? });
        }
        let Some(info) = self.observe(&record).await? else {
            return Ok(PowerResponse { changed: false, forced: false, workload: self.view(id)? });
        };
        if !matches!(info.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused) {
            return Ok(PowerResponse { changed: false, forced: false, workload: self.view(id)? });
        }
        self.state.lock().unwrap().restart_due.remove(id);
        // Persisted before the signal, so the exit reads as a stop even if blocklyd dies now.
        record.stop_requested_at = Some(now_str());
        record.updated_at = now_str();
        self.save_record(&record)?;
        let result = if kill {
            self.runtime.kill(&record.container_name).await
        } else {
            let seconds = grace.unwrap_or(record.spec.stop.timeout_seconds);
            let _stopping = self.mark_stopping(id);
            self.runtime
                .stop(&record.container_name, record.spec.stop.signal.as_str(), Duration::from_secs(seconds as u64))
                .await
        };
        self.note(result)?;
        let after = self.observe(&record).await?;
        // A graceful stop that ended in SIGKILL didn't get to save.
        let forced = !kill && after.as_ref().is_some_and(|i| i.exit_code == 137 && !i.oom_killed);
        Ok(PowerResponse { changed: true, forced, workload: self.view(id)? })
    }
}
