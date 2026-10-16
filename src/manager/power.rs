// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Changes whether a workload runs when the control plane asks: start, suspend, stop, kill. A
//! suspended workload is frozen with its memory kept, and a start resumes it; one whose resume
//! doesn't come back in time is killed and booted afresh on its data. A spec deferred while it
//! slept is put in place at its next boot. Restarts blocklyd makes on its own are in
//! `restarts.rs`; the stop a fence makes is in `fence.rs`.

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

    async fn start_locked(self: &Arc<Self>, id: &WorkloadId, epoch: Option<u64>) -> Result<PowerResponse, NodeError> {
        let record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Exact)?;
        match record.phase {
            Phase::Creating => {
                return Err(NodeError::Conflict {
                    code: ConflictCode::NotCreated,
                    message: "the workload's creation didn't finish; PUT its spec again".into(),
                });
            }
            Phase::Retained => {
                return Err(NodeError::Conflict {
                    code: ConflictCode::NoCompute,
                    message: "the workload was decommissioned; PUT its spec to bring it back".into(),
                });
            }
            Phase::Active => {}
        }
        let Some(info) = self.observe(&record).await? else {
            self.set_issue(
                id,
                Issue::new(IssueCode::ContainerMissing, "the runtime no longer has this workload's container"),
            );
            return Err(NodeError::Conflict {
                code: ConflictCode::ContainerMissing,
                message: "the container is gone; PUT the spec to make it again".into(),
            });
        };
        if info.status == ContainerStatus::Paused && self.suspension(id).suspended() {
            return self.resume_suspended(record).await;
        }
        if matches!(info.status, ContainerStatus::Running | ContainerStatus::Restarting) {
            return Ok(PowerResponse { changed: false, forced: false, resumed: None, workload: self.view(id)? });
        }
        self.boot(record, info).await?;
        Ok(PowerResponse { changed: true, forced: false, resumed: None, workload: self.view(id)? })
    }

    /// Starts a workload that isn't running, as a new run with all its retries, with the spec a
    /// `PUT` deferred put in place first.
    async fn boot(self: &Arc<Self>, mut record: WorkloadRecord, info: ContainerInfo) -> Result<(), NodeError> {
        let id = record.id.clone();
        let deferred = self.boot_spec(&record, &info);
        let memory = deferred.as_ref().map_or(record.spec.resources.memory_mb, |spec| spec.resources.memory_mb);
        self.admit(&id, u64::from(memory))?;
        match deferred {
            Some(spec) => {
                let digest = spec.record().digest();
                // A new generation for the deferred spec; it no longer sleeps.
                self.replace(record, Some(info), spec, digest, None).await?;
                record = self.record(&id)?;
            }
            None => self.end_suspension(&id, true).await?,
        }
        self.state.lock().unwrap().restart_due.remove(&id);
        // A requested start begins a new run, with all its retries.
        record.restart_count = 0;
        record.stop_requested_at = None;
        record.updated_at = now_str();
        self.save_record(&record).await?;
        self.note(self.runtime.start(&record.container_name).await).map_err(|e| port_clash(&id, e))?;
        self.state.lock().unwrap().last_failure.remove(&id);
        self.clear_issue(&id, IssueCode::InsufficientCapacity);
        self.observe(&record).await?;
        Ok(())
    }

    /// The spec a `PUT` deferred, whole, for its workload's next boot. Memory holds it with its
    /// secret values; after a restart of blocklyd they are taken from the container it replaces,
    /// which runs with them. A deferred spec naming a secret that container lacks can't be made
    /// here and is dropped: the control plane, seeing the old digest, sends it again.
    fn boot_spec(&self, record: &WorkloadRecord, info: &ContainerInfo) -> Option<WorkloadSpec> {
        let suspend = self.suspension(&record.id);
        let pending = suspend.pending?;
        let held = self.state.lock().unwrap().pending.get(&record.id).cloned();
        if let Some(spec) = held.filter(|spec| spec.record().digest() == pending.spec_digest) {
            return Some(spec);
        }
        let spec = pending.spec.with_secrets(&info.env, suspend.resumes_on);
        if spec.is_none() {
            tracing::warn!(
                workload = %record.id,
                deferred = %pending.spec_digest,
                "the deferred spec names a secret this node no longer holds; booting the current spec"
            );
        }
        spec
    }

    /// Ends what the workload kept for its suspension: that it was asked to, and, when it boots
    /// without it (`deferred`), the spec it deferred.
    pub(super) async fn end_suspension(&self, id: &WorkloadId, deferred: bool) -> Result<(), NodeError> {
        let suspend = self.suspension(id);
        if !suspend.suspended() && !(deferred && suspend.pending.is_some()) {
            return Ok(());
        }
        let pending = if deferred { None } else { suspend.pending };
        self.save_suspension(id, SuspendRecord { suspended_at: None, pending, ..suspend }).await
    }

    /// Resumes a suspended workload: thawed, the same process from the same start, if it comes
    /// back within `resume_timeout_seconds`. Otherwise it is killed and booted afresh on its data.
    async fn resume_suspended(self: &Arc<Self>, mut record: WorkloadRecord) -> Result<PowerResponse, NodeError> {
        let id = record.id.clone();
        let timeout = self.config.workloads.resume_timeout();
        let thawed = match tokio::time::timeout(timeout, self.runtime.unpause(&record.container_name)).await {
            Ok(result) => match self.note(result) {
                Ok(()) => true,
                Err(e) if e.is_unavailable() => return Err(e.into()),
                Err(e) => {
                    tracing::warn!(workload = %id, error = %e, "the resume failed; starting the workload afresh");
                    false
                }
            },
            Err(_) => {
                tracing::warn!(
                    workload = %id,
                    seconds = timeout.as_secs(),
                    "the resume didn't come back in time; starting the workload afresh"
                );
                false
            }
        };
        if thawed {
            self.end_suspension(&id, false).await?;
            self.observe(&record).await?;
            return Ok(PowerResponse { changed: true, forced: false, resumed: Some(true), workload: self.view(&id)? });
        }
        // Asked for, so the kill reads as a stop, never a crash to restart.
        record.stop_requested_at = Some(now_str());
        record.updated_at = now_str();
        self.save_record(&record).await?;
        self.note(self.runtime.kill(&record.container_name).await)?;
        let Some(info) = self.observe(&record).await? else {
            return Err(NodeError::Conflict {
                code: ConflictCode::ContainerMissing,
                message: "the container is gone; PUT the spec to make it again".into(),
            });
        };
        self.boot(record, info).await?;
        Ok(PowerResponse { changed: true, forced: false, resumed: Some(false), workload: self.view(&id)? })
    }

    /// Freezes a running workload with its memory kept, until a start resumes it. It keeps
    /// holding that memory on the host, as any frozen container does.
    pub async fn suspend(self: &Arc<Self>, id: WorkloadId, epoch: Option<u64>) -> Result<PowerResponse, NodeError> {
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.suspend_locked(&id, epoch).await;
        self.metrics.operation("suspend", &result, started.elapsed());
        result
    }

    async fn suspend_locked(&self, id: &WorkloadId, epoch: Option<u64>) -> Result<PowerResponse, NodeError> {
        let record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Exact)?;
        let not_running = || NodeError::Conflict {
            code: ConflictCode::NotRunning,
            message: "only a running workload can be suspended".into(),
        };
        let info = if record.phase == Phase::Active { self.observe(&record).await? } else { None };
        let suspend = self.suspension(id);
        let status = info.map(|i| i.status);
        if status == Some(ContainerStatus::Paused) && suspend.suspended() {
            return Ok(PowerResponse { changed: false, forced: false, resumed: None, workload: self.view(id)? });
        }
        if !matches!(status, Some(ContainerStatus::Running | ContainerStatus::Paused)) {
            return Err(not_running());
        }
        // Recorded before the freeze: a frozen container blocklyd made is never taken for a running
        // one, even if blocklyd dies right after.
        self.save_suspension(id, SuspendRecord { suspended_at: Some(now_str()), ..suspend.clone() }).await?;
        if status == Some(ContainerStatus::Running)
            && let Err(e) = self.note(self.runtime.pause(&record.container_name).await)
        {
            if let Err(unrecorded) = self.save_suspension(id, suspend).await {
                tracing::warn!(workload = %id, error = %unrecorded, "couldn't record that the suspend failed");
            }
            return Err(match e {
                RuntimeError::Conflict(_) | RuntimeError::NotFound(_) => not_running(),
                e => e.into(),
            });
        }
        self.observe(&record).await?;
        Ok(PowerResponse { changed: true, forced: false, resumed: None, workload: self.view(id)? })
    }

    pub async fn stop(
        self: &Arc<Self>,
        id: WorkloadId,
        grace: Option<u32>,
        epoch: Option<u64>,
    ) -> Result<PowerResponse, NodeError> {
        if let Some(g) = grace
            && !(1..=MAX_STOP_TIMEOUT).contains(&g)
        {
            return Err(NodeError::Invalid(vec![FieldError {
                field: "timeoutSeconds".into(),
                problem: format!("must be between 1 and {MAX_STOP_TIMEOUT}"),
            }]));
        }
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.stop_locked(&id, grace, false, epoch).await;
        self.metrics.operation("stop", &result, started.elapsed());
        result
    }

    pub(crate) async fn kill(self: &Arc<Self>, id: WorkloadId, epoch: Option<u64>) -> Result<PowerResponse, NodeError> {
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
            return Ok(PowerResponse { changed: false, forced: false, resumed: None, workload: self.view(id)? });
        }
        let Some(info) = self.observe(&record).await? else {
            return Ok(PowerResponse { changed: false, forced: false, resumed: None, workload: self.view(id)? });
        };
        if !matches!(info.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused) {
            return Ok(PowerResponse { changed: false, forced: false, resumed: None, workload: self.view(id)? });
        }
        self.state.lock().unwrap().restart_due.remove(id);
        // Persisted before the signal, so the exit reads as a stop even if blocklyd dies now.
        record.stop_requested_at = Some(now_str());
        record.updated_at = now_str();
        self.save_record(&record).await?;
        let result = if kill {
            self.runtime.kill(&record.container_name).await
        } else {
            let seconds = grace.unwrap_or(record.spec.stop.timeout_seconds);
            let _stopping = self.mark_stopping(id);
            self.runtime
                .stop(&record.container_name, record.spec.stop.signal.as_str(), Duration::from_secs(u64::from(seconds)))
                .await
        };
        self.note(result)?;
        self.end_suspension(id, false).await?;
        let after = self.observe(&record).await?;
        // A graceful stop that ended in SIGKILL didn't get to save.
        let forced = !kill && after.as_ref().is_some_and(|i| i.exit_code == exit_by(libc::SIGKILL) && !i.oom_killed);
        Ok(PowerResponse { changed: true, forced, resumed: None, workload: self.view(id)? })
    }
}
