//! Makes a workload match the spec it was sent (`PUT`): creates it, or replaces it with a new
//! generation. Starting and stopping on request are in `power.rs`; whether the host has room is
//! asked of `room.rs`.

use super::*;

impl Manager {
    // ─── ensure (PUT) ──────────────────────────────────────────────────────────────────────

    pub async fn ensure(
        self: &Arc<Self>,
        id: WorkloadId,
        spec: WorkloadSpec,
        precondition: Precondition,
        epoch: Option<u64>,
    ) -> Result<EnsureResponse, NodeError> {
        spec.validate(&self.spec_policy()).map_err(NodeError::Invalid)?;
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.ensure_locked(&id, spec, precondition, epoch).await;
        self.metrics.operation("ensure", &result, started.elapsed());
        result
    }

    async fn ensure_locked(
        self: &Arc<Self>,
        id: &WorkloadId,
        spec: WorkloadSpec,
        precondition: Precondition,
        epoch: Option<u64>,
    ) -> Result<EnsureResponse, NodeError> {
        let existing = self.state.lock().unwrap().records.get(id).cloned();
        if let Some(record) = &existing {
            check_epoch(record, epoch, EpochRule::Place)?;
        }
        let digest = spec.record().digest();
        match (&existing, &precondition) {
            (Some(r), Precondition::IfNoneMatch) => {
                return Err(NodeError::PreconditionFailed { current: Some(r.spec_digest.clone()) });
            }
            (None, Precondition::IfMatch(_)) => return Err(NodeError::PreconditionFailed { current: None }),
            (Some(r), Precondition::IfMatch(want)) if want != "*" && *want != r.spec_digest => {
                return Err(NodeError::PreconditionFailed { current: Some(r.spec_digest.clone()) });
            }
            _ => {}
        }
        match existing {
            None => self.create_fresh(id, spec, digest, epoch).await,
            Some(record) => {
                let info =
                    if record.phase == Phase::Retained { None } else { self.inspect(&record.container_name).await? };
                // A new epoch is a new placement: the container is made again, so its labels
                // (which Docker can't change) carry the epoch it now runs for.
                let placed_anew = epoch.is_some() && epoch != record.epoch;
                let same = !placed_anew
                    && record.phase == Phase::Active
                    && record.spec_digest == digest
                    && info
                        .as_ref()
                        .is_some_and(|i| i.labels.get(LABEL_DIGEST) == Some(&digest) && secrets_match(i, &spec));
                if same {
                    self.remember(id, info);
                    return Ok(EnsureResponse {
                        outcome: EnsureOutcome::Unchanged,
                        restarted: false,
                        workload: self.view(id)?,
                    });
                }
                self.replace(record, info, spec, digest, epoch).await
            }
        }
    }

    /// Ports for a spec. Those named in `keep` (a previous generation) keep their host port, so
    /// an apply never moves a workload's address.
    fn allocate_ports(
        &self,
        id: &WorkloadId,
        spec: &WorkloadSpec,
        keep: &[AllocatedPort],
    ) -> Result<Vec<AllocatedPort>, NodeError> {
        let mut state = self.state.lock().unwrap();
        let mut out = Vec::new();
        let mut fresh = Vec::new();
        for p in &spec.ports {
            let reused = keep.iter().find(|k| k.name == p.name && k.protocol == p.protocol).map(|k| k.host_port);
            let host_port = match reused {
                Some(port) => port,
                None => match state.ports.allocate(id, p.protocol) {
                    Ok(port) => {
                        fresh.push((p.protocol, port));
                        port
                    }
                    Err(e) => {
                        // Handed out under this same lock and never published: back as they were,
                        // without the quarantine meant for ports a route may still know.
                        for (proto, port) in fresh {
                            state.ports.unreserve(id, proto, port);
                        }
                        return Err(NodeError::NoFreePorts(e.to_string()));
                    }
                },
            };
            out.push(AllocatedPort {
                name: p.name.clone(),
                protocol: p.protocol,
                container_port: p.container_port,
                host_port,
            });
        }
        // Ports the old generation had and the new one doesn't name go back.
        for k in keep {
            if !out.iter().any(|o| o.protocol == k.protocol && o.host_port == k.host_port) {
                state.ports.release(id, k.protocol, k.host_port);
            }
        }
        Ok(out)
    }

    fn container_spec(&self, record: &WorkloadRecord, spec: &WorkloadSpec) -> ContainerSpec {
        let r = &spec.resources;
        let mut labels = spec.labels.clone();
        labels.insert(LABEL_MANAGED.into(), "true".into());
        labels.insert(LABEL_DEPLOYMENT.into(), self.config.deployment_id.clone());
        labels.insert(LABEL_NODE.into(), self.config.node_id.clone());
        labels.insert(LABEL_WORKLOAD.into(), record.id.to_string());
        labels.insert(LABEL_GENERATION.into(), record.generation.to_string());
        labels.insert(LABEL_DIGEST.into(), record.spec_digest.clone());
        if let Some(epoch) = record.epoch {
            labels.insert(LABEL_EPOCH.into(), epoch.to_string());
        }
        labels.insert(LABEL_RECORD.into(), serde_json::to_string(&LabelRecord::of(record)).expect("serializes"));
        let mut env: Vec<(String, String)> = spec.env.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        env.extend(spec.secrets.iter().map(|(k, v)| (k.clone(), v.expose().to_owned())));
        ContainerSpec {
            name: record.container_name.clone(),
            image: spec.image.clone(),
            entrypoint: spec.entrypoint.clone(),
            env,
            labels,
            user: self.config.workloads.user.clone(),
            memory_bytes: r.memory_mb as i64 * 1024 * 1024,
            nano_cpus: r.cpu_millis.map(|m| m as i64 * 1_000_000),
            // Weight follows size unless asked otherwise: a 4 GB server gets Docker's default
            // 1024, a 3 GB one 768, an 8 GB one 2048. Contention is shared as the price is.
            cpu_shares: r.cpu_weight.map_or_else(|| (r.memory_mb as i64 * 1024 / 4096).clamp(2, 262_144), i64::from),
            pids_limit: r.pids_limit.unwrap_or(self.config.workloads.default_pids_limit) as i64,
            read_only_rootfs: self.config.workloads.read_only_rootfs,
            tmp_size_mb: self.config.workloads.tmp_size_mb,
            data_dir: self.store.data_dir(&record.id),
            mount_path: spec.storage.mount_path.clone(),
            ports: record
                .ports
                .iter()
                .map(|p| {
                    let audience =
                        spec.ports.iter().find(|s| s.name == p.name).map(|s| s.audience.clone()).unwrap_or_default();
                    PortBinding {
                        container_port: p.container_port,
                        protocol: p.protocol,
                        host_ips: host_ips(&self.config, &audience),
                        host_port: p.host_port,
                    }
                })
                .collect(),
            stop_signal: spec.stop.signal.as_str().into(),
            stop_timeout_secs: spec.stop.timeout_seconds,
            network: self.config.docker.network.clone(),
            log_max_size_mb: self.config.workloads.log_max_size_mb,
            log_max_files: self.config.workloads.log_max_files,
            oom_score_adj: self.config.workloads.oom_score_adj,
        }
    }

    /// Makes the container. A container already under this name is ours by construction (the
    /// name holds the deployment and workload id): one left by an interrupted attempt or an older
    /// generation, removed first.
    async fn make_container(&self, record: &WorkloadRecord, spec: &WorkloadSpec) -> Result<String, NodeError> {
        let pull = Duration::from_secs(self.config.docker.pull_timeout_seconds);
        self.note(self.runtime.ensure_image(&spec.image, pull).await)?;
        let container = self.container_spec(record, spec);
        match self.note(self.runtime.create(&container).await) {
            Ok(id) => Ok(id),
            Err(RuntimeError::Conflict(_)) => {
                let existing = self.inspect(&record.container_name).await?;
                if existing.as_ref().and_then(|i| i.labels.get(LABEL_WORKLOAD)) != Some(&record.id.to_string()) {
                    return Err(NodeError::Conflict {
                        code: "name_taken",
                        message: format!(
                            "a container named {} exists and isn't this workload's",
                            record.container_name
                        ),
                    });
                }
                self.note(self.runtime.remove(&record.container_name).await)?;
                Ok(self.note(self.runtime.create(&container).await)?)
            }
            Err(e) => Err(e.into()),
        }
    }

    async fn create_fresh(
        self: &Arc<Self>,
        id: &WorkloadId,
        spec: WorkloadSpec,
        digest: String,
        epoch: Option<u64>,
    ) -> Result<EnsureResponse, NodeError> {
        self.check_disk()?;
        let ports = self.allocate_ports(id, &spec, &[])?;
        let at = now_str();
        let mut record = WorkloadRecord {
            version: RECORD_VERSION,
            id: id.clone(),
            generation: 1,
            phase: Phase::Creating,
            spec: spec.record(),
            spec_digest: digest,
            ports,
            container_name: self.container_name(id),
            container_id: None,
            epoch,
            superseded_by: None,
            stop_requested_at: None,
            running_boot: None,
            restart_count: 0,
            created_at: at.clone(),
            updated_at: at,
        };
        let undo = |this: &Self| {
            this.state.lock().unwrap().ports.release_all(id);
            this.state.lock().unwrap().records.remove(id);
            this.store.abandon(id);
        };
        // Persisted before the container exists: a crash from here on leaves a record, which the
        // next PUT or reconciliation finishes or reports. Never an unfindable container.
        if let Err(e) = self.store.ensure_data_dir(id, self.config.data_owner_ids()).map_err(NodeError::from) {
            undo(self);
            return Err(e);
        }
        if let Err(e) = self.save_record(&record) {
            undo(self);
            return Err(e);
        }
        match self.make_container(&record, &spec).await {
            Ok(container_id) => {
                record.container_id = Some(container_id);
                record.phase = Phase::Active;
                record.updated_at = now_str();
                self.save_record(&record)?;
                self.observe(&record).await?;
                Ok(EnsureResponse { outcome: EnsureOutcome::Created, restarted: false, workload: self.view(id)? })
            }
            Err(e) => {
                // Nothing ran: give everything back, including a data directory made just now
                // (only if still empty, so nothing can be lost).
                let _ = self.runtime.remove(&record.container_name).await;
                undo(self);
                Err(e)
            }
        }
    }

    /// A new generation: same data, same ports where the spec still names them, a new container.
    async fn replace(
        self: &Arc<Self>,
        mut record: WorkloadRecord,
        info: Option<ContainerInfo>,
        spec: WorkloadSpec,
        digest: String,
        epoch: Option<u64>,
    ) -> Result<EnsureResponse, NodeError> {
        let id = record.id.clone();
        let was_running =
            info.as_ref().is_some_and(|i| matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting));
        if record.phase == Phase::Retained {
            self.check_disk()?;
        }
        if was_running {
            self.admit(&id, spec.resources.memory_mb as u64)?;
        }
        // Pull first: if the new image can't be had, the running workload is left untouched.
        let pull = Duration::from_secs(self.config.docker.pull_timeout_seconds);
        self.note(self.runtime.ensure_image(&spec.image, pull).await)?;
        let old_ports = record.ports.clone();
        let ports = self.allocate_ports(&id, &spec, &old_ports)?;
        self.persist_resting_ports();
        if info.is_some() {
            if was_running {
                let grace = Duration::from_secs(record.spec.stop.timeout_seconds as u64);
                let stopping = self.mark_stopping(&id);
                let stopped = self.runtime.stop(&record.container_name, record.spec.stop.signal.as_str(), grace).await;
                drop(stopping);
                self.note(stopped)?;
            }
            self.note(self.runtime.remove(&record.container_name).await)?;
        }
        self.store.ensure_data_dir(&id, self.config.data_owner_ids())?;
        record.generation += 1;
        if epoch.is_some() && epoch != record.epoch {
            // Placed here anew: whatever superseded the old copy is older than this placement.
            record.epoch = epoch;
            record.superseded_by = None;
        }
        record.spec = spec.record();
        record.spec_digest = digest;
        record.ports = ports;
        record.phase = Phase::Active;
        record.container_id = None;
        record.stop_requested_at = if was_running { None } else { record.stop_requested_at.take() };
        // A new generation begins a new run: the old one's failures don't use up its retries.
        record.restart_count = 0;
        record.updated_at = now_str();
        self.save_record(&record)?;
        let container_id = self.make_container(&record, &spec).await?;
        record.container_id = Some(container_id);
        self.save_record(&record)?;
        if was_running {
            self.note(self.runtime.start(&record.container_name).await).map_err(|e| port_clash(&id, e))?;
        }
        self.clear_resolved_issues(&id);
        self.observe(&record).await?;
        Ok(EnsureResponse { outcome: EnsureOutcome::Replaced, restarted: was_running, workload: self.view(&id)? })
    }
}

/// Whether the container runs with exactly these secret values. blocklyd keeps no copy of
/// secrets, so the container's own environment is what a new spec is compared against.
fn secrets_match(info: &ContainerInfo, spec: &WorkloadSpec) -> bool {
    spec.secrets.iter().all(|(k, v)| info.env.get(k).map(String::as_str) == Some(v.expose()))
}
