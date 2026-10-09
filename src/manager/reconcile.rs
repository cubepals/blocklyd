//! Makes the records agree with what Docker actually has, at startup and on a schedule. It fixes
//! only blocklyd's own bookkeeping, and at startup clears what a crash left of its own files, and
//! reports the rest; resuming what the host went down under is `restarts.rs`, and the schedule
//! itself runs in `crate::reconcile`.

use super::*;

/// Where a reconciliation takes the records from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Records {
    /// Read again from disk, and what a crash left settled: when blocklyd starts.
    FromDisk,
    /// As memory holds them: every pass after the first.
    InMemory,
}

impl Manager {
    // ─── reconciliation ────────────────────────────────────────────────────────────────────

    /// Rebuilds what blocklyd knows from disk (on startup) and the runtime (always), and
    /// reports whatever doesn't add up. It fixes only its own bookkeeping: a record interrupted
    /// mid-create is completed from its container, a container whose record was lost gets its
    /// record back from its labels. It never starts, stops or deletes a workload.
    pub async fn reconcile(self: &Arc<Self>, from: Records) -> ReconcileView {
        let started = Instant::now();
        let result = self.reconcile_inner(from).await;
        let view = match result {
            Ok((workloads, adopted)) => {
                self.reconciled.store(true, Ordering::SeqCst);
                self.schedule_resumes();
                ReconcileView {
                    finished_at: now_str(),
                    duration_ms: started.elapsed().as_millis() as u64,
                    workloads,
                    adopted,
                    issues: self.issue_count() as u64,
                    error: None,
                }
            }
            Err(e) => {
                // Read before building the view: a lock guard in one field would outlive the
                // field and deadlock the next (std's mutex isn't reentrant).
                let workloads = self.state.lock().unwrap().records.len() as u64;
                let issues = self.issue_count() as u64;
                ReconcileView {
                    finished_at: now_str(),
                    duration_ms: started.elapsed().as_millis() as u64,
                    workloads,
                    adopted: 0,
                    issues,
                    error: Some(e.to_string()),
                }
            }
        };
        self.metrics.reconcile(view.error.is_none(), started.elapsed());
        self.state.lock().unwrap().last_reconcile = Some(view.clone());
        view
    }

    async fn reconcile_inner(self: &Arc<Self>, from: Records) -> Result<(u64, u64), NodeError> {
        let mut host_issues = Vec::new();
        let mut issues: HashMap<WorkloadId, Vec<Issue>> = HashMap::new();
        if from == Records::FromDisk {
            let (records, bad) = self.store.load_all()?;
            for b in bad {
                host_issues.push(Issue::new("unreadable_record", format!("{}: {}", b.path.display(), b.problem)));
            }
            // What a crash left is settled before anything uses the data.
            host_issues.extend(self.settle_leftovers(&records, &mut issues).await);
            let resting = self.store.load_resting_ports();
            let now_unix = now().unix_timestamp();
            let mut state = self.state.lock().unwrap();
            state.records = records.into_iter().map(|r| (r.id.clone(), r)).collect();
            state.ports.restore_resting(
                resting
                    .into_iter()
                    .map(|p| (p.protocol, p.port, Duration::from_secs((now_unix - p.released_at_unix).max(0) as u64))),
            );
        }

        let network_labels = BTreeMap::from([
            (LABEL_MANAGED.to_owned(), "true".to_owned()),
            (LABEL_DEPLOYMENT.to_owned(), self.config.deployment_id.clone()),
        ]);
        let network = self.note(self.runtime.ensure_network(&self.config.docker.network, &network_labels).await)?;
        if !network.isolated {
            host_issues.push(Issue::new(
                "network_not_isolated",
                format!("{} lets workloads reach each other (enable_icc is not false)", self.config.docker.network),
            ));
        }
        if let Ok(info) = self.runtime.info().await {
            if info.live_restore != Some(true) {
                host_issues.push(Issue::new(
                    "live_restore_off",
                    "the Docker daemon stops every workload when it restarts (live-restore is off)",
                ));
            }
            *self.runtime_info.lock().unwrap() = Some(info);
        }

        let containers = self.note(self.runtime.list(&self.owner_labels()).await)?;
        let mut by_workload: HashMap<WorkloadId, Vec<ContainerInfo>> = HashMap::new();
        for c in containers {
            match c.labels.get(LABEL_WORKLOAD).map(|w| WorkloadId::parse(w)) {
                Some(Ok(id)) => by_workload.entry(id).or_default().push(c),
                _ => host_issues
                    .push(Issue::new("unlabelled_container", format!("{} carries no valid workload id", c.name))),
            }
        }

        let mut adopted = 0u64;
        let records: Vec<WorkloadRecord> = self.state.lock().unwrap().records.values().cloned().collect();
        let mut seen = BTreeSet::new();
        for mut record in records {
            seen.insert(record.id.clone());
            let found = by_workload.remove(&record.id).unwrap_or_default();
            let (mine, strays): (Vec<_>, Vec<_>) = found.into_iter().partition(|c| c.name == record.container_name);
            for s in strays {
                issues
                    .entry(record.id.clone())
                    .or_default()
                    .push(Issue::new("duplicate_container", format!("{} also claims this workload", s.name)));
            }
            let info = mine.into_iter().next();
            match (record.phase, &info) {
                (Phase::Creating, Some(c)) => {
                    // The container was made; the record just didn't hear back.
                    record.phase = Phase::Active;
                    record.container_id = Some(c.id.clone());
                    self.save_record(&record).await?;
                    adopted += 1;
                }
                (Phase::Creating, None) => issues.entry(record.id.clone()).or_default().push(Issue::new(
                    "create_incomplete",
                    "creation was interrupted before the container existed; PUT the spec again",
                )),
                (Phase::Active, None) => issues.entry(record.id.clone()).or_default().push(Issue::new(
                    "container_missing",
                    "the runtime no longer has this workload's container; PUT the spec to make it again",
                )),
                (Phase::Retained, Some(c)) => {
                    // Compute exists though it was let go: the runtime is the truth.
                    issues.entry(record.id.clone()).or_default().push(Issue::new(
                        "unexpected_container",
                        format!("{} exists for a decommissioned workload", c.name),
                    ));
                }
                _ => {}
            }
            if let Some(c) = &info {
                if c.labels.get(LABEL_DIGEST) != Some(&record.spec_digest) {
                    issues.entry(record.id.clone()).or_default().push(Issue::new(
                        "digest_mismatch",
                        "the container was made from a different spec than the record holds",
                    ));
                }
                if let Some(labelled) =
                    c.labels.get(LABEL_RECORD).and_then(|l| serde_json::from_str::<LabelRecord>(l).ok())
                    && labelled.ports != record.ports
                {
                    issues.entry(record.id.clone()).or_default().push(Issue::new(
                        "port_mismatch",
                        "the container publishes different ports than the record holds",
                    ));
                }
            }
            self.remember(&record.id, info).await;
        }

        // Containers with no record: rebuilt from their labels, so a lost state directory costs
        // nothing but this pass.
        for (id, containers) in by_workload {
            let Some(c) = containers.into_iter().next() else { continue };
            let Some(label) = c.labels.get(LABEL_RECORD).and_then(|l| serde_json::from_str::<LabelRecord>(l).ok())
            else {
                host_issues
                    .push(Issue::new("unrecoverable_container", format!("{} has no readable record label", c.name)));
                continue;
            };
            let record = WorkloadRecord {
                version: RECORD_VERSION,
                id: id.clone(),
                generation: label.generation,
                phase: Phase::Active,
                spec_digest: label.spec_digest,
                spec: label.spec,
                ports: label.ports,
                container_name: c.name.clone(),
                container_id: Some(c.id.clone()),
                // A supersession isn't in the labels (Docker can't relabel a container); the
                // control plane fences the copy again at its next heartbeat.
                epoch: label.epoch,
                superseded_by: None,
                stop_requested_at: None,
                running_boot: None,
                restart_count: 0,
                created_at: label.created_at,
                updated_at: now_str(),
            };
            self.store.ensure_data_dir(&id, self.config.data_owner_ids())?;
            self.save_record(&record).await?;
            issues.entry(id.clone()).or_default().push(Issue::new(
                "record_rebuilt",
                "blocklyd's record was missing and was rebuilt from the container's labels",
            ));
            seen.insert(id.clone());
            self.remember(&id, Some(c)).await;
            adopted += 1;
        }

        // Ports: rebuilt from records, every pass, so the allocator can't drift from them.
        {
            let mut state = self.state.lock().unwrap();
            state.ports.clear();
            let records: Vec<WorkloadRecord> = state.records.values().cloned().collect();
            for r in records.iter().filter(|r| r.phase != Phase::Retained) {
                for p in &r.ports {
                    if let Err(e) = state.ports.claim(&r.id, p.protocol, p.host_port) {
                        let issue = match e {
                            PortError::Conflict { .. } => Issue::new("port_conflict", e.to_string()),
                            _ => Issue::new("port_out_of_range", e.to_string()),
                        };
                        issues.entry(r.id.clone()).or_default().push(issue);
                        if let PortError::Conflict { holder, .. } = &e {
                            issues.entry(holder.clone()).or_default().push(Issue::new("port_conflict", e.to_string()));
                        }
                    }
                }
            }
        }

        for dir in self.store.orphan_dirs(&seen) {
            host_issues.push(Issue::new(
                "orphan_data",
                format!("{} holds data no workload claims; left alone", dir.display()),
            ));
        }

        let workloads = seen.len() as u64;
        let mut state = self.state.lock().unwrap();
        // A pass finds each issue again, but for two it can't see, which are kept: storage use
        // (measured on its own interval) and a restart refused for room. Those of workloads that no
        // longer exist go.
        let known: BTreeSet<_> = state.records.keys().cloned().collect();
        state.issues.retain(|id, _| known.contains(id));
        for (id, list) in state.issues.iter_mut() {
            list.retain(|i| matches!(i.code.as_str(), "over_storage" | "insufficient_capacity"));
            if let Some(new) = issues.remove(id) {
                list.extend(new);
            }
        }
        for (id, list) in issues {
            state.issues.insert(id, list);
        }
        state.host_issues = host_issues;
        Ok((workloads, adopted))
    }

    /// Settles what a crash left on disk: each record's restore that didn't get to finish
    /// (`settle_restore`), then the spool, the snapshots that never got their description and the
    /// temp files of record writes (`Store::clear_leftovers`). Each removal is logged. Only from
    /// disk, at startup: under the state directory's lock and before anything is served, so none of
    /// it can be in use. Returns the host's issues: what it left for a human.
    async fn settle_leftovers(
        &self,
        records: &[WorkloadRecord],
        issues: &mut HashMap<WorkloadId, Vec<Issue>>,
    ) -> Vec<Issue> {
        for r in records {
            match self.settle_restore(&r.id).await {
                Ok(Some(issue)) => issues.entry(r.id.clone()).or_default().push(issue),
                Ok(None) => {}
                Err(e) => tracing::warn!(workload = %r.id, error = %e, "couldn't settle an interrupted restore"),
            }
        }
        let (store, ids) = (self.store.clone(), records.iter().map(|r| r.id.clone()).collect::<Vec<_>>());
        let Ok(leftovers) = blocking(move || store.clear_leftovers(&ids)).await else { return Vec::new() };
        for path in &leftovers.removed {
            tracing::warn!(path = %path.display(), "removed what a crash left, which nothing would read");
        }
        for failed in &leftovers.failed {
            tracing::warn!(path = %failed.path.display(), error = %failed.problem, "couldn't remove what a crash left");
        }
        let unreadable = leftovers.unreadable.into_iter();
        unreadable.map(|b| Issue::new("unreadable_snapshot", format!("{}: {}", b.path.display(), b.problem))).collect()
    }
}
