// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Samples what workloads use: CPU, memory, disk. The node-wide totals made from these samples
//! are in `node.rs`; the memory a workload was promised is counted in `room.rs`.

use super::*;

const MIB: u64 = 1 << 20;
/// Beside a workload's data: the project a finished walk put it in (`quota::assign`).
const QUOTA_DONE: &str = "quota-project";
const GIB: u64 = 1 << 30;

impl Manager {
    // ─── observation ───────────────────────────────────────────────────────────────────────

    pub async fn stats(self: &Arc<Self>, id: &WorkloadId) -> Result<StatsView, NodeError> {
        let record = self.record(id)?;
        let info = self.observe(&record).await?;
        let raw = if info.as_ref().is_some_and(|i| i.status == ContainerStatus::Running) {
            self.note(self.runtime.stats(&record.container_name).await)?
        } else {
            None
        };
        let sample = raw.map(|raw| self.store_sample(id, raw));
        let state = self.state_of(&record, info.as_ref());
        let (used, _) = self.state.lock().unwrap().disk.get(id).cloned().unzip();
        let s = sample.as_ref();
        Ok(StatsView {
            at: now_str(),
            state,
            uptime_seconds: info
                .as_ref()
                .filter(|i| i.status == ContainerStatus::Running)
                .and_then(|i| i.started_at)
                .map(|t| (now() - t).whole_seconds().max(0) as u64),
            cpu_cores: s.and_then(|s| s.cores),
            cpu_seconds_total: s.and_then(|s| s.raw.cpu_total_ns).map(|ns| ns as f64 / 1e9),
            cpu_throttled_periods: s.and_then(|s| s.raw.cpu_throttled_periods),
            cpu_limit_millis: record.spec.resources.cpu_millis,
            memory_bytes: s.and_then(|s| s.raw.memory_usage),
            memory_working_set_bytes: s.and_then(StatsSample::working_set),
            memory_limit_bytes: s.and_then(|s| s.raw.memory_limit),
            pids: s.and_then(|s| s.raw.pids),
            pids_limit: s.and_then(|s| s.raw.pids_limit),
            network_rx_bytes: s.and_then(|s| s.raw.rx_bytes),
            network_tx_bytes: s.and_then(|s| s.raw.tx_bytes),
            data_used_bytes: used,
        })
    }

    /// Keeps a sample and works out cores in use since the previous one.
    fn store_sample(&self, id: &WorkloadId, raw: RawStats) -> StatsSample {
        let mut state = self.state.lock().unwrap();
        let at = Instant::now();
        let cores = state.stats.get(id).and_then(|prev| {
            let dt = at.duration_since(prev.at).as_secs_f64();
            let (now_ns, then_ns) = (raw.cpu_total_ns?, prev.raw.cpu_total_ns?);
            (dt >= 0.5 && now_ns >= then_ns).then(|| (now_ns - then_ns) as f64 / 1e9 / dt)
        });
        let sample = StatsSample { at, raw, cores: cores.or(state.stats.get(id).and_then(|p| p.cores)) };
        state.stats.insert(id.clone(), sample.clone());
        sample
    }

    /// Samples every running workload (the metrics' source). Called on an interval.
    pub async fn sample_all(self: &Arc<Self>) {
        let running: Vec<(WorkloadId, String)> = {
            let state = self.state.lock().unwrap();
            state
                .records
                .values()
                .filter(|r| {
                    state
                        .observed
                        .get(&r.id)
                        .and_then(|o| o.info.as_ref())
                        .is_some_and(|i| i.status == ContainerStatus::Running)
                })
                .map(|r| (r.id.clone(), r.container_name.clone()))
                .collect()
        };
        for (id, name) in running {
            if let Ok(Some(raw)) = self.runtime.stats(&name).await {
                self.store_sample(&id, raw);
            }
        }
    }

    /// Measures each workload's data on disk, and the snapshots beside it. Called on an interval;
    /// blocking work off-thread.
    pub async fn measure_disk(self: &Arc<Self>) {
        let ids: Vec<WorkloadId> = self.state.lock().unwrap().records.keys().cloned().collect();
        let dirs: Vec<std::path::PathBuf> = ids.iter().map(|id| self.store.snapshots_dir(id)).collect();
        let snapshots = tokio::task::spawn_blocking(move || {
            dirs.iter().filter(|d| d.exists()).filter_map(|d| crate::tree::disk_usage(d).ok()).sum::<u64>()
        })
        .await;
        if let Ok(bytes) = snapshots {
            self.state.lock().unwrap().snapshot_bytes = Some(bytes);
        }
        self.release_trash().await;
        for id in ids {
            let Some(size_gb) = self.state.lock().unwrap().records.get(&id).map(|r| r.spec.storage.size_gb) else {
                continue;
            };
            let held = self.hold_to_size(&id, size_gb).await;
            let used = match held {
                Some(project) => self.quotas.as_ref().and_then(|q| q.used(project).ok()),
                None => {
                    let dir = self.store.data_dir(&id);
                    tokio::task::spawn_blocking(move || crate::tree::disk_usage(&dir)).await.ok().and_then(Result::ok)
                }
            };
            if let Some(bytes) = used {
                let mut state = self.state.lock().unwrap();
                if state.records.contains_key(&id) {
                    state.disk.insert(id.clone(), (bytes, now()));
                }
                // Held by a quota, data stops at its size: within a mebibyte of it is full.
                let size = u64::from(size_gb) * GIB;
                let over = if held.is_some() { bytes + MIB >= size } else { bytes > size };
                drop(state);
                if over {
                    self.set_issue(
                        &id,
                        Issue::new(IssueCode::OverStorage, "data has reached the size the spec promised"),
                    );
                } else {
                    self.clear_issue(&id, IssueCode::OverStorage);
                }
            }
        }
    }

    /// The project the workload's data is counted in: the one it took, or else the first free one
    /// from what its key offers, reserved in `done` before its data is walked.
    fn project_for(&self, id: &WorkloadId, done: &std::path::Path) -> std::io::Result<u32> {
        let _taking = self.quota_projects.lock().unwrap();
        if let Some(project) = std::fs::read_to_string(done).ok().and_then(|t| crate::quota::recorded(&t)) {
            return Ok(project);
        }
        let others: Vec<WorkloadId> =
            self.state.lock().unwrap().records.keys().filter(|other| *other != id).cloned().collect();
        let taken: std::collections::HashSet<u32> = others
            .iter()
            .filter_map(|other| std::fs::read_to_string(self.store.workload_dir(other).join(QUOTA_DONE)).ok())
            .filter_map(|text| crate::quota::recorded(&text))
            .collect();
        let project = crate::quota::first_free(id, &taken);
        std::fs::write(done, format!("{} {project}", crate::quota::PENDING))?;
        Ok(project)
    }

    /// Sets `project`'s limit where it differs from what `quota::limit_for` says it should be.
    fn hold(&self, quotas: &crate::quota::Quotas, project: u32, size_gb: u32) -> std::io::Result<()> {
        let (used, current) = (quotas.used(project)?, quotas.limit_of(project)?);
        match crate::quota::limit_for(size_gb, used, current) {
            Some(limit) if limit != current => quotas.limit(project, limit),
            _ => Ok(()),
        }
    }

    /// Puts the workload's data in its project and holds that to `size_gb`, where this host keeps
    /// project quotas: its project, or None when it isn't held (no quotas, or they couldn't be set,
    /// which is logged and never refuses anything; the walk measures it then).
    pub(crate) async fn hold_to_size(self: &Arc<Self>, id: &WorkloadId, size_gb: u32) -> Option<u32> {
        let quotas = self.quotas.as_ref()?;
        let done = self.store.workload_dir(id).join(QUOTA_DONE);
        let project = match self.project_for(id, &done) {
            Ok(project) => project,
            Err(e) => {
                tracing::warn!(workload = %id, error = %e, "couldn't take a project for the workload's data");
                return None;
            }
        };
        let dir = self.store.data_dir(id);
        let assigned = tokio::task::spawn_blocking(move || crate::quota::assign(&dir, project, &done)).await;
        let held = match assigned {
            Ok(Ok(())) => self.hold(quotas, project, size_gb),
            Ok(Err(e)) => Err(e),
            Err(e) => Err(std::io::Error::other(e)),
        };
        match held {
            Ok(()) => Some(project),
            Err(e) => {
                tracing::warn!(workload = %id, project, error = %e, "couldn't hold the workload's data to its size");
                None
            }
        }
    }

    /// Takes what the trash keeps (data a restore replaced, deleted workloads) out of the projects
    /// it was counted in, so a workload's limit holds only what it holds now.
    async fn release_trash(self: &Arc<Self>) {
        if self.quotas.is_none() {
            return;
        }
        let trash = self.store.trash_dir();
        let released = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let Ok(entries) = std::fs::read_dir(&trash) else { return Ok(()) };
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|t| t.is_dir()) {
                    crate::quota::release(&entry.path())?;
                }
            }
            Ok(())
        })
        .await;
        if let Ok(Err(e)) = released {
            tracing::warn!(error = %e, "couldn't take the trash out of its projects");
        }
    }
}
