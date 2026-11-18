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
        self.let_go_of_quotas(&ids).await;
        for id in ids {
            let Some((size_gb, container)) =
                self.state.lock().unwrap().records.get(&id).map(|r| (r.spec.storage.size_gb, r.container_name.clone()))
            else {
                continue;
            };
            let held = if self.confined(&container).await { self.hold_to_size(&id, size_gb).await } else { None };
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
        let taken = taken_projects(&self.store.workloads_dir(), &self.store.workload_dir(id))?;
        let project = crate::quota::first_free(id, &taken);
        let pending = format!("{} {project}", crate::quota::PENDING);
        crate::durable::write_atomic(done, pending.as_bytes(), crate::quota::RECORD_MODE)
            .map_err(std::io::Error::other)?;
        Ok(project)
    }

    /// Sets `project`'s limit where it differs from what `quota::limit_for` says it should be.
    fn hold(&self, quotas: &crate::quota::Quotas, project: u32, size_gb: u32) -> std::io::Result<()> {
        let (used, current) = (quotas.used(project)?, quotas.limit_of(project)?);
        let (files, files_current) = (quotas.files(project)?, quotas.files_limit_of(project)?);
        let bytes = crate::quota::limit_for(size_gb, used, current).unwrap_or(0);
        let files_limit = crate::quota::files_for(size_gb, files, files_current).unwrap_or(0);
        if bytes == current && files_limit == files_current {
            return Ok(());
        }
        quotas.limit(project, bytes, files_limit)
    }

    /// Puts the workload's data in its project and holds that to `size_gb`, where this host keeps
    /// project quotas: its project, or None when it isn't held (no quotas, or they couldn't be set,
    /// which is logged and never refuses anything; the walk measures it then).
    pub(crate) async fn hold_to_size(&self, id: &WorkloadId, size_gb: u32) -> Option<u32> {
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
        // A tree not yet in its project moves in with no limit, so data larger than the limit
        // the project had (a restore) isn't refused on the way; the limit follows below.
        let (to_check, done_check) = (dir.clone(), done.clone());
        let walks = tokio::task::spawn_blocking(move || crate::quota::assigned(&to_check, project, &done_check))
            .await
            .map_or(true, |a| !a.unwrap_or(false));
        if walks && let Err(e) = quotas.limit(project, 0, 0) {
            tracing::warn!(workload = %id, project, error = %e, "couldn't lift the project's limit before its walk");
        }
        let assigned = tokio::task::spawn_blocking(move || crate::quota::assign(&dir, project, &done))
            .await
            .unwrap_or_else(|e| Err(std::io::Error::other(e)));
        // Held either way: a walk that couldn't finish leaves the project with a limit, not the
        // none it moved in with, over what did come in.
        let held = self.hold(quotas, project, size_gb);
        match (assigned, held) {
            (Ok(()), Ok(())) => Some(project),
            (Err(e), _) | (_, Err(e)) => {
                tracing::warn!(workload = %id, project, error = %e, "couldn't hold the workload's data to its size");
                None
            }
        }
    }

    /// Holds data for a container about to be made: what the trash keeps leaves its project first,
    /// so a workload made again under the same id isn't sized with the copy it deleted.
    pub(crate) async fn hold_for_new(&self, id: &WorkloadId, size_gb: u32) {
        if self.quotas.is_some() {
            self.release_trash().await;
            self.hold_to_size(id, size_gb).await;
        }
    }

    /// Holds a workload about to start in the container it has, where that container can't take
    /// its data out of the project: data a restore swapped in since the last pass is held to its
    /// size, and not counted with the copy it replaced.
    pub(crate) async fn hold_for_start(&self, record: &WorkloadRecord) {
        if self.quotas.is_some() && self.confined(&record.container_name).await {
            self.release_trash().await;
            self.hold_to_size(&record.id, record.spec.storage.size_gb).await;
        }
    }

    /// Whether `container` was made under the seccomp profile that keeps a workload's data in its
    /// project. One made before it (an older blocklyd's) isn't held, and is measured by walking,
    /// until a start makes it again (`power.rs`).
    async fn confined(&self, container: &str) -> bool {
        let info = self.runtime.inspect(container).await.ok().flatten();
        info.is_some_and(|i| {
            i.labels.get(crate::labels::LABEL_CONFINED).map(String::as_str)
                == Some(crate::runtime::seccomp::CONFINEMENT)
        })
    }

    /// Where quotas were turned off, takes each workload's data out of the project an earlier run
    /// held it in and lifts that project's limit, so nothing stays held to a size no one enforces.
    async fn let_go_of_quotas(&self, ids: &[WorkloadId]) {
        let Some(lapsed) = self.lapsed_quotas.as_ref() else { return };
        for id in ids {
            let done = self.store.workload_dir(id).join(QUOTA_DONE);
            let Some(project) = std::fs::read_to_string(&done).ok().and_then(|t| crate::quota::recorded(&t)) else {
                continue;
            };
            let dir = self.store.data_dir(id);
            let released = tokio::task::spawn_blocking(move || crate::quota::release(&dir)).await;
            let let_go = match released {
                Ok(Ok(())) => lapsed.limit(project, 0, 0).and_then(|()| std::fs::remove_file(&done)),
                Ok(Err(e)) => Err(e),
                Err(e) => Err(std::io::Error::other(e)),
            };
            if let Err(e) = let_go {
                tracing::warn!(workload = %id, project, error = %e, "couldn't let go of the workload's quota");
            }
        }
    }

    /// Takes what the trash keeps (data a restore replaced, deleted workloads) out of the projects
    /// it was counted in, so a workload's limit holds only what it holds now.
    async fn release_trash(&self) {
        if self.quotas.is_none() {
            return;
        }
        let trash = self.store.trash_dir();
        // An entry is either data a restore replaced or a deleted workload's whole directory, whose
        // own root was never in a project but whose data is.
        let _ = tokio::task::spawn_blocking(move || {
            let Ok(entries) = std::fs::read_dir(&trash) else { return };
            for entry in entries.flatten().filter(|e| e.file_type().is_ok_and(|t| t.is_dir())) {
                let data = entry.path().join(crate::store::DATA_DIR);
                let dirs = std::iter::once(entry.path()).chain(data.is_dir().then_some(data));
                for dir in dirs {
                    if let Err(e) = crate::quota::release(&dir) {
                        tracing::warn!(dir = %dir.display(), error = %e, "couldn't take the trash out of its projects");
                    }
                }
            }
        })
        .await;
    }
}

/// The projects every workload directory under `workloads` but `mine` holds or has reserved. The
/// directories on disk, not the records: a create still on its way reserves one before its record
/// is published.
fn taken_projects(
    workloads: &std::path::Path,
    mine: &std::path::Path,
) -> std::io::Result<std::collections::HashSet<u32>> {
    Ok(std::fs::read_dir(workloads)?
        .flatten()
        .map(|entry| entry.path())
        .filter(|dir| dir != mine)
        .filter_map(|dir| std::fs::read_to_string(dir.join(QUOTA_DONE)).ok())
        .filter_map(|text| crate::quota::recorded(&text))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reservation_still_on_its_way_is_taken() {
        let workloads = tempfile::tempdir().unwrap();
        for (name, record) in [("a", "pending 1048600"), ("b", "1048601"), ("mine", "1048602")] {
            std::fs::create_dir(workloads.path().join(name)).unwrap();
            std::fs::write(workloads.path().join(name).join(QUOTA_DONE), record).unwrap();
        }
        std::fs::create_dir(workloads.path().join("none")).unwrap();
        let taken = taken_projects(workloads.path(), &workloads.path().join("mine")).unwrap();
        assert_eq!(taken, std::collections::HashSet::from([1_048_600, 1_048_601]));
    }
}
