//! Samples what workloads use: CPU, memory, disk. The node-wide totals made from these samples
//! are in `node.rs`; the memory a workload was promised is counted in `room.rs`.

use super::*;

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
    pub(crate) async fn measure_disk(self: &Arc<Self>) {
        let ids: Vec<WorkloadId> = self.state.lock().unwrap().records.keys().cloned().collect();
        let dirs: Vec<std::path::PathBuf> = ids.iter().map(|id| self.store.snapshots_dir(id)).collect();
        let snapshots = tokio::task::spawn_blocking(move || {
            dirs.iter().filter(|d| d.exists()).filter_map(|d| crate::tree::disk_usage(d).ok()).sum::<u64>()
        })
        .await;
        if let Ok(bytes) = snapshots {
            self.state.lock().unwrap().snapshot_bytes = Some(bytes);
        }
        for id in ids {
            let dir = self.store.data_dir(&id);
            let used = tokio::task::spawn_blocking(move || crate::tree::disk_usage(&dir)).await;
            if let Ok(Ok(bytes)) = used {
                let mut state = self.state.lock().unwrap();
                if state.records.contains_key(&id) {
                    state.disk.insert(id.clone(), (bytes, now()));
                }
                let over =
                    state.records.get(&id).is_some_and(|r| bytes > r.spec.storage.size_gb as u64 * 1024 * 1024 * 1024);
                drop(state);
                if over {
                    self.set_issue(
                        &id,
                        Issue::new(IssueCode::OverStorage, "data is larger than the size the spec promised"),
                    );
                } else {
                    self.clear_issue(&id, IssueCode::OverStorage);
                }
            }
        }
    }
}
