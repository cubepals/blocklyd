//! What the node reports about itself as a whole: health, capacity, status. A heartbeat's body is
//! put together from these in `crate::fleet::heartbeat`.
//! Each workload's own view is `view.rs`; the lease is `fleet.rs`; admission is `room.rs`.

use super::*;

impl Manager {
    /// Asks the runtime now rather than trusting the last call's outcome.
    pub async fn health(&self) -> NodeHealth {
        let _ = self.note(self.runtime.info().await);
        let docker = self.docker_up();
        let reconciled = self.reconciled();
        NodeHealth {
            status: if docker && reconciled { "ok".into() } else { "degraded".into() },
            docker,
            reconciled,
            daemon_version: env!("CARGO_PKG_VERSION").into(),
            protocol: ProtocolVersions::ours(),
            features: crate::protocol::features(),
            node_id: self.config.node_id.clone(),
            deployment_id: self.config.deployment_id.clone(),
        }
    }

    pub fn ports_usage(&self) -> (u32, u32) {
        let state = self.state.lock().unwrap();
        (state.ports.allocated(), state.ports.capacity())
    }

    pub fn issue_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.issues.values().map(Vec::len).sum::<usize>() + state.host_issues.len()
    }

    /// Per-workload numbers for the metrics endpoint.
    pub fn workload_samples(&self) -> Vec<WorkloadSample> {
        let state = self.state.lock().unwrap();
        state
            .records
            .keys()
            .map(|id| {
                let s = state.stats.get(id);
                WorkloadSample {
                    id: id.to_string(),
                    memory_bytes: s.and_then(StatsSample::working_set),
                    cores: s.and_then(|s| s.cores),
                    disk_bytes: state.disk.get(id).map(|(b, _)| *b),
                }
            })
            .collect()
    }

    /// What running workloads use, from their latest samples. A workload that doesn't run uses
    /// nothing, whatever its last sample said, and one that runs but hasn't been sampled yet is
    /// left out: a total is unknown only while workloads run and none of them has a sample.
    fn running_usage(&self) -> (Option<u64>, Option<f64>) {
        let state = self.state.lock().unwrap();
        let (mut running, mut memory, mut cores) = (false, None::<u64>, None::<f64>);
        for id in state.records.keys() {
            let runs = state
                .observed
                .get(id)
                .and_then(|o| o.info.as_ref())
                .is_some_and(|i| i.status == ContainerStatus::Running);
            if !runs {
                continue;
            }
            running = true;
            let Some(sample) = state.stats.get(id) else { continue };
            if let Some(bytes) = sample.working_set() {
                *memory.get_or_insert(0) += bytes;
            }
            if let Some(c) = sample.cores {
                *cores.get_or_insert(0.0) += c;
            }
        }
        if running { (memory, cores) } else { (Some(0), Some(0.0)) }
    }

    /// Capacity as this node sees it: physical, reserved, what its workloads were given, and
    /// what they use.
    pub fn capacity(&self) -> crate::protocol::wire::NodeCapacity {
        let facts = host::facts();
        let (disk_total, disk_available) = host::disk(self.store.root()).unzip();
        let (ports_allocated, ports_total) = self.ports_usage();
        let (used_memory, used_cores) = self.running_usage();
        crate::protocol::wire::NodeCapacity {
            memory_total_mb: self.memory_total_mb,
            reserved_memory_mb: self.config.capacity.reserved_memory_mb,
            allocatable_memory_mb: self.allocatable_memory_mb(),
            provisioned_memory_mb: self.provisioned_memory_mb(),
            running_memory_mb: self.running_memory_mb(None),
            used_memory_bytes: used_memory,
            cpus: self.cpus,
            reserved_cpu_millis: self.config.capacity.reserved_cpu_millis,
            used_cpu_cores: used_cores,
            load_average: facts.load_average,
            disk_total_bytes: disk_total,
            disk_available_bytes: disk_available,
            min_free_disk_mb: self.config.capacity.min_free_disk_mb,
            snapshot_bytes: self.snapshot_bytes(),
            reflink: Some(self.reflink),
            ports_total,
            ports_allocated,
        }
    }

    /// What local snapshots hold on disk, as last measured.
    pub fn snapshot_bytes(&self) -> Option<u64> {
        self.state.lock().unwrap().snapshot_bytes
    }

    /// Where the control plane and the edge reach this node, as its configuration says now: the
    /// control plane follows a change, so the node's identity doesn't hang on an address.
    pub fn addresses(&self) -> Option<crate::protocol::wire::NodeAddresses> {
        let fleet = self.config.fleet.as_ref()?;
        Some(crate::protocol::wire::NodeAddresses {
            api: fleet.api_address.unwrap_or(self.config.api.listen).to_string(),
            edge: self.config.network.edge_ips.first()?.to_string(),
            control: self.config.network.control_ips.first()?.to_string(),
        })
    }

    /// What is wrong with the host as a whole, as the last full pass found it.
    pub fn host_issues(&self) -> Vec<Issue> {
        self.state.lock().unwrap().host_issues.clone()
    }

    pub fn last_reconcile(&self) -> Option<ReconcileView> {
        self.state.lock().unwrap().last_reconcile.clone()
    }

    pub async fn node_status(self: &Arc<Self>) -> NodeStatus {
        if let Ok(info) = self.note(self.runtime.info().await) {
            *self.runtime_info.lock().unwrap() = Some(info);
        }
        let facts = host::facts();
        let (disk_total, disk_available) = host::disk(self.store.root()).unzip();
        let process = host::process();
        let trash = {
            let dir = self.store.trash_dir();
            tokio::task::spawn_blocking(move || crate::store::disk_usage(&dir).ok()).await.ok().flatten()
        };
        let (allocated, capacity) = self.ports_usage();
        let host_issues = self.state.lock().unwrap().host_issues.clone();
        let info = self.runtime_info.lock().unwrap().clone().unwrap_or_default();
        NodeStatus {
            node_id: self.config.node_id.clone(),
            deployment_id: self.config.deployment_id.clone(),
            fleet: self.fleet_view(),
            daemon: DaemonView {
                version: env!("CARGO_PKG_VERSION").into(),
                started_at: format_time(self.started_at),
                uptime_seconds: self.uptime().as_secs(),
                rss_bytes: process.rss_bytes,
                cpu_seconds: process.cpu_seconds,
            },
            hostname: facts.hostname,
            kernel: facts.kernel,
            cpus: facts.cpus,
            load_average: facts.load_average,
            memory_total_bytes: facts.memory_total_bytes,
            memory_available_bytes: facts.memory_available_bytes,
            disk_path: self.store.root().to_string_lossy().into_owned(),
            disk_total_bytes: disk_total,
            disk_available_bytes: disk_available,
            trash_bytes: trash,
            capacity: CapacityView {
                allocatable_memory_mb: self.allocatable_memory_mb(),
                running_memory_mb: self.running_memory_mb(None),
                provisioned_memory_mb: self.provisioned_memory_mb(),
                ports_total: capacity,
                ports_allocated: allocated,
            },
            workloads_by_state: self
                .workloads_by_state()
                .into_iter()
                .map(|(s, n)| (s.as_str().to_owned(), n))
                .collect(),
            docker: DockerView {
                reachable: self.docker_up(),
                version: info.version,
                api_version: info.api_version,
                cgroup_version: info.cgroup_version,
                cgroup_driver: info.cgroup_driver,
                storage_driver: info.storage_driver,
                security_options: info.security_options,
                live_restore: info.live_restore,
            },
            reconcile: self.last_reconcile(),
            issues: host_issues,
        }
    }
}

/// One workload's latest usage, for the metrics endpoint.
pub struct WorkloadSample {
    pub id: String,
    pub memory_bytes: Option<u64>,
    pub cores: Option<f64>,
    pub disk_bytes: Option<u64>,
}

#[cfg(test)]
mod tests {
    use crate::config::CapacityConfig;

    #[test]
    fn workloads_may_be_given_what_the_reserve_leaves_and_no_more_than_the_ceiling() {
        let capacity = |reserved_memory_mb, allocatable_memory_mb| CapacityConfig {
            reserved_memory_mb,
            allocatable_memory_mb,
            ..CapacityConfig::default()
        };
        // No ceiling: all but the reserve.
        assert_eq!(capacity(2048, None).allocatable_mb(16_384), 14_336);
        // A ceiling below that holds; one above it doesn't raise it.
        assert_eq!(capacity(2048, Some(4096)).allocatable_mb(16_384), 4096);
        assert_eq!(capacity(2048, Some(32_768)).allocatable_mb(16_384), 14_336);
        // A reserve above what the host has (or a host whose memory is unknown, 0) leaves nothing.
        assert_eq!(capacity(2048, None).allocatable_mb(1024), 0);
        assert_eq!(capacity(0, Some(4096)).allocatable_mb(0), 0);
    }
}
