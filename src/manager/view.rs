//! A workload as the protocol shows it: its state, ports, storage and issues, built from what the
//! core last recorded. It never asks the runtime; looking at a container is `observe`, in
//! `manager.rs`, and how a record and a container make a state is `derive_state`, in
//! `workload_state.rs`.

use super::*;

impl Manager {
    pub(super) fn state_of(&self, record: &WorkloadRecord, info: Option<&ContainerInfo>) -> WorkloadState {
        let stopping = self.state.lock().unwrap().stopping.contains(&record.id);
        pending_restart(
            &self.state.lock().unwrap(),
            &record.id,
            derive_state(record, info, stopping, self.trustworthy()),
        )
    }

    // ─── views ─────────────────────────────────────────────────────────────────────────────

    pub fn view(&self, id: &WorkloadId) -> Result<WorkloadView, NodeError> {
        let state = self.state.lock().unwrap();
        let record = state.records.get(id).ok_or_else(|| NodeError::NotFound(id.clone()))?;
        let info = state.observed.get(id).and_then(|o| o.info.as_ref());
        let stopping = state.stopping.contains(id);
        Ok(self.build_view(&state, record, info, stopping))
    }

    fn build_view(
        &self,
        state: &State,
        record: &WorkloadRecord,
        info: Option<&ContainerInfo>,
        stopping: bool,
    ) -> WorkloadView {
        let workload_state =
            pending_restart(state, &record.id, derive_state(record, info, stopping, self.trustworthy()));
        let exit = info
            .filter(|i| matches!(i.status, ContainerStatus::Exited | ContainerStatus::Dead))
            .map(|i| ExitInfo { code: i.exit_code, oom_killed: i.oom_killed, at: i.finished_at.map(format_time) });
        let restart_count = record.restart_count;
        let last_failure_at = state
            .last_failure
            .get(&record.id)
            .copied()
            .or_else(|| info.filter(|i| i.restart_count > 0).and_then(|i| i.started_at))
            .map(format_time);
        let changed = info
            .and_then(|i| match (i.started_at, i.finished_at) {
                (Some(s), Some(f)) => Some(s.max(f)),
                (s, f) => s.or(f),
            })
            .map(format_time)
            .unwrap_or_else(|| record.updated_at.clone());
        let ports = record
            .ports
            .iter()
            .map(|p| {
                let audience =
                    record.spec.ports.iter().find(|s| s.name == p.name).map(|s| s.audience.clone()).unwrap_or_default();
                let mut endpoints = crate::protocol::Endpoints::default();
                if audience.contains(&Audience::Edge) {
                    endpoints.edge = self.config.network.edge_ips.iter().map(|ip| endpoint(ip, p.host_port)).collect();
                }
                if audience.contains(&Audience::Control) {
                    endpoints.control =
                        self.config.network.control_ips.iter().map(|ip| endpoint(ip, p.host_port)).collect();
                }
                PortView {
                    name: p.name.clone(),
                    protocol: p.protocol,
                    container_port: p.container_port,
                    host_port: p.host_port,
                    endpoints,
                }
            })
            .collect();
        let (used, measured) =
            state.disk.get(&record.id).map(|(b, at)| (Some(*b), Some(format_time(*at)))).unwrap_or_default();
        WorkloadView {
            id: record.id.clone(),
            generation: record.generation,
            epoch: record.epoch,
            superseded_by: record.superseded_by,
            spec_digest: record.spec_digest.clone(),
            image: record.spec.image.clone(),
            state: workload_state,
            health: info.filter(|i| i.status == ContainerStatus::Running).and_then(|i| i.health),
            exit,
            restart_count,
            last_failure_at,
            started_at: info.and_then(|i| i.started_at).map(format_time),
            finished_at: info.and_then(|i| i.finished_at).map(format_time),
            changed_at: changed,
            ports,
            resources: record.spec.resources.clone(),
            storage: StorageView {
                mount_path: record.spec.storage.mount_path.clone(),
                size_gb: record.spec.storage.size_gb,
                used_bytes: used,
                measured_at: measured,
            },
            labels: record.spec.labels.clone(),
            secret_names: record.spec.secret_names.clone(),
            issues: state.issues.get(&record.id).cloned().unwrap_or_default(),
            locate: Locate {
                container_name: record.container_name.clone(),
                container_id: info.map(|i| i.id.clone()).or_else(|| record.container_id.clone()),
                data_dir: self.store.data_dir(&record.id).to_string_lossy().into_owned(),
            },
            created_at: record.created_at.clone(),
            updated_at: record.updated_at.clone(),
        }
    }

    /// Every workload, or those whose state changed at or after `since` (Blockly's
    /// `observeChanged`: level-triggered, answered from one local listing).
    pub fn list(&self, since: Option<OffsetDateTime>) -> Vec<WorkloadView> {
        let state = self.state.lock().unwrap();
        state
            .records
            .values()
            .map(|r| {
                let info = state.observed.get(&r.id).and_then(|o| o.info.as_ref());
                self.build_view(&state, r, info, state.stopping.contains(&r.id))
            })
            .filter(|v| match since {
                None => true,
                Some(since) => {
                    parse_time(Some(&v.changed_at)).is_none_or(|t| t >= since)
                        || parse_time(Some(&v.updated_at)).is_none_or(|t| t >= since)
                }
            })
            .collect()
    }

    pub fn workloads_by_state(&self) -> BTreeMap<WorkloadState, u64> {
        let state = self.state.lock().unwrap();
        let mut counts: BTreeMap<WorkloadState, u64> = WorkloadState::ALL.iter().map(|s| (*s, 0)).collect();
        for r in state.records.values() {
            let info = state.observed.get(&r.id).and_then(|o| o.info.as_ref());
            let derived = derive_state(r, info, state.stopping.contains(&r.id), self.trustworthy());
            *counts.entry(pending_restart(&state, &r.id, derived)).or_default() += 1;
        }
        counts
    }
}

/// A failure blocklyd will restart reads as restarting, not crashed: the host is starting it again,
/// as Docker's own policy would say.
fn pending_restart(state: &State, id: &WorkloadId, derived: WorkloadState) -> WorkloadState {
    let waiting = match derived {
        WorkloadState::Crashed => state.restart_due.contains_key(id) || state.resuming.contains(id),
        WorkloadState::Stopped => state.resuming.contains(id),
        _ => false,
    };
    if waiting { WorkloadState::Restarting } else { derived }
}
