//! blocklyd's core: workloads on this host, their records, ports and observations, and every
//! operation the protocol offers.
//!
//! Authority is split deliberately (docs/fleet.md, "The parts"). The control plane owns what
//! should exist and whether it should run. This host owns what does exist: containers, the ports
//! it handed out, where data lives, what it observed. blocklyd never decides on its own to
//! start, stop or delete a workload, with three exceptions it carries out itself (every
//! container's Docker restart policy is `no`): the restart-on-failure policy the spec asked for
//! and resuming what was running when the host went down, both under the execution lease in fleet
//! mode, and stopping a superseded copy it finds running (a fence recorded but not carried out).
//!
//! Operations on one workload are serialized by a per-workload lock, so a duplicate or retried
//! request waits for the first and then finds the work done. Each verb is idempotent: `ensure`
//! compares specs, `start` of a running workload and `stop` of a stopped one are no-ops, and a
//! second `delete` finds nothing.
//!
//! Parts (`manager/`):
//! - `room.rs`: whether the host has room for a workload: memory to run, disk above the floor.
//! - `view.rs`: a workload as the protocol shows it.
//! - `ensure.rs`: makes a workload match the spec it was sent.
//! - `power.rs`: starts, stops and kills a workload when asked.
//! - `restarts.rs`: brings a workload back without being asked, after a crash or a host restart.
//! - `fence.rs`: fences a copy a newer placement has superseded.
//! - `delete.rs`: removes a workload, and its data when told to.
//! - `archive.rs`: carries a world's data between this node and an object store.
//! - `snapshots.rs`: local snapshots of a workload's data.
//! - `exec.rs`: runs a command inside a running workload.
//! - `logs.rs`: reads a workload's log.
//! - `stats.rs`: samples what workloads use.
//! - `fleet.rs`: the node's standing with its control plane.
//! - `node.rs`: what the node reports about itself as a whole.
//! - `reconcile.rs`: makes the records agree with what Docker has.
//! - `persist.rs`: writes a record from memory, in turn with every other write of the state.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::IpAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use time::OffsetDateTime;
use tokio::sync::OnceCell;
use tokio::sync::mpsc;

use crate::clock::now;
use crate::config::Config;
use crate::host;
use crate::ids::{SnapshotId, WorkloadId};
use crate::labels::{
    LABEL_DEPLOYMENT, LABEL_DIGEST, LABEL_EPOCH, LABEL_GENERATION, LABEL_MANAGED, LABEL_NODE, LABEL_RECORD,
    LABEL_WORKLOAD,
};
use crate::metrics::Metrics;
use crate::ports::{PortAllocator, PortError, Probe};
use crate::protocol::{
    Audience, CapacityView, DaemonView, DataDisposition, DataOutcome, DeleteResponse, DockerView, EnsureOutcome,
    EnsureResponse, ExecRequest, ExecResponse, ExitInfo, ExportRequest, ExportResponse, FenceResponse, FieldError,
    Issue, Locate, LogRecord, NodeHealth, NodeStatus, PartsTarget, PortView, PowerResponse, ProtocolVersions, PutPart,
    ReconcileView, RestoreRequest, RestoreResponse, SnapshotDeleteResponse, SnapshotList, SnapshotRequest,
    SnapshotResponse, SnapshotView, SpecPolicy, SpecRecord, StatsView, StorageView, UploadRequest, WorkloadSpec,
    WorkloadState, WorkloadView,
};
use crate::runtime::{
    ContainerInfo, ContainerRuntime, ContainerSpec, ContainerStatus, LogOptions, LogStream, PortBinding, RawStats,
    RuntimeError, RuntimeInfo, format_time, parse_time,
};
use crate::store::{AllocatedPort, Phase, RECORD_VERSION, Recovery, Store, WorkloadRecord};

mod archive;
mod delete;
mod ensure;
mod exec;
mod fence;
mod fleet;
mod logs;
mod node;
mod persist;
mod power;
mod reconcile;
mod restarts;
mod room;
mod snapshots;
mod stats;
mod view;

pub use logs::lines_of;
pub use node::WorkloadSample;

fn now_str() -> String {
    format_time(now())
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum NodeError {
    #[error("the request is not valid")]
    Invalid(Vec<FieldError>),
    #[error("no workload {0}")]
    NotFound(WorkloadId),
    #[error("no snapshot {0} of this workload on this node")]
    SnapshotNotFound(String),
    #[error("the workload's current spec is not the one the request expected")]
    PreconditionFailed { current: Option<String> },
    #[error("{message}")]
    Conflict { code: &'static str, message: String },
    #[error("{0}")]
    InsufficientCapacity(String),
    #[error("{0}")]
    InsufficientDisk(String),
    #[error("{0}")]
    NoFreePorts(String),
    #[error("deleting data needs the {} header set to the workload id", crate::protocol::CONFIRM_DELETE_HEADER)]
    ConfirmationRequired,
    #[error("this idempotency key was used for a different request")]
    IdempotencyMismatch,
    #[error("{0}")]
    RuntimeUnavailable(String),
    #[error("{0}")]
    Timeout(String),
    #[error("the container runtime refused: {0}")]
    Runtime(String),
    #[error("{0}")]
    Internal(String),
    #[error("this request acts for epoch {asked}; this copy belongs to epoch {current:?}, which is newer")]
    StaleEpoch { asked: u64, current: Option<u64> },
    #[error("this copy belongs to epoch {current:?}; PUT the spec with epoch {asked} first")]
    EpochAhead { asked: u64, current: Option<u64> },
    #[error("this copy was superseded by epoch {by}; it will not run again")]
    Superseded { by: u64 },
    #[error("this workload belongs to epoch {current}; send it in the {} header", crate::protocol::EPOCH_HEADER)]
    EpochRequired { current: u64 },
    #[error("{0}")]
    InvalidArchive(String),
    #[error("the archive's sha256 is {actual}, not {expected}")]
    ChecksumMismatch { expected: String, actual: String },
    #[error("{0}")]
    Transfer(String),
    #[error("the archive is {size_bytes} bytes; one upload to the store carries at most {limit_bytes}")]
    ArchiveTooLarge { size_bytes: u64, limit_bytes: u64 },
}

/// How a verb treats the placement epoch it is sent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EpochRule {
    /// Runs the copy (start, exec): only for exactly its epoch, and never once superseded.
    Exact,
    /// Tears the copy down (stop, kill, delete): its epoch or any newer one, since a newer
    /// placement may always clean up an older copy.
    Teardown,
    /// Places the workload here (ensure): its epoch, or a newer one that takes the copy over.
    Place,
}

/// The fencing check every mutating verb makes before touching the runtime. Workloads made
/// without an epoch keep the single-node protocol: none is asked for and none is checked.
pub fn check_epoch(record: &WorkloadRecord, asked: Option<u64>, rule: EpochRule) -> Result<(), NodeError> {
    match (asked, record.epoch) {
        (None, None) => {}
        (None, Some(current)) => return Err(NodeError::EpochRequired { current }),
        (Some(asked), current) => {
            let floor = current.unwrap_or(0);
            if asked < floor {
                return Err(NodeError::StaleEpoch { asked, current });
            }
            if asked > floor && rule == EpochRule::Exact {
                return Err(NodeError::EpochAhead { asked, current });
            }
        }
    }
    if let Some(by) = record.superseded_by {
        let revived = rule == EpochRule::Place && asked.is_some_and(|a| a > by);
        if rule != EpochRule::Teardown && !revived {
            return Err(NodeError::Superseded { by });
        }
    }
    Ok(())
}

impl From<RuntimeError> for NodeError {
    fn from(e: RuntimeError) -> Self {
        match e {
            RuntimeError::Unavailable(m) => NodeError::RuntimeUnavailable(m),
            RuntimeError::Timeout(m) => NodeError::Timeout(m),
            other => NodeError::Runtime(other.to_string()),
        }
    }
}

impl From<crate::store::StoreError> for NodeError {
    fn from(e: crate::store::StoreError) -> Self {
        NodeError::Internal(e.to_string())
    }
}

/// HTTP conditional semantics on `PUT /v1/workloads/{id}`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Precondition {
    /// Create, or converge onto this spec (replacing a different one).
    None,
    /// `If-Match: "<digest>"` or `*`: only replace this exact current spec (or any existing one).
    IfMatch(String),
    /// `If-None-Match: *`: only create; never touch an existing workload.
    IfNoneMatch,
}

#[derive(Clone, Debug)]
struct Observed {
    info: Option<ContainerInfo>,
    /// The runtime era it was seen in (see `runtime_era`): a transition only counts as a crash
    /// blocklyd saw happen if nothing interrupted its view in between.
    era: u64,
}

#[derive(Clone, Debug)]
struct StatsSample {
    at: Instant,
    raw: RawStats,
    cores: Option<f64>,
}

impl StatsSample {
    /// Usage minus inactive page cache: what the kernel would have to reclaim to fit.
    fn working_set(&self) -> Option<u64> {
        self.raw.memory_usage.map(|u| u.saturating_sub(self.raw.memory_inactive_file.unwrap_or(0)))
    }
}

#[derive(Default)]
struct IdempotencyCache {
    entries: HashMap<String, (String, Arc<OnceCell<ExecResponse>>, Instant)>,
}

struct State {
    records: BTreeMap<WorkloadId, WorkloadRecord>,
    observed: HashMap<WorkloadId, Observed>,
    ports: PortAllocator,
    issues: HashMap<WorkloadId, Vec<Issue>>,
    host_issues: Vec<Issue>,
    stats: HashMap<WorkloadId, StatsSample>,
    disk: HashMap<WorkloadId, (u64, OffsetDateTime)>,
    stopping: HashSet<WorkloadId>,
    last_failure: HashMap<WorkloadId, OffsetDateTime>,
    /// Failures waiting for their restart, and when it is due.
    restart_due: HashMap<WorkloadId, Instant>,
    /// Workloads the host went down under, waiting to be resumed (see `resume`).
    resuming: HashSet<WorkloadId>,
    /// Snapshots being uploaded, which a delete waits for.
    uploading: HashSet<(WorkloadId, SnapshotId)>,
    /// What local snapshots hold on disk, as last measured.
    snapshot_bytes: Option<u64>,
    last_reconcile: Option<ReconcileView>,
}

pub struct Manager {
    pub config: Arc<Config>,
    pub runtime: Arc<dyn ContainerRuntime>,
    pub store: Store,
    pub metrics: Arc<Metrics>,
    state: Mutex<State>,
    /// Records and the port quarantine are written one at a time, newest last (`persist.rs`).
    disk_writes: Mutex<()>,
    locks: Mutex<HashMap<WorkloadId, Arc<tokio::sync::Mutex<()>>>>,
    exec_keys: Mutex<IdempotencyCache>,
    pub started_at: OffsetDateTime,
    started: Instant,
    docker_up: AtomicBool,
    reconciled: AtomicBool,
    /// Bumped whenever the runtime stops answering: what was seen before an outage can't be
    /// compared with what is seen after it.
    runtime_era: std::sync::atomic::AtomicU64,
    restart_wake: tokio::sync::Notify,
    runtime_info: Mutex<Option<RuntimeInfo>>,
    memory_total_mb: u64,
    cpus: u32,
    fleet: Mutex<FleetState>,
    /// This boot of the host (the kernel's boot id), if it can be read.
    boot_id: Option<String>,
    /// Whether the data's filesystem can share blocks between files (probed at startup).
    reflink: bool,
    /// Workloads the host went down under have been looked for, once, after the first full pass.
    resume_checked: AtomicBool,
}

/// What the node knows of its control plane, in fleet mode.
#[derive(Clone, Debug, Default)]
struct FleetState {
    node_id: Option<String>,
    control_plane: Option<String>,
    lifecycle: Option<String>,
    last_contact: Option<Instant>,
    last_contact_at: Option<OffsetDateTime>,
    last_error: Option<String>,
    last_latency_ms: Option<u64>,
    beats_ok: u64,
    beats_failed: u64,
    /// Until when this node may restart a failed workload on its own: the execution lease, from
    /// the moment the heartbeat that granted it was sent, on `boottime()`.
    lease_until: Option<Duration>,
    renewed_at: Option<OffsetDateTime>,
}

/// What the execution lease allows now.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lease {
    /// Not in fleet mode: the single-node daemon restarts as its policy says.
    NotFleet,
    Held,
    /// No answer from the control plane yet in this run, or none for longer than the lease.
    Lapsed,
    /// The control plane holds this node lost: its workloads may be running elsewhere.
    Revoked,
}

/// What `try_restart` made of a due restart.
enum Restart {
    Done,
    /// A resume waits for the control plane to grant a lease, or the workload is busy.
    Waiting,
}

/// The ports and their addresses, per audience, as the host publishes them.
fn host_ips(config: &Config, audience: &[Audience]) -> Vec<IpAddr> {
    let mut ips: Vec<IpAddr> = Vec::new();
    for a in audience {
        let list = match a {
            Audience::Edge => &config.network.edge_ips,
            Audience::Control => &config.network.control_ips,
        };
        for ip in list {
            if !ips.contains(ip) {
                ips.push(*ip);
            }
        }
    }
    ips
}

fn endpoint(ip: &IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(v4) => format!("{v4}:{port}"),
        IpAddr::V6(v6) => format!("[{v6}]:{port}"),
    }
}

/// Requested stops end with these without being failures: SIGTERM handled late (143), SIGINT
/// (130), or a clean exit.
fn clean_exit(code: i64) -> bool {
    matches!(code, 0 | 130 | 143)
}

impl Manager {
    pub fn new(
        config: Arc<Config>,
        runtime: Arc<dyn ContainerRuntime>,
        store: Store,
        metrics: Arc<Metrics>,
        probe: Probe,
    ) -> Arc<Self> {
        Self::with_boot(config, runtime, store, metrics, probe, host::boot_id())
    }

    /// As `new`, in a boot of the host's choosing: tests restart "the host" this way.
    pub fn with_boot(
        config: Arc<Config>,
        runtime: Arc<dyn ContainerRuntime>,
        store: Store,
        metrics: Arc<Metrics>,
        probe: Probe,
        boot_id: Option<String>,
    ) -> Arc<Self> {
        let [low, high] = config.network.port_range;
        let ports = PortAllocator::new(low..=high, Duration::from_secs(config.network.port_quarantine_seconds), probe);
        let facts = host::facts();
        Arc::new(Self {
            state: Mutex::new(State {
                records: BTreeMap::new(),
                observed: HashMap::new(),
                ports,
                issues: HashMap::new(),
                host_issues: Vec::new(),
                stats: HashMap::new(),
                disk: HashMap::new(),
                stopping: HashSet::new(),
                last_failure: HashMap::new(),
                restart_due: HashMap::new(),
                resuming: HashSet::new(),
                uploading: HashSet::new(),
                snapshot_bytes: None,
                last_reconcile: None,
            }),
            disk_writes: Mutex::new(()),
            locks: Mutex::new(HashMap::new()),
            exec_keys: Mutex::new(IdempotencyCache::default()),
            started_at: now(),
            started: Instant::now(),
            docker_up: AtomicBool::new(false),
            reconciled: AtomicBool::new(false),
            runtime_era: std::sync::atomic::AtomicU64::new(0),
            restart_wake: tokio::sync::Notify::new(),
            runtime_info: Mutex::new(None),
            memory_total_mb: facts.memory_total_bytes.map_or(0, |b| b / (1024 * 1024)),
            cpus: facts.cpus,
            fleet: Mutex::new(FleetState::default()),
            boot_id,
            reflink: crate::tree::reflink_supported(&store.workloads_dir()),
            resume_checked: AtomicBool::new(false),
            config,
            runtime,
            store,
            metrics,
        })
    }

    pub fn uptime(&self) -> Duration {
        self.started.elapsed()
    }

    pub fn docker_up(&self) -> bool {
        self.docker_up.load(Ordering::SeqCst)
    }

    pub fn reconciled(&self) -> bool {
        self.reconciled.load(Ordering::SeqCst)
    }

    /// Whether what blocklyd remembers about workloads is current: the runtime answers, and a
    /// full pass has looked since it last didn't. Otherwise every state reads `unknown`: after an
    /// outage the last observation is history (a daemon stop kills workloads), and reporting it
    /// as the state would tell the control plane something false.
    pub fn trustworthy(&self) -> bool {
        self.docker_up() && self.reconciled()
    }

    /// Records whether the runtime answered; every runtime error funnels through here. An
    /// outage also forgets that blocklyd was reconciled: whatever happened while it couldn't
    /// look (a daemon stop kills workloads) is unknown until a full pass has looked again.
    fn note<T>(&self, result: Result<T, RuntimeError>) -> Result<T, RuntimeError> {
        match &result {
            Err(e) if e.is_unavailable() => {
                self.docker_up.store(false, Ordering::SeqCst);
                self.reconciled.store(false, Ordering::SeqCst);
                self.runtime_era.fetch_add(1, Ordering::SeqCst);
            }
            _ => self.docker_up.store(true, Ordering::SeqCst),
        }
        result
    }

    fn lock_for(&self, id: &WorkloadId) -> Arc<tokio::sync::Mutex<()>> {
        self.locks.lock().unwrap().entry(id.clone()).or_default().clone()
    }

    fn container_name(&self, id: &WorkloadId) -> String {
        format!("blockly-{}-{}", self.config.deployment_id, id)
    }

    pub fn allocatable_memory_mb(&self) -> u64 {
        self.config.capacity.allocatable_mb(self.memory_total_mb)
    }

    pub fn spec_policy(&self) -> SpecPolicy {
        SpecPolicy {
            allowed_images: self.config.workloads.allowed_images.clone(),
            min_memory_mb: self.config.workloads.min_memory_mb,
            max_memory_mb: self.allocatable_memory_mb().min(u32::MAX as u64) as u32,
            max_cpu_millis: self.cpus.max(1) * 1000,
        }
    }

    pub fn owner_labels(&self) -> Vec<String> {
        vec![
            format!("{LABEL_MANAGED}=true"),
            format!("{LABEL_DEPLOYMENT}={}", self.config.deployment_id),
            format!("{LABEL_NODE}={}", self.config.node_id),
        ]
    }

    fn set_issue(&self, id: &WorkloadId, issue: Issue) {
        let mut state = self.state.lock().unwrap();
        let list = state.issues.entry(id.clone()).or_default();
        if !list.iter().any(|i| i.code == issue.code) {
            list.push(issue);
        }
    }

    /// A freshly made container settles whatever was wrong with the old one, a restart it was
    /// refused included.
    fn clear_resolved_issues(&self, id: &WorkloadId) {
        const RESOLVED: [&str; 6] = [
            "container_missing",
            "create_incomplete",
            "digest_mismatch",
            "port_mismatch",
            "unexpected_container",
            "insufficient_capacity",
        ];
        if let Some(list) = self.state.lock().unwrap().issues.get_mut(id) {
            list.retain(|i| !RESOLVED.contains(&i.code.as_str()));
        }
    }

    fn clear_issue(&self, id: &WorkloadId, code: &str) {
        if let Some(list) = self.state.lock().unwrap().issues.get_mut(id) {
            list.retain(|i| i.code != code);
        }
    }

    /// Writes the port quarantine to disk, as it is when its turn comes. A failure is logged, not
    /// fatal: at worst a restart forgets which ports were resting.
    fn persist_resting_ports(&self) {
        let _turn = self.disk_writes.lock().unwrap();
        let now_unix = now().unix_timestamp();
        let resting: Vec<crate::store::RestingPort> = self
            .state
            .lock()
            .unwrap()
            .ports
            .resting()
            .into_iter()
            .map(|(protocol, port, ago)| crate::store::RestingPort {
                protocol,
                port,
                released_at_unix: now_unix - ago.as_secs() as i64,
            })
            .collect();
        if let Err(e) = self.store.save_resting_ports(&resting) {
            tracing::warn!(error = %e, "couldn't persist the port quarantine");
        }
    }

    fn save_record(&self, record: &WorkloadRecord) -> Result<(), NodeError> {
        let _turn = self.disk_writes.lock().unwrap();
        self.store.save(record)?;
        self.state.lock().unwrap().records.insert(record.id.clone(), record.clone());
        Ok(())
    }

    fn record(&self, id: &WorkloadId) -> Result<WorkloadRecord, NodeError> {
        self.state.lock().unwrap().records.get(id).cloned().ok_or_else(|| NodeError::NotFound(id.clone()))
    }

    async fn inspect(&self, name: &str) -> Result<Option<ContainerInfo>, NodeError> {
        Ok(self.note(self.runtime.inspect(name).await)?)
    }

    /// Looks at the container now and remembers what it saw.
    async fn observe(&self, record: &WorkloadRecord) -> Result<Option<ContainerInfo>, NodeError> {
        let info = if record.phase == Phase::Retained { None } else { self.inspect(&record.container_name).await? };
        self.remember(&record.id, info.clone());
        Ok(info)
    }

    fn remember(&self, id: &WorkloadId, info: Option<ContainerInfo>) {
        let mut state = self.state.lock().unwrap();
        let era = self.runtime_era.load(Ordering::SeqCst);
        // A failure blocklyd watched happen: it was running at the last look, in this same era,
        // and has now exited badly. Anything else (found dead at startup, after a reboot or a
        // runtime outage) is reported and left alone.
        let was_running = state.observed.get(id).is_some_and(|o| {
            o.era == era
                && o.info
                    .as_ref()
                    .is_some_and(|i| matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting))
        });
        let failed = info.as_ref().is_some_and(|i| {
            matches!(i.status, ContainerStatus::Exited | ContainerStatus::Dead)
                && (!clean_exit(i.exit_code) || i.oom_killed)
        });
        if let (true, Some(i)) = (was_running && failed, info.as_ref())
            && let Some(at) = i.finished_at
        {
            state.last_failure.insert(id.clone(), at);
        }
        // Which boot last saw it running: what tells, after the host restarts, that it went down
        // under the workload rather than the workload stopping first.
        let running = info.as_ref().is_some_and(|i| {
            matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused)
        });
        let mut boot_changed = false;
        if let (Some(boot), Some(record)) = (self.boot_id.as_deref(), state.records.get_mut(id)) {
            let mark = if running {
                (record.running_boot.as_deref() != Some(boot)).then(|| Some(boot.to_owned()))
            } else {
                (record.running_boot.as_deref() == Some(boot)).then_some(None)
            };
            if let Some(mark) = mark {
                record.running_boot = mark;
                boot_changed = true;
            }
        }
        let attempt = state.records.get(id).map_or(0, |r| r.restart_count);
        // Only when a restart will really follow: its policy allows one, retries are left, nothing
        // superseded this copy, and the exit reads as a crash (one after a stop was asked for is
        // that stop, a kill or a SIGKILL included). Otherwise the failure reads as what it is.
        let restartable = state.records.get(id).is_some_and(|r| {
            r.superseded_by.is_none()
                && r.spec.restart.policy == crate::protocol::RestartPolicy::OnFailure
                && attempt < r.spec.restart.max_retries
                && derive_power_state(r, info.as_ref(), false, true) == WorkloadState::Crashed
        });
        state.observed.insert(id.clone(), Observed { info, era });
        if was_running && failed && restartable {
            state.restart_due.insert(id.clone(), Instant::now() + restart_backoff(attempt));
            self.restart_wake.notify_one();
        }
        drop(state);
        if boot_changed && let Err(e) = self.write_record(id) {
            tracing::warn!(workload = %id, error = %e, "couldn't record which boot the workload ran in");
        }
    }

    /// Takes a runtime event: a hint to look again, never the truth itself.
    pub async fn on_event(self: &Arc<Self>, container: &str, action: &str, exit_code: Option<i64>, at: OffsetDateTime) {
        self.metrics.runtime_event(action);
        let id = {
            let state = self.state.lock().unwrap();
            state
                .records
                .values()
                .find(|r| r.container_id.as_deref() == Some(container) || r.container_name == container)
                .map(|r| r.id.clone())
        };
        let Some(id) = id else { return };
        if action == "die" && exit_code.is_some_and(|c| !clean_exit(c)) {
            self.state.lock().unwrap().last_failure.insert(id.clone(), at);
        }
        if let Ok(record) = self.record(&id) {
            let _ = self.observe(&record).await;
        }
    }
}

/// Docker's own rhythm: short at first, then doubling, capped.
fn restart_backoff(attempt: u32) -> Duration {
    Duration::from_millis(500u64.saturating_mul(1 << attempt.min(5))).min(Duration::from_secs(10))
}

/// The record as a label carries it.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct LabelRecord {
    pub version: u32,
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    pub spec_digest: String,
    pub ports: Vec<AllocatedPort>,
    pub spec: SpecRecord,
    pub created_at: String,
}

impl LabelRecord {
    fn of(record: &WorkloadRecord) -> Self {
        Self {
            version: RECORD_VERSION,
            generation: record.generation,
            epoch: record.epoch,
            spec_digest: record.spec_digest.clone(),
            ports: record.ports.clone(),
            spec: record.spec.clone(),
            created_at: record.created_at.clone(),
        }
    }
}

/// A start that fails because the port is taken says so plainly, and names the workload.
fn port_clash(id: &WorkloadId, e: RuntimeError) -> NodeError {
    let text = e.to_string();
    if text.contains("port is already allocated") || text.contains("address already in use") {
        NodeError::Conflict { code: "port_conflict", message: format!("{id}: a host port it holds is in use: {text}") }
    } else {
        e.into()
    }
}

pub fn derive_state(
    record: &WorkloadRecord,
    info: Option<&ContainerInfo>,
    stopping: bool,
    trustworthy: bool,
) -> WorkloadState {
    let state = derive_power_state(record, info, stopping, trustworthy);
    let at_rest = matches!(
        state,
        WorkloadState::Created | WorkloadState::Stopped | WorkloadState::Crashed | WorkloadState::Missing
    );
    if record.superseded_by.is_some() && at_rest { WorkloadState::Fenced } else { state }
}

fn derive_power_state(
    record: &WorkloadRecord,
    info: Option<&ContainerInfo>,
    stopping: bool,
    trustworthy: bool,
) -> WorkloadState {
    match record.phase {
        Phase::Retained => WorkloadState::Retained,
        Phase::Creating if info.is_none() => WorkloadState::Creating,
        // What blocklyd last saw is history, not the state: a daemon stop may have killed it.
        _ if !trustworthy => WorkloadState::Unknown,
        _ => match info {
            None => WorkloadState::Missing,
            Some(i) => match i.status {
                ContainerStatus::Created => WorkloadState::Created,
                ContainerStatus::Running | ContainerStatus::Paused => {
                    if stopping {
                        WorkloadState::Stopping
                    } else {
                        WorkloadState::Running
                    }
                }
                ContainerStatus::Restarting => WorkloadState::Restarting,
                ContainerStatus::Removing => WorkloadState::Stopping,
                ContainerStatus::Exited | ContainerStatus::Dead => {
                    let requested = parse_time(record.stop_requested_at.as_deref());
                    // A little slack: the runtime's clock and ours are the same host's, but its
                    // timestamps are finer and a stop can be recorded a hair after the exit.
                    let after_request =
                        requested.is_some_and(|r| i.finished_at.is_none_or(|f| f >= r - time::Duration::seconds(2)));
                    if after_request || (clean_exit(i.exit_code) && !i.oom_killed) {
                        WorkloadState::Stopped
                    } else {
                        WorkloadState::Crashed
                    }
                }
                ContainerStatus::Unknown => WorkloadState::Unknown,
            },
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Phase;

    fn record(phase: Phase, stop_requested_at: Option<&str>) -> WorkloadRecord {
        let mut r = crate::store::tests::record("w");
        r.phase = phase;
        r.stop_requested_at = stop_requested_at.map(str::to_owned);
        r
    }

    fn exited(code: i64, oom: bool, finished: &str) -> ContainerInfo {
        ContainerInfo {
            id: "c".into(),
            name: "n".into(),
            labels: BTreeMap::new(),
            env: BTreeMap::new(),
            status: ContainerStatus::Exited,
            exit_code: code,
            oom_killed: oom,
            started_at: None,
            finished_at: parse_time(Some(finished)),
            restart_count: 0,
            health: None,
        }
    }

    #[test]
    fn exits_are_stops_or_crashes_by_what_was_asked() {
        let at = "2026-09-28T10:00:00Z";
        let later = "2026-09-28T10:00:05Z";
        let r = record(Phase::Active, None);
        assert_eq!(derive_state(&r, Some(&exited(0, false, later)), false, true), WorkloadState::Stopped);
        assert_eq!(derive_state(&r, Some(&exited(1, false, later)), false, true), WorkloadState::Crashed);
        assert_eq!(derive_state(&r, Some(&exited(137, true, later)), false, true), WorkloadState::Crashed);
        let asked = record(Phase::Active, Some(at));
        assert_eq!(derive_state(&asked, Some(&exited(137, false, later)), false, true), WorkloadState::Stopped);
        // A crash before the stop request is still a crash.
        let long_before = "2026-09-28T09:00:00Z";
        assert_eq!(derive_state(&asked, Some(&exited(1, false, long_before)), false, true), WorkloadState::Crashed);
    }

    #[test]
    fn missing_vs_unknown_depends_on_whether_the_runtime_answered() {
        let r = record(Phase::Active, None);
        assert_eq!(derive_state(&r, None, false, true), WorkloadState::Missing);
        assert_eq!(derive_state(&r, None, false, false), WorkloadState::Unknown);
        assert_eq!(derive_state(&record(Phase::Retained, None), None, false, true), WorkloadState::Retained);
        assert_eq!(derive_state(&record(Phase::Creating, None), None, false, true), WorkloadState::Creating);
    }
}
