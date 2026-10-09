//! Prometheus metrics (OpenMetrics text) on the ops listener.
//!
//! prometheus-client is the Prometheus project's own Rust client: no global registry, no
//! background thread, a few hundred lines of dependency. Counters and histograms are recorded as
//! things happen; gauges that describe state (workloads by state, ports, per-workload usage) are
//! computed at scrape time from the manager, so they can't drift from it.

use std::sync::Mutex;
use std::time::Duration;

use hyper::Method;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicU64;

use crate::manager::{Manager, NodeError};

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OpLabels {
    op: String,
    outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct OpOnly {
    op: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpLabels {
    method: String,
    route: String,
    status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Reason {
    reason: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct Outcome {
    outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StateLabel {
    state: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct WorkloadLabel {
    workload: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ActionLabel {
    action: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct InfoLabels {
    version: String,
    protocol: String,
    node: String,
    deployment: String,
}

type FloatGauge = Gauge<f64, AtomicU64>;

pub struct Metrics {
    registry: Registry,
    operations: Family<OpLabels, Counter>,
    operation_seconds: Family<OpOnly, Histogram>,
    http_requests: Family<HttpLabels, Counter>,
    auth_failures: Family<Reason, Counter>,
    reconciles: Family<Outcome, Counter>,
    reconcile_seconds: Histogram,
    runtime_events: Family<ActionLabel, Counter>,
    info: Family<InfoLabels, Gauge>,
    uptime: FloatGauge,
    docker_up: Gauge,
    workloads: Family<StateLabel, Gauge>,
    ports_allocated: Gauge,
    ports_capacity: Gauge,
    issues: Gauge,
    workload_memory: Family<WorkloadLabel, Gauge>,
    workload_cpu: Family<WorkloadLabel, FloatGauge>,
    workload_disk: Family<WorkloadLabel, Gauge>,
    daemon_rss: Gauge,
    daemon_cpu: FloatGauge,
    heartbeats: Family<Outcome, Counter>,
    fleet_contact_age: FloatGauge,
    fleet_lease: FloatGauge,
    renewals: Family<Outcome, Counter>,
    snapshot_bytes: Gauge,
    scrape: Mutex<()>,
}

fn op_buckets() -> impl Iterator<Item = f64> {
    [0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0].into_iter()
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("blocklyd");
        let operations = Family::<OpLabels, Counter>::default();
        let operation_seconds = Family::<OpOnly, Histogram>::new_with_constructor(|| Histogram::new(op_buckets()));
        let http_requests = Family::<HttpLabels, Counter>::default();
        let auth_failures = Family::<Reason, Counter>::default();
        let reconciles = Family::<Outcome, Counter>::default();
        let reconcile_seconds = Histogram::new(op_buckets());
        let runtime_events = Family::<ActionLabel, Counter>::default();
        let info = Family::<InfoLabels, Gauge>::default();
        let uptime = FloatGauge::default();
        let docker_up = Gauge::default();
        let workloads = Family::<StateLabel, Gauge>::default();
        let ports_allocated = Gauge::default();
        let ports_capacity = Gauge::default();
        let issues = Gauge::default();
        let workload_memory = Family::<WorkloadLabel, Gauge>::default();
        let workload_cpu = Family::<WorkloadLabel, FloatGauge>::default();
        let workload_disk = Family::<WorkloadLabel, Gauge>::default();
        let daemon_rss = Gauge::default();
        let daemon_cpu = FloatGauge::default();
        let heartbeats = Family::<Outcome, Counter>::default();
        let fleet_contact_age = FloatGauge::default();
        let fleet_lease = FloatGauge::default();
        let renewals = Family::<Outcome, Counter>::default();
        let snapshot_bytes = Gauge::default();

        registry.register("operations", "Workload operations by kind and outcome", operations.clone());
        registry.register("operation_duration_seconds", "Time per workload operation", operation_seconds.clone());
        registry.register("http_requests", "API requests by route and status", http_requests.clone());
        registry.register("auth_failures", "Refused TLS handshakes and unauthorized identities", auth_failures.clone());
        registry.register("reconciles", "Reconciliation passes by outcome", reconciles.clone());
        registry.register("reconcile_duration_seconds", "Time per reconciliation pass", reconcile_seconds.clone());
        registry.register("runtime_events", "Container runtime events seen, by action", runtime_events.clone());
        registry.register("info", "blocklyd version, protocol and node identity", info.clone());
        registry.register("uptime_seconds", "Seconds since blocklyd started", uptime.clone());
        registry.register(
            "docker_up",
            "1 when the container runtime answered last time it was asked",
            docker_up.clone(),
        );
        registry.register("workloads", "Workloads by state", workloads.clone());
        registry.register("ports_allocated", "Host ports held by workloads", ports_allocated.clone());
        registry.register("ports_capacity", "Host ports this node may hand out, per protocol", ports_capacity.clone());
        registry.register("issues", "Open reconciliation issues, host and workload", issues.clone());
        registry.register("workload_memory_working_set_bytes", "Memory in use per workload", workload_memory.clone());
        registry.register("workload_cpu_cores", "Cores in use per workload over the last sample", workload_cpu.clone());
        registry.register("workload_data_bytes", "Data on disk per workload", workload_disk.clone());
        registry.register("process_resident_memory_bytes", "blocklyd's own resident memory", daemon_rss.clone());
        registry.register("process_cpu_seconds", "blocklyd's own CPU time", daemon_cpu.clone());
        registry.register("heartbeats", "Heartbeats to the control plane, by outcome (fleet mode)", heartbeats.clone());
        registry.register(
            "fleet_contact_age_seconds",
            "Seconds since the control plane last answered a heartbeat (fleet mode; -1 before the first)",
            fleet_contact_age.clone(),
        );
        registry.register(
            "fleet_lease_remaining_seconds",
            "Seconds this node may still restart a failed workload on its own (fleet mode; -1 without a lease)",
            fleet_lease.clone(),
        );
        registry.register("certificate_renewals", "Certificate renewals, by outcome (fleet mode)", renewals.clone());
        registry.register(
            "snapshot_bytes",
            "Disk local snapshots hold, as their files count it",
            snapshot_bytes.clone(),
        );

        Self {
            registry,
            operations,
            operation_seconds,
            http_requests,
            auth_failures,
            reconciles,
            reconcile_seconds,
            runtime_events,
            info,
            uptime,
            docker_up,
            workloads,
            ports_allocated,
            ports_capacity,
            issues,
            workload_memory,
            workload_cpu,
            workload_disk,
            daemon_rss,
            daemon_cpu,
            heartbeats,
            fleet_contact_age,
            fleet_lease,
            renewals,
            snapshot_bytes,
            scrape: Mutex::new(()),
        }
    }

    pub fn operation<T>(&self, op: &str, result: &Result<T, NodeError>, took: Duration) {
        let outcome = match result {
            Ok(_) => "ok",
            Err(
                NodeError::Invalid(_)
                | NodeError::PreconditionFailed { .. }
                | NodeError::NotFound(_)
                | NodeError::SnapshotNotFound(_),
            ) => "refused",
            Err(NodeError::Conflict { .. } | NodeError::ConfirmationRequired | NodeError::IdempotencyMismatch) => {
                "refused"
            }
            Err(NodeError::InsufficientCapacity(_) | NodeError::InsufficientDisk(_) | NodeError::NoFreePorts(_)) => {
                "full"
            }
            Err(NodeError::RuntimeUnavailable(_)) => "runtime_unavailable",
            Err(_) => "error",
        };
        self.operations.get_or_create(&OpLabels { op: op.into(), outcome: outcome.into() }).inc();
        self.operation_seconds.get_or_create(&OpOnly { op: op.into() }).observe(took.as_secs_f64());
    }

    /// A request the API answered. A client may send any token as its method, and every new label
    /// value is a series kept for good, so only the usual methods are named (`method_label`).
    pub fn http(&self, method: &Method, route: &str, status: u16) {
        let method = method_label(method).into();
        self.http_requests.get_or_create(&HttpLabels { method, route: route.into(), status: status.to_string() }).inc();
    }

    pub fn auth_failure(&self, reason: &str) {
        self.auth_failures.get_or_create(&Reason { reason: reason.into() }).inc();
    }

    pub fn reconcile(&self, ok: bool, took: Duration) {
        self.reconciles.get_or_create(&Outcome { outcome: if ok { "ok" } else { "error" }.into() }).inc();
        self.reconcile_seconds.observe(took.as_secs_f64());
    }

    pub fn heartbeat(&self, ok: bool) {
        self.heartbeats.get_or_create(&Outcome { outcome: if ok { "ok" } else { "error" }.into() }).inc();
    }

    pub fn renewal(&self, ok: bool) {
        self.renewals.get_or_create(&Outcome { outcome: if ok { "ok" } else { "error" }.into() }).inc();
    }

    pub fn runtime_event(&self, action: &str) {
        // Health events carry their result after a colon; exec events their command.
        let action = action.split(':').next().unwrap_or(action).trim();
        self.runtime_events.get_or_create(&ActionLabel { action: action.into() }).inc();
    }

    /// Sets the state gauges from the manager, then encodes everything.
    pub fn render(&self, manager: &Manager) -> String {
        let _one_at_a_time = self.scrape.lock().unwrap();
        self.info
            .get_or_create(&InfoLabels {
                version: env!("CARGO_PKG_VERSION").into(),
                protocol: crate::protocol::PROTOCOL_VERSION.to_string(),
                node: manager.config.node_id.to_string(),
                deployment: manager.config.deployment_id.clone(),
            })
            .set(1);
        self.uptime.set(manager.uptime().as_secs_f64());
        self.docker_up.set(i64::from(manager.docker_up()));
        for (state, count) in manager.workloads_by_state() {
            self.workloads.get_or_create(&StateLabel { state: state.as_str().into() }).set(count as i64);
        }
        let (allocated, capacity) = manager.ports_usage();
        self.ports_allocated.set(allocated as i64);
        self.ports_capacity.set(capacity as i64);
        self.issues.set(manager.issue_count() as i64);
        self.workload_memory.clear();
        self.workload_cpu.clear();
        self.workload_disk.clear();
        for sample in manager.workload_samples() {
            let label = WorkloadLabel { workload: sample.id };
            if let Some(m) = sample.memory_bytes {
                self.workload_memory.get_or_create(&label).set(m as i64);
            }
            if let Some(c) = sample.cores {
                self.workload_cpu.get_or_create(&label).set(c);
            }
            if let Some(d) = sample.disk_bytes {
                self.workload_disk.get_or_create(&label).set(d as i64);
            }
        }
        let process = crate::host::process();
        if let Some(rss) = process.rss_bytes {
            self.daemon_rss.set(rss as i64);
        }
        if let Some(cpu) = process.cpu_seconds {
            self.daemon_cpu.set(cpu);
        }
        self.fleet_contact_age.set(manager.fleet_contact_age().unwrap_or(-1.0));
        self.fleet_lease.set(manager.lease_remaining().map_or(-1.0, |d| d.as_secs_f64()));
        self.snapshot_bytes.set(manager.snapshot_bytes().unwrap_or(0) as i64);
        let mut out = String::new();
        encode(&mut out, &self.registry).expect("writing to a String");
        out
    }
}

/// A method as the `http_requests` label: the usual ones keep their names, and any other is
/// `other`.
fn method_label(method: &Method) -> &'static str {
    match *method {
        Method::GET => "GET",
        Method::PUT => "PUT",
        Method::POST => "POST",
        Method::DELETE => "DELETE",
        Method::HEAD => "HEAD",
        Method::OPTIONS => "OPTIONS",
        Method::PATCH => "PATCH",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_usual_methods_are_named_in_a_label() {
        assert_eq!(method_label(&Method::GET), "GET");
        assert_eq!(method_label(&Method::PATCH), "PATCH");
        assert_eq!(method_label(&Method::from_bytes(b"FOO").unwrap()), "other");
        assert_eq!(method_label(&Method::from_bytes(b"get").unwrap()), "other", "methods are case-sensitive");
    }
}
