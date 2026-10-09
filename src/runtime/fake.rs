//! An in-memory runtime for tests: blocklyd's logic (idempotency, ports, reconciliation,
//! auth) is exercised without Docker. It imitates the Docker behaviour blocklyd depends on,
//! including the awkward parts: port clashes surface at start, not create; a stop of a stopped
//! container succeeds; a removed container is simply gone.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::{self, BoxStream, StreamExt};
use time::OffsetDateTime;
use tokio::sync::broadcast;

use super::{
    ContainerInfo, ContainerRuntime, ContainerSpec, ContainerStatus, ExecOutput, LogChunk, LogOptions, LogStream,
    NetworkReport, RawStats, RuntimeError, RuntimeEvent, RuntimeInfo, format_time,
};

#[derive(Clone, Debug)]
pub struct FakeContainer {
    pub(crate) id: String,
    pub spec: ContainerSpec,
    pub status: ContainerStatus,
    pub(crate) exit_code: i64,
    pub(crate) oom_killed: bool,
    pub(crate) started_at: Option<OffsetDateTime>,
    pub(crate) finished_at: Option<OffsetDateTime>,
    pub(crate) restart_count: u32,
    pub(crate) logs: Vec<(LogStream, String)>,
    /// Ignores the stop signal, so a stop ends in a kill (exit 137).
    pub(crate) stubborn: bool,
}

pub struct FakeRuntime {
    containers: Mutex<HashMap<String, FakeContainer>>,
    available: AtomicBool,
    next_id: AtomicU64,
    events: broadcast::Sender<RuntimeEvent>,
    pub calls: Mutex<Vec<String>>,
    pub(crate) images: Mutex<Vec<String>>,
    /// What `info` answers while the daemon is up.
    pub(crate) runtime_info: Mutex<RuntimeInfo>,
    /// What `network_isolated` answers: None for a network that doesn't exist.
    pub(crate) network_isolated: Mutex<Option<bool>>,
    /// Execs wait here, once called, before they look for their container: a test that holds it
    /// for writing acts between blocklyd's checks and the exec itself.
    pub exec_gate: tokio::sync::RwLock<()>,
    /// When set, a followed log stays open once it has sent what the log holds, as Docker's does
    /// while a container runs and prints nothing. Each one open holds a clone of `follows`.
    pub hold_follows: AtomicBool,
    pub follows: std::sync::Arc<()>,
}

impl Default for FakeRuntime {
    fn default() -> Self {
        Self::new()
    }
}

impl FakeRuntime {
    pub fn new() -> Self {
        Self {
            containers: Mutex::new(HashMap::new()),
            available: AtomicBool::new(true),
            next_id: AtomicU64::new(1),
            events: broadcast::channel(256).0,
            calls: Mutex::new(Vec::new()),
            images: Mutex::new(Vec::new()),
            runtime_info: Mutex::new(RuntimeInfo { version: Some("fake".into()), ..Default::default() }),
            network_isolated: Mutex::new(Some(true)),
            exec_gate: tokio::sync::RwLock::new(()),
            hold_follows: AtomicBool::new(false),
            follows: std::sync::Arc::new(()),
        }
    }

    pub fn set_available(&self, up: bool) {
        self.available.store(up, Ordering::SeqCst);
    }

    fn up(&self) -> Result<(), RuntimeError> {
        if self.available.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(RuntimeError::Unavailable("fake daemon is down".into()))
        }
    }

    fn call(&self, what: impl Into<String>) {
        self.calls.lock().unwrap().push(what.into());
    }

    pub fn count_calls(&self, prefix: &str) -> usize {
        self.calls.lock().unwrap().iter().filter(|c| c.starts_with(prefix)).count()
    }

    pub fn container(&self, name: &str) -> Option<FakeContainer> {
        self.containers.lock().unwrap().get(name).cloned()
    }

    fn emit(&self, container: &FakeContainer, action: &str) {
        let _ = self.events.send(RuntimeEvent {
            container_id: container.id.clone(),
            action: action.into(),
            exit_code: Some(container.exit_code),
            at: OffsetDateTime::now_utc(),
        });
    }

    /// The workload exits on its own (a crash, or the kernel's OOM killer).
    pub fn crash(&self, name: &str, code: i64, oom: bool) {
        let mut all = self.containers.lock().unwrap();
        let c = all.get_mut(name).expect("container exists");
        c.status = ContainerStatus::Exited;
        c.exit_code = code;
        c.oom_killed = oom;
        c.finished_at = Some(OffsetDateTime::now_utc());
        let snapshot = c.clone();
        drop(all);
        self.emit(&snapshot, "die");
    }

    /// Someone removed the container behind blocklyd's back.
    pub fn vanish(&self, name: &str) {
        self.containers.lock().unwrap().remove(name);
    }

    /// Adds a line to a container's log.
    pub fn log(&self, name: &str, stream: LogStream, line: &str) {
        if let Some(c) = self.containers.lock().unwrap().get_mut(name) {
            c.logs.push((stream, line.into()));
        }
    }

    pub fn make_stubborn(&self, name: &str) {
        self.containers.lock().unwrap().get_mut(name).expect("exists").stubborn = true;
    }

    fn info(c: &FakeContainer) -> ContainerInfo {
        ContainerInfo {
            id: c.id.clone(),
            name: c.spec.name.clone(),
            labels: c.spec.labels.clone(),
            env: c.spec.env.iter().cloned().collect(),
            status: c.status,
            exit_code: c.exit_code,
            oom_killed: c.oom_killed,
            started_at: c.started_at,
            finished_at: c.finished_at,
            restart_count: c.restart_count,
            health: None,
        }
    }
}

#[async_trait]
impl ContainerRuntime for FakeRuntime {
    async fn info(&self) -> Result<RuntimeInfo, RuntimeError> {
        self.up()?;
        Ok(self.runtime_info.lock().unwrap().clone())
    }

    async fn ensure_network(
        &self,
        name: &str,
        _labels: &BTreeMap<String, String>,
    ) -> Result<NetworkReport, RuntimeError> {
        self.up()?;
        self.call(format!("network {name}"));
        Ok(NetworkReport { isolated: true })
    }

    async fn network_isolated(&self, _name: &str) -> Result<Option<bool>, RuntimeError> {
        self.up()?;
        Ok(*self.network_isolated.lock().unwrap())
    }

    async fn ensure_image(&self, image: &str, _timeout: Duration) -> Result<bool, RuntimeError> {
        self.up()?;
        let mut images = self.images.lock().unwrap();
        if images.iter().any(|i| i == image) {
            return Ok(false);
        }
        images.push(image.into());
        self.call(format!("pull {image}"));
        Ok(true)
    }

    async fn create(&self, spec: &ContainerSpec) -> Result<String, RuntimeError> {
        self.up()?;
        self.call(format!("create {}", spec.name));
        let mut all = self.containers.lock().unwrap();
        if all.contains_key(&spec.name) {
            return Err(RuntimeError::Conflict(format!("the name {} is in use", spec.name)));
        }
        let id = format!("{:064x}", self.next_id.fetch_add(1, Ordering::SeqCst));
        let container = FakeContainer {
            id: id.clone(),
            spec: spec.clone(),
            status: ContainerStatus::Created,
            exit_code: 0,
            oom_killed: false,
            started_at: None,
            finished_at: None,
            restart_count: 0,
            logs: Vec::new(),
            stubborn: false,
        };
        all.insert(spec.name.clone(), container);
        Ok(id)
    }

    async fn start(&self, name: &str) -> Result<(), RuntimeError> {
        self.up()?;
        self.call(format!("start {name}"));
        let mut all = self.containers.lock().unwrap();
        let taken: Vec<(super::super::protocol::Proto, u16)> = all
            .values()
            .filter(|c| c.spec.name != name && c.status == ContainerStatus::Running)
            .flat_map(|c| c.spec.ports.iter().map(|p| (p.protocol, p.host_port)))
            .collect();
        let c = all.get_mut(name).ok_or_else(|| RuntimeError::NotFound(name.into()))?;
        if c.status == ContainerStatus::Running {
            return Ok(());
        }
        if let Some(port) = c.spec.ports.iter().find(|p| taken.contains(&(p.protocol, p.host_port))) {
            return Err(RuntimeError::Rejected {
                status: 500,
                message: format!("Bind for 0.0.0.0:{} failed: port is already allocated", port.host_port),
            });
        }
        c.status = ContainerStatus::Running;
        c.started_at = Some(OffsetDateTime::now_utc());
        c.restart_count = 0;
        c.oom_killed = false;
        c.exit_code = 0;
        c.logs.push((LogStream::Stdout, "started".into()));
        let snapshot = c.clone();
        drop(all);
        self.emit(&snapshot, "start");
        Ok(())
    }

    async fn stop(&self, name: &str, _signal: &str, _grace: Duration) -> Result<(), RuntimeError> {
        self.up()?;
        self.call(format!("stop {name}"));
        let mut all = self.containers.lock().unwrap();
        let c = all.get_mut(name).ok_or_else(|| RuntimeError::NotFound(name.into()))?;
        if c.status != ContainerStatus::Running && c.status != ContainerStatus::Restarting {
            return Ok(());
        }
        c.status = ContainerStatus::Exited;
        c.exit_code = if c.stubborn { 137 } else { 0 };
        c.finished_at = Some(OffsetDateTime::now_utc());
        let snapshot = c.clone();
        drop(all);
        self.emit(&snapshot, "die");
        Ok(())
    }

    async fn kill(&self, name: &str) -> Result<(), RuntimeError> {
        self.up()?;
        self.call(format!("kill {name}"));
        let mut all = self.containers.lock().unwrap();
        let c = all.get_mut(name).ok_or_else(|| RuntimeError::NotFound(name.into()))?;
        if c.status == ContainerStatus::Running {
            c.status = ContainerStatus::Exited;
            c.exit_code = 137;
            c.finished_at = Some(OffsetDateTime::now_utc());
        }
        Ok(())
    }

    async fn disable_restart(&self, name: &str) -> Result<(), RuntimeError> {
        self.up()?;
        self.call(format!("disable_restart {name}"));
        // Nothing to switch off: every container is made with Docker's restart policy "no".
        if self.containers.lock().unwrap().contains_key(name) {
            Ok(())
        } else {
            Err(RuntimeError::NotFound(name.into()))
        }
    }

    async fn remove(&self, name: &str) -> Result<(), RuntimeError> {
        self.up()?;
        self.call(format!("remove {name}"));
        self.containers.lock().unwrap().remove(name);
        Ok(())
    }

    async fn inspect(&self, name: &str) -> Result<Option<ContainerInfo>, RuntimeError> {
        self.up()?;
        let all = self.containers.lock().unwrap();
        Ok(all.get(name).or_else(|| all.values().find(|c| c.id == name)).map(Self::info))
    }

    async fn list(&self, label_filters: &[String]) -> Result<Vec<ContainerInfo>, RuntimeError> {
        self.up()?;
        let all = self.containers.lock().unwrap();
        let matches = |c: &FakeContainer| {
            label_filters.iter().all(|f| match f.split_once('=') {
                Some((k, v)) => c.spec.labels.get(k).map(String::as_str) == Some(v),
                None => c.spec.labels.contains_key(f),
            })
        };
        Ok(all.values().filter(|c| matches(c)).map(Self::info).collect())
    }

    async fn exec(
        &self,
        container: &str,
        command: &[String],
        timeout: Duration,
        max_output: usize,
    ) -> Result<ExecOutput, RuntimeError> {
        self.up()?;
        self.call(format!("exec {container} {}", command.join(" ")));
        {
            let _open = self.exec_gate.read().await;
            // By id, as Docker takes it (a name works too, as it does there).
            let all = self.containers.lock().unwrap();
            let c = all
                .get(container)
                .or_else(|| all.values().find(|c| c.id == container))
                .ok_or_else(|| RuntimeError::NotFound(container.into()))?;
            if c.status != ContainerStatus::Running {
                return Err(RuntimeError::Conflict(format!("container {container} is not running")));
            }
        }
        // `sleep N` sleeps for real, so timeouts can be tested; anything else echoes its argv.
        if command.first().map(String::as_str) == Some("sleep") {
            let secs: f64 = command.get(1).and_then(|s| s.parse().ok()).unwrap_or(0.0);
            if Duration::from_secs_f64(secs) > timeout {
                tokio::time::sleep(timeout).await;
                return Ok(ExecOutput { timed_out: true, killed: true, ..Default::default() });
            }
            tokio::time::sleep(Duration::from_secs_f64(secs)).await;
            return Ok(ExecOutput { exit_code: Some(0), ..Default::default() });
        }
        let out = command.join(" ").into_bytes();
        let truncated = out.len() > max_output;
        Ok(ExecOutput {
            exit_code: Some(0),
            stdout: out[..out.len().min(max_output)].to_vec(),
            stdout_truncated: truncated,
            ..Default::default()
        })
    }

    fn logs(&self, name: &str, options: LogOptions) -> BoxStream<'static, Result<LogChunk, RuntimeError>> {
        if let Err(e) = self.up() {
            return stream::iter([Err(e)]).boxed();
        }
        let lines = self.containers.lock().unwrap().get(name).map(|c| c.logs.clone());
        let Some(lines) = lines else {
            return stream::iter([Err(RuntimeError::NotFound(name.into()))]).boxed();
        };
        let skip = options.tail.map_or(0, |n| lines.len().saturating_sub(n as usize));
        let now = format_time(OffsetDateTime::now_utc());
        let logged = stream::iter(
            lines
                .into_iter()
                .skip(skip)
                .map(move |(stream, line)| Ok(LogChunk { stream, bytes: Bytes::from(format!("{now} {line}\n")) })),
        );
        if !(options.follow && self.hold_follows.load(Ordering::SeqCst)) {
            return logged.boxed();
        }
        let open = self.follows.clone();
        logged
            .chain(stream::once(async move {
                let _open = open;
                std::future::pending().await
            }))
            .boxed()
    }

    async fn stats(&self, name: &str) -> Result<Option<RawStats>, RuntimeError> {
        self.up()?;
        let all = self.containers.lock().unwrap();
        Ok(all.get(name).filter(|c| c.status == ContainerStatus::Running).map(|c| RawStats {
            cpu_total_ns: Some(1_000_000_000),
            cpu_throttled_periods: Some(0),
            memory_usage: Some(512 * 1024 * 1024),
            memory_inactive_file: Some(64 * 1024 * 1024),
            memory_limit: Some(c.spec.memory_bytes as u64),
            pids: Some(42),
            pids_limit: Some(c.spec.pids_limit as u64),
            rx_bytes: Some(1000),
            tx_bytes: Some(2000),
        }))
    }

    fn events(&self, _label_filters: &[String]) -> BoxStream<'static, Result<RuntimeEvent, RuntimeError>> {
        let receiver = self.events.subscribe();
        stream::unfold(receiver, |mut receiver| async move {
            loop {
                match receiver.recv().await {
                    Ok(event) => return Some((Ok(event), receiver)),
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        })
        .boxed()
    }
}
