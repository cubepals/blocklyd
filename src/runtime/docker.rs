//! Docker Engine API over the local unix socket (bollard). blocklyd is the only client that
//! needs the socket, which is root-equivalent: whoever holds it can start a privileged container
//! with the host's filesystem mounted. blocklyd never exposes it; it exposes the narrow
//! workload protocol instead, and fills every container's isolation from host policy.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::time::Duration;

use async_trait::async_trait;
use bollard::Docker;
use bollard::container::LogOutput;
use bollard::errors::Error as BollardError;
use bollard::exec::{CreateExecOptions, StartExecOptions, StartExecResults};
use bollard::models::{
    ContainerCreateBody, ContainerInspectResponse, ContainerStateStatusEnum, ContainerUpdateBody, HealthStatusEnum,
    HostConfig, HostConfigLogConfig, Mount, MountType, NetworkCreateRequest, PortBinding as DockerPortBinding,
    RestartPolicy as DockerRestartPolicy, RestartPolicyNameEnum,
};
use bollard::query_parameters::{
    CreateContainerOptionsBuilder, CreateImageOptionsBuilder, EventsOptionsBuilder, InspectNetworkOptions,
    KillContainerOptionsBuilder, ListContainersOptionsBuilder, LogsOptionsBuilder, RemoveContainerOptionsBuilder,
    StatsOptionsBuilder, StopContainerOptionsBuilder,
};
use futures_util::stream::{BoxStream, StreamExt};
use time::OffsetDateTime;

use super::{
    ContainerInfo, ContainerRuntime, ContainerSpec, ContainerStatus, ExecOutput, LogChunk, LogOptions, LogStream,
    NetworkReport, RawStats, RuntimeError, RuntimeEvent, RuntimeInfo, parse_time,
};
use crate::protocol::Health;

/// Longer than the longest stop (600 s): Docker answers a stop only once the container exited.
const CLIENT_TIMEOUT_SECS: u64 = 660;
/// Quick calls get a much shorter leash of their own.
const QUICK: Duration = Duration::from_secs(20);
const ICC_OPTION: &str = "com.docker.network.bridge.enable_icc";
/// How long a timed-out exec's process is looked for. Docker answers the start before the process
/// exists, and reports pid 0 until it does.
const EXEC_PID_WAIT: Duration = Duration::from_secs(2);
const EXEC_PID_STEP: Duration = Duration::from_millis(50);

pub struct DockerRuntime {
    socket: String,
    client: std::sync::Mutex<Option<Docker>>,
}

impl DockerRuntime {
    /// Doesn't connect yet: blocklyd starts even while Docker is down (bollard refuses to build a
    /// client for a socket that doesn't exist yet), and says so until it's back.
    pub fn new(socket: &Path) -> Result<Self, RuntimeError> {
        let socket = socket.to_str().ok_or_else(|| RuntimeError::Other("socket path is not UTF-8".into()))?.to_owned();
        Ok(Self { socket, client: std::sync::Mutex::new(None) })
    }

    /// The client, made on first use once the socket exists. Cheap to clone (shared inside).
    fn docker(&self) -> Result<Docker, RuntimeError> {
        let mut client = self.client.lock().unwrap();
        if let Some(docker) = client.as_ref() {
            return Ok(docker.clone());
        }
        let docker = Docker::connect_with_unix(&self.socket, CLIENT_TIMEOUT_SECS, bollard::API_DEFAULT_VERSION)
            .map_err(map_err)?;
        *client = Some(docker.clone());
        Ok(docker)
    }

    /// The host pid of an exec and its container's id. Pid 0 means not started yet, so it looks
    /// again for a short while; None if no process showed up, or the exec ended without one.
    async fn exec_pid(&self, exec_id: &str) -> Option<(i32, String)> {
        let deadline = tokio::time::Instant::now() + EXEC_PID_WAIT;
        loop {
            let inspect = quick("inspect exec", self.docker().ok()?.inspect_exec(exec_id)).await.ok()?;
            let pid = inspect.pid.unwrap_or(0);
            if pid > 0 {
                return Some((i32::try_from(pid).ok()?, inspect.container_id?));
            }
            if inspect.exit_code.is_some() || tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(EXEC_PID_STEP).await;
        }
    }
}

fn map_err(error: BollardError) -> RuntimeError {
    match error {
        BollardError::DockerResponseServerError { status_code: 404, message } => RuntimeError::NotFound(message),
        BollardError::DockerResponseServerError { status_code: 409, message } => RuntimeError::Conflict(message),
        BollardError::DockerResponseServerError { status_code, message } => {
            RuntimeError::Rejected { status: status_code, message }
        }
        BollardError::RequestTimeoutError => RuntimeError::Timeout("the Docker API didn't answer in time".into()),
        e @ (BollardError::IOError { .. }
        | BollardError::HyperResponseError { .. }
        | BollardError::HyperLegacyError { .. }
        | BollardError::SocketNotFoundError(_)) => RuntimeError::Unavailable(e.to_string()),
        other => RuntimeError::Other(other.to_string()),
    }
}

async fn quick<T>(
    what: &str,
    fut: impl std::future::Future<Output = Result<T, BollardError>>,
) -> Result<T, RuntimeError> {
    match tokio::time::timeout(QUICK, fut).await {
        Ok(result) => result.map_err(map_err),
        Err(_) => Err(RuntimeError::Timeout(format!("{what} took longer than {}s", QUICK.as_secs()))),
    }
}

fn filters(label_filters: &[String]) -> HashMap<String, Vec<String>> {
    HashMap::from([("label".to_owned(), label_filters.to_vec())])
}

/// The tag to pull a reference by: "latest" for a name with neither tag nor digest, which is what
/// inspect reads it as, where Docker would otherwise pull every tag. Only then: a tag passed with
/// one already named replaces it. A registry's port ("reg:5000/x") is no tag, so only the last
/// path segment is looked at.
fn pull_tag(image: &str) -> Option<&'static str> {
    let last = image.rsplit('/').next().unwrap_or(image);
    (!image.contains('@') && !last.contains(':')).then_some("latest")
}

fn info_from(inspect: ContainerInspectResponse) -> ContainerInfo {
    let state = inspect.state.unwrap_or_default();
    let config = inspect.config.unwrap_or_default();
    let status = match state.status {
        Some(ContainerStateStatusEnum::CREATED) => ContainerStatus::Created,
        Some(ContainerStateStatusEnum::RUNNING) => ContainerStatus::Running,
        Some(ContainerStateStatusEnum::PAUSED) => ContainerStatus::Paused,
        Some(ContainerStateStatusEnum::RESTARTING) => ContainerStatus::Restarting,
        Some(ContainerStateStatusEnum::REMOVING) => ContainerStatus::Removing,
        Some(ContainerStateStatusEnum::EXITED) => ContainerStatus::Exited,
        Some(ContainerStateStatusEnum::DEAD) => ContainerStatus::Dead,
        _ => ContainerStatus::Unknown,
    };
    let health = state.health.and_then(|h| h.status).and_then(|s| match s {
        HealthStatusEnum::STARTING => Some(Health::Starting),
        HealthStatusEnum::HEALTHY => Some(Health::Healthy),
        HealthStatusEnum::UNHEALTHY => Some(Health::Unhealthy),
        _ => None,
    });
    let env = config
        .env
        .unwrap_or_default()
        .into_iter()
        .filter_map(|kv| kv.split_once('=').map(|(k, v)| (k.to_owned(), v.to_owned())))
        .collect();
    ContainerInfo {
        id: inspect.id.unwrap_or_default(),
        name: inspect.name.unwrap_or_default().trim_start_matches('/').to_owned(),
        labels: config.labels.unwrap_or_default().into_iter().collect(),
        env,
        status,
        exit_code: state.exit_code.unwrap_or(0),
        oom_killed: state.oom_killed.unwrap_or(false),
        started_at: parse_time(state.started_at.as_deref()),
        finished_at: parse_time(state.finished_at.as_deref()),
        restart_count: inspect.restart_count.unwrap_or(0).max(0) as u32,
        health,
    }
}

fn body_of(spec: &ContainerSpec) -> ContainerCreateBody {
    let mut exposed = Vec::new();
    let mut bindings: HashMap<String, Option<Vec<DockerPortBinding>>> = HashMap::new();
    for port in &spec.ports {
        let key = format!("{}/{}", port.container_port, port.protocol.as_str());
        exposed.push(key.clone());
        let host = port
            .host_ips
            .iter()
            .map(|ip| DockerPortBinding { host_ip: Some(ip.to_string()), host_port: Some(port.host_port.to_string()) })
            .collect();
        bindings.insert(key, Some(host));
    }
    let tmpfs = spec.read_only_rootfs.then(|| {
        // exec: the JVM's native transports and some mods load libraries from /tmp, and the data
        // mount is executable anyway, so noexec here would break things and protect nothing.
        HashMap::from([("/tmp".to_owned(), format!("rw,exec,nosuid,nodev,size={}m", spec.tmp_size_mb))])
    });
    let host_config = HostConfig {
        memory: Some(spec.memory_bytes),
        // Equal to memory: no swap. A JVM that swaps is a server that lags.
        memory_swap: Some(spec.memory_bytes),
        nano_cpus: spec.nano_cpus,
        cpu_shares: Some(spec.cpu_shares),
        pids_limit: Some(spec.pids_limit),
        readonly_rootfs: Some(spec.read_only_rootfs),
        tmpfs,
        cap_drop: Some(vec!["ALL".into()]),
        security_opt: Some(vec!["no-new-privileges:true".into()]),
        privileged: Some(false),
        ipc_mode: Some("private".into()),
        init: Some(false),
        // Docker never restarts a workload by itself: its on-failure also fires after an unclean
        // reboot, before blocklyd runs, which would bring back a fenced copy. blocklyd applies the
        // spec's policy itself (`restart_supervisor`).
        restart_policy: Some(DockerRestartPolicy { name: Some(RestartPolicyNameEnum::NO), maximum_retry_count: None }),
        mounts: Some(vec![Mount {
            typ: Some(MountType::BIND),
            source: Some(spec.data_dir.to_string_lossy().into_owned()),
            target: Some(spec.mount_path.clone()),
            read_only: Some(false),
            ..Default::default()
        }]),
        port_bindings: Some(bindings),
        network_mode: Some(spec.network.clone()),
        // Rotated and bounded: a chatty server can't fill the disk with logs.
        log_config: Some(HostConfigLogConfig {
            typ: Some("local".into()),
            config: Some(HashMap::from([
                ("max-size".into(), format!("{}m", spec.log_max_size_mb)),
                ("max-file".into(), spec.log_max_files.to_string()),
            ])),
        }),
        oom_score_adj: Some(spec.oom_score_adj as i64),
        ..Default::default()
    };
    ContainerCreateBody {
        image: Some(spec.image.clone()),
        entrypoint: spec.entrypoint.clone(),
        env: Some(spec.env.iter().map(|(k, v)| format!("{k}={v}")).collect()),
        labels: Some(spec.labels.clone().into_iter().collect()),
        user: Some(spec.user.clone()),
        exposed_ports: Some(exposed),
        stop_signal: Some(spec.stop_signal.clone()),
        stop_timeout: Some(spec.stop_timeout_secs as i64),
        host_config: Some(host_config),
        ..Default::default()
    }
}

#[async_trait]
impl ContainerRuntime for DockerRuntime {
    async fn info(&self) -> Result<RuntimeInfo, RuntimeError> {
        let version = quick("version", self.docker()?.version()).await?;
        let info = quick("info", self.docker()?.info()).await?;
        Ok(RuntimeInfo {
            version: version.version,
            api_version: version.api_version,
            cgroup_version: info.cgroup_version.map(|v| v.to_string()),
            cgroup_driver: info.cgroup_driver.map(|v| v.to_string()),
            storage_driver: info.driver,
            security_options: info.security_options.unwrap_or_default(),
            live_restore: info.live_restore_enabled,
            log_driver: info.logging_driver,
        })
    }

    async fn ensure_network(
        &self,
        name: &str,
        labels: &BTreeMap<String, String>,
    ) -> Result<NetworkReport, RuntimeError> {
        if let Some(isolated) = self.network_isolated(name).await? {
            return Ok(NetworkReport { isolated });
        }
        let request = NetworkCreateRequest {
            name: name.to_owned(),
            driver: Some("bridge".into()),
            // Workloads can't reach each other directly; each reaches the world and is reached
            // through its published ports only.
            options: Some(HashMap::from([(ICC_OPTION.to_owned(), "false".to_owned())])),
            labels: Some(labels.clone().into_iter().collect()),
            ..Default::default()
        };
        match quick("create network", self.docker()?.create_network(request)).await {
            Ok(_) => Ok(NetworkReport { isolated: true }),
            // Made by someone else in between, maybe without isolation: look again.
            Err(RuntimeError::Conflict(message)) => match self.network_isolated(name).await? {
                Some(isolated) => Ok(NetworkReport { isolated }),
                None => Err(RuntimeError::Conflict(message)),
            },
            Err(e) => Err(e),
        }
    }

    async fn network_isolated(&self, name: &str) -> Result<Option<bool>, RuntimeError> {
        match quick("inspect network", self.docker()?.inspect_network(name, None::<InspectNetworkOptions>)).await {
            Ok(network) => {
                Ok(Some(network.options.unwrap_or_default().get(ICC_OPTION).map(String::as_str) == Some("false")))
            }
            Err(RuntimeError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn ensure_image(&self, image: &str, timeout: Duration) -> Result<bool, RuntimeError> {
        match quick("inspect image", self.docker()?.inspect_image(image)).await {
            Ok(_) => return Ok(false),
            Err(RuntimeError::NotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let mut options = CreateImageOptionsBuilder::new().from_image(image);
        if let Some(tag) = pull_tag(image) {
            options = options.tag(tag);
        }
        let options = options.build();
        let pull = async {
            let mut stream = self.docker()?.create_image(Some(options), None, None);
            while let Some(item) = stream.next().await {
                // bollard hands an errorDetail in the progress stream over as a DockerStreamError.
                item.map_err(|e| match e {
                    BollardError::DockerStreamError { error } => {
                        RuntimeError::Other(format!("pulling {image}: {error}"))
                    }
                    other => map_err(other),
                })?;
            }
            Ok(())
        };
        match tokio::time::timeout(timeout, pull).await {
            Ok(result) => result.map(|()| true),
            Err(_) => Err(RuntimeError::Timeout(format!("pulling {image} took longer than {}s", timeout.as_secs()))),
        }
    }

    async fn create(&self, spec: &ContainerSpec) -> Result<String, RuntimeError> {
        let options = CreateContainerOptionsBuilder::new().name(&spec.name).build();
        let created = quick("create", self.docker()?.create_container(Some(options), body_of(spec))).await?;
        Ok(created.id)
    }

    async fn start(&self, name: &str) -> Result<(), RuntimeError> {
        quick("start", self.docker()?.start_container(name, None)).await
    }

    async fn stop(&self, name: &str, signal: &str, grace: Duration) -> Result<(), RuntimeError> {
        let options =
            StopContainerOptionsBuilder::new().signal(signal).t(grace.as_secs().min(i32::MAX as u64) as i32).build();
        // Docker answers once the container exited: the grace, plus a margin for the kill.
        let budget = grace + Duration::from_secs(30);
        match tokio::time::timeout(budget, self.docker()?.stop_container(name, Some(options))).await {
            Ok(result) => result.map_err(map_err),
            Err(_) => Err(RuntimeError::Timeout(format!("stopping {name} took longer than {}s", budget.as_secs()))),
        }
    }

    async fn kill(&self, name: &str) -> Result<(), RuntimeError> {
        let options = KillContainerOptionsBuilder::new().signal("SIGKILL").build();
        match quick("kill", self.docker()?.kill_container(name, Some(options))).await {
            // 409: not running. Nothing left to kill.
            Err(RuntimeError::Conflict(_)) => Ok(()),
            other => other,
        }
    }

    async fn disable_restart(&self, name: &str) -> Result<(), RuntimeError> {
        let body = ContainerUpdateBody {
            restart_policy: Some(DockerRestartPolicy {
                name: Some(RestartPolicyNameEnum::NO),
                maximum_retry_count: None,
            }),
            ..Default::default()
        };
        quick("update", self.docker()?.update_container(name, body)).await
    }

    async fn remove(&self, name: &str) -> Result<(), RuntimeError> {
        // v: anonymous volumes an image declares go with the container instead of leaking; the
        // workload's data is a bind mount, which removal never touches.
        let options = RemoveContainerOptionsBuilder::new().force(true).v(true).build();
        match quick("remove", self.docker()?.remove_container(name, Some(options))).await {
            Err(RuntimeError::NotFound(_)) => Ok(()),
            other => other,
        }
    }

    async fn inspect(&self, name: &str) -> Result<Option<ContainerInfo>, RuntimeError> {
        match quick("inspect", self.docker()?.inspect_container(name, None)).await {
            Ok(inspect) => Ok(Some(info_from(inspect))),
            Err(RuntimeError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn list(&self, label_filters: &[String]) -> Result<Vec<ContainerInfo>, RuntimeError> {
        let options = ListContainersOptionsBuilder::new().all(true).filters(&filters(label_filters)).build();
        let summaries = quick("list", self.docker()?.list_containers(Some(options))).await?;
        let mut out = Vec::with_capacity(summaries.len());
        for summary in summaries {
            let Some(id) = summary.id else { continue };
            // The summary has no exit code or health; inspect is local and cheap.
            if let Some(info) = self.inspect(&id).await? {
                out.push(info);
            }
        }
        Ok(out)
    }

    async fn exec(
        &self,
        container: &str,
        command: &[String],
        timeout: Duration,
        max_output: usize,
    ) -> Result<ExecOutput, RuntimeError> {
        let options = CreateExecOptions::<String> {
            cmd: Some(command.to_vec()),
            attach_stdout: Some(true),
            attach_stderr: Some(true),
            attach_stdin: Some(false),
            tty: Some(false),
            privileged: Some(false),
            ..Default::default()
        };
        let created = quick("create exec", self.docker()?.create_exec(container, options)).await?;
        let exec_id = created.id;
        let started = quick(
            "start exec",
            self.docker()?
                .start_exec(&exec_id, Some(StartExecOptions { detach: false, tty: false, output_capacity: None })),
        )
        .await?;
        let StartExecResults::Attached { mut output, .. } = started else {
            return Err(RuntimeError::Other("exec started detached".into()));
        };

        let mut result = ExecOutput::default();
        let collect = async {
            while let Some(item) = output.next().await {
                let (buffer, truncated, bytes) = match item.map_err(map_err)? {
                    LogOutput::StdOut { message } => (&mut result.stdout, &mut result.stdout_truncated, message),
                    LogOutput::StdErr { message } => (&mut result.stderr, &mut result.stderr_truncated, message),
                    _ => continue,
                };
                let room = max_output.saturating_sub(buffer.len());
                buffer.extend_from_slice(&bytes[..bytes.len().min(room)]);
                *truncated |= bytes.len() > room;
            }
            Ok::<(), RuntimeError>(())
        };
        match tokio::time::timeout(timeout, collect).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(_) => {
                result.timed_out = true;
                // Docker has no API to stop an exec (moby/moby#35703). From the host, blocklyd
                // can: find its pid, make sure it is still this container's, and kill its tree.
                if let Some((pid, container)) = self.exec_pid(&exec_id).await {
                    result.killed = kill_exec_tree(pid, &container) > 0;
                }
            }
        }
        drop(output);
        // The exit code lands a moment after the stream ends.
        for _ in 0..50 {
            let inspect = quick("inspect exec", self.docker()?.inspect_exec(&exec_id)).await?;
            if inspect.running != Some(true) {
                result.exit_code = inspect.exit_code;
                break;
            }
            if result.timed_out && !result.killed {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok(result)
    }

    fn logs(&self, name: &str, options: LogOptions) -> BoxStream<'static, Result<LogChunk, RuntimeError>> {
        let mut builder = LogsOptionsBuilder::new().stdout(true).stderr(true).timestamps(true).follow(options.follow);
        builder = builder.tail(&options.tail.map_or_else(|| "all".to_owned(), |n| n.to_string()));
        if let Some(since) = options.since_unix {
            builder = builder.since(since.clamp(0, i32::MAX as i64) as i32);
        }
        let docker = match self.docker() {
            Ok(docker) => docker,
            Err(e) => return futures_util::stream::iter([Err(e)]).boxed(),
        };
        let stream = docker.logs(name, Some(builder.build()));
        stream
            .filter_map(|item| async move {
                match item {
                    Ok(LogOutput::StdOut { message }) => {
                        Some(Ok(LogChunk { stream: LogStream::Stdout, bytes: message }))
                    }
                    Ok(LogOutput::StdErr { message }) => {
                        Some(Ok(LogChunk { stream: LogStream::Stderr, bytes: message }))
                    }
                    Ok(LogOutput::Console { message }) => {
                        Some(Ok(LogChunk { stream: LogStream::Stdout, bytes: message }))
                    }
                    Ok(LogOutput::StdIn { .. }) => None,
                    Err(e) => Some(Err(map_err(e))),
                }
            })
            .boxed()
    }

    async fn stats(&self, name: &str) -> Result<Option<RawStats>, RuntimeError> {
        let options = StatsOptionsBuilder::new().stream(false).one_shot(true).build();
        let mut stream = self.docker()?.stats(name, Some(options));
        let first = match tokio::time::timeout(QUICK, stream.next()).await {
            Ok(Some(Ok(stats))) => stats,
            Ok(Some(Err(e))) => {
                return match map_err(e) {
                    RuntimeError::NotFound(_) => Ok(None),
                    other => Err(other),
                };
            }
            Ok(None) => return Ok(None),
            Err(_) => return Err(RuntimeError::Timeout("stats".into())),
        };
        let cpu = first.cpu_stats.unwrap_or_default();
        let memory = first.memory_stats.unwrap_or_default();
        let stats = memory.stats.unwrap_or_default();
        // cgroup v2 names it inactive_file, v1 total_inactive_file.
        let inactive = stats.get("inactive_file").or_else(|| stats.get("total_inactive_file")).copied();
        let (rx, tx) = first
            .networks
            .unwrap_or_default()
            .values()
            .fold((0u64, 0u64), |(rx, tx), n| (rx + n.rx_bytes.unwrap_or(0), tx + n.tx_bytes.unwrap_or(0)));
        let pids = first.pids_stats.unwrap_or_default();
        Ok(Some(RawStats {
            cpu_total_ns: cpu.cpu_usage.and_then(|u| u.total_usage),
            cpu_throttled_periods: cpu.throttling_data.and_then(|t| t.throttled_periods),
            memory_usage: memory.usage,
            memory_inactive_file: inactive,
            memory_limit: memory.limit,
            pids: pids.current,
            pids_limit: pids.limit,
            rx_bytes: Some(rx),
            tx_bytes: Some(tx),
        }))
    }

    fn events(&self, label_filters: &[String]) -> BoxStream<'static, Result<RuntimeEvent, RuntimeError>> {
        let mut filters = filters(label_filters);
        filters.insert("type".into(), vec!["container".into()]);
        let options = EventsOptionsBuilder::new().filters(&filters).build();
        let docker = match self.docker() {
            Ok(docker) => docker,
            Err(e) => return futures_util::stream::iter([Err(e)]).boxed(),
        };
        docker
            .events(Some(options))
            .map(|item| {
                let message = item.map_err(map_err)?;
                let actor = message.actor.unwrap_or_default();
                let at = message
                    .time_nano
                    .and_then(|ns| OffsetDateTime::from_unix_timestamp_nanos(ns as i128).ok())
                    .unwrap_or_else(OffsetDateTime::now_utc);
                Ok(RuntimeEvent {
                    container_id: actor.id.unwrap_or_default(),
                    action: message.action.unwrap_or_default(),
                    exit_code: actor.attributes.as_ref().and_then(|a| a.get("exitCode")).and_then(|c| c.parse().ok()),
                    at,
                })
            })
            .boxed()
    }
}

/// SIGKILLs `pid` and every process descended from it, each only if its cgroup still names
/// `container` (a pid may have been reused since the exec began). Returns how many it killed.
fn kill_exec_tree(pid: i32, container: &str) -> usize {
    let mut killed = 0;
    for target in descendants(pid, &children_by_parent()) {
        let owned = std::fs::read_to_string(format!("/proc/{target}/cgroup")).is_ok_and(|c| c.contains(container));
        if owned
            && let Some(p) = rustix::process::Pid::from_raw(target)
            && rustix::process::kill_process(p, rustix::process::Signal::KILL).is_ok()
        {
            killed += 1;
        }
    }
    killed
}

/// Every process's children, keyed by parent pid, as /proc shows them now.
fn children_by_parent() -> HashMap<i32, Vec<i32>> {
    let mut parents: HashMap<i32, Vec<i32>> = HashMap::new();
    if let Ok(entries) = std::fs::read_dir("/proc") {
        for entry in entries.filter_map(Result::ok) {
            let Ok(child) = entry.file_name().to_string_lossy().parse::<i32>() else { continue };
            let Ok(stat) = std::fs::read_to_string(format!("/proc/{child}/stat")) else { continue };
            // The command name is parenthesised and may hold spaces; fields resume after ')'.
            let Some(after) = stat.rfind(')').map(|i| &stat[i + 1..]) else { continue };
            let Some(ppid) = after.split_whitespace().nth(1).and_then(|p| p.parse::<i32>().ok()) else { continue };
            parents.entry(ppid).or_default().push(child);
        }
    }
    parents
}

/// `root` and everything below it, breadth first. Nothing for 0 or 1: the tree under either is
/// every process on the host, the container's own included, and the cgroup check would let
/// exactly those through. Docker reports pid 0 for an exec it hasn't started yet.
fn descendants(root: i32, children: &HashMap<i32, Vec<i32>>) -> Vec<i32> {
    if root <= 1 {
        return Vec::new();
    }
    let mut tree = vec![root];
    let mut i = 0;
    while i < tree.len() {
        if let Some(below) = children.get(&tree[i]) {
            tree.extend(below);
        }
        i += 1;
    }
    tree
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> ContainerSpec {
        ContainerSpec {
            name: "blockly-test-w".into(),
            image: "alpine:3.22".into(),
            entrypoint: None,
            env: Vec::new(),
            labels: BTreeMap::new(),
            user: "1000:1000".into(),
            memory_bytes: 256 * 1024 * 1024,
            nano_cpus: None,
            cpu_shares: 1024,
            pids_limit: 4096,
            read_only_rootfs: true,
            tmp_size_mb: 64,
            data_dir: "/var/lib/blocklyd/workloads/w/data".into(),
            mount_path: "/data".into(),
            ports: Vec::new(),
            stop_signal: "SIGTERM".into(),
            stop_timeout_secs: 90,
            network: "blockly-workloads".into(),
            log_max_size_mb: 10,
            log_max_files: 3,
            oom_score_adj: 500,
        }
    }

    #[test]
    fn docker_is_never_asked_to_restart_a_workload() {
        let host = body_of(&spec()).host_config.unwrap();
        assert_eq!(
            host.restart_policy,
            Some(DockerRestartPolicy { name: Some(RestartPolicyNameEnum::NO), maximum_retry_count: None })
        );
    }

    #[test]
    fn only_a_reference_without_tag_or_digest_is_pulled_as_latest() {
        for (image, tag) in [
            ("repo", Some("latest")),
            ("a/b/c", Some("latest")),
            ("reg:5000/repo", Some("latest")),
            ("repo:1", None),
            ("reg:5000/repo:1", None),
            ("repo@sha256:abc", None),
            ("reg:5000/repo@sha256:abc", None),
        ] {
            assert_eq!(pull_tag(image), tag, "{image}");
        }
    }

    #[test]
    fn the_tree_walk_never_starts_from_0_or_1() {
        let children = HashMap::from([(0, vec![1, 2]), (1, vec![100, 200]), (100, vec![101]), (101, vec![102])]);
        assert!(descendants(0, &children).is_empty());
        assert!(descendants(1, &children).is_empty());
        assert!(descendants(-1, &children).is_empty());
        assert_eq!(descendants(100, &children), vec![100, 101, 102]);
        assert_eq!(descendants(200, &children), vec![200], "a leaf is its own tree");
    }

    #[test]
    fn killing_from_0_or_1_signals_nothing() {
        // An id no cgroup names, so even a walk that did start could kill nothing on this host.
        let container = "0".repeat(64);
        assert_eq!(kill_exec_tree(0, &container), 0);
        assert_eq!(kill_exec_tree(1, &container), 0);
        assert_eq!(kill_exec_tree(-1, &container), 0);
    }
}
