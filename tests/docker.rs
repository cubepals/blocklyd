// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Against a real Docker daemon, with small alpine workloads. Ignored by default so `cargo test`
//! never needs Docker:
//!
//!     cargo test --test docker -- --ignored --test-threads 2
//!
//! Each test uses its own deployment id, so they don't see each other's containers, and removes
//! what it made.

mod support;

use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};

use blocklyd::config::DockerConfig;
use blocklyd::labels::LABEL_DEPLOYMENT;
use blocklyd::manager::{ConflictCode, Manager, NodeError, Precondition, Records};
use blocklyd::protocol::{DataDisposition, EnsureOutcome, ExecRequest, WorkloadSpec, WorkloadState};
use blocklyd::runtime::ContainerRuntime;
use blocklyd::runtime::docker::DockerRuntime;
use support::{id, manager_on, manager_probed};

fn docker() -> Arc<dyn ContainerRuntime> {
    Arc::new(DockerRuntime::new(&DockerConfig::default().socket).unwrap())
}

/// A test's own deployment id, which carries its port slot for `extra`. Slots are handed out in
/// order, so two daemons running at once never share a range, as a random slot could.
fn deployment() -> String {
    static NEXT_SLOT: AtomicU16 = AtomicU16::new(0);
    let slot = NEXT_SLOT.fetch_add(1, Ordering::Relaxed);
    assert!(slot < 400, "44000..47999 holds 400 slots of 10 ports");
    format!("t{slot:03}{}", &uuid::Uuid::new_v4().simple().to_string()[..7])
}

/// Removes every container of a test's deployment when the test ends, passed or panicked.
struct Cleanup(String);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let filter = format!("label={LABEL_DEPLOYMENT}={}", self.0);
        if let Ok(out) = std::process::Command::new("docker").args(["ps", "-aq", "--filter", &filter]).output() {
            let ids: Vec<String> = String::from_utf8_lossy(&out.stdout).split_whitespace().map(str::to_owned).collect();
            if !ids.is_empty() {
                let _ = std::process::Command::new("docker").arg("rm").arg("-f").args(&ids).output();
            }
        }
    }
}

/// Each test is a separate daemon on one Docker host, so each gets its own ports: two allocators
/// over one range collide, and Docker only says so at start ("port is already allocated"). That
/// is the reason there is one blocklyd per host; these tests found it by accident first.
/// As on a host: each workload's data belongs to the workloads' user (the fixture otherwise gives
/// it to the test's own).
const HOST_OWNERSHIP: &str = "[workloads]\ndata_owner = \"1000:1000\"\n";

fn extra(deployment: &str) -> String {
    let slot: u16 = deployment[1..4].parse().unwrap();
    let low = 44000 + slot * 10;
    format!(
        "deployment_id = \"{deployment}\"\n[network]\nport_range = [{low}, {}]\nport_quarantine_seconds = 0\n{HOST_OWNERSHIP}",
        low + 9
    )
}

fn daemon(dir: &std::path::Path, dep: &str) -> Arc<Manager> {
    let probe = blocklyd::ports::bind_probe(vec!["127.0.0.1".parse().unwrap()]);
    let manager = manager_probed(dir, docker(), &extra(dep), probe);
    // As `serve` does: blocklyd, not Docker, applies restart policies.
    tokio::spawn(manager.clone().restart_supervisor(tokio_util::sync::CancellationToken::new()));
    manager
}

/// A shell workload that saves on SIGTERM, like Blockly's image does.
fn shell(script: &str, memory_mb: u32) -> WorkloadSpec {
    serde_json::from_value(serde_json::json!({
        "image": "alpine:3.22",
        "entrypoint": ["/bin/sh", "-c", script],
        "secrets": { "RCON_PASSWORD": "docker-test-secret" },
        "resources": { "memoryMb": memory_mb, "cpuMillis": 500 },
        "storage": { "mountPath": "/data", "sizeGb": 1 },
        "ports": [{ "name": "game", "containerPort": blocklyd::minecraft::GAME_PORT, "audience": ["edge", "control"] }],
        "stop": { "timeoutSeconds": 10 },
        "restart": { "policy": "on-failure", "maxRetries": 2 }
    }))
    .unwrap()
}

const SERVER: &str = "trap 'echo saving; echo saved > /data/saved; exit 0' TERM; echo booting; \
     while true; do sleep 0.2; done";

async fn eventually(
    manager: &Arc<Manager>,
    id: &str,
    what: &str,
    f: impl Fn(&blocklyd::protocol::WorkloadView) -> bool,
) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        manager.stats(&support::id(id)).await.ok();
        let view = manager.view(&support::id(id)).unwrap();
        if f(&view) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}: {view:#?}");
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

async fn cleanup(manager: &Arc<Manager>, ids: &[&str]) {
    for w in ids {
        let _ = manager.delete(id(w), DataDisposition::Delete, Some((*w).to_owned()), None).await;
    }
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn lifecycle_hardening_and_graceful_stop() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let m = daemon(dir.path(), &dep);
    assert!(m.reconcile(Records::FromDisk).await.error.is_none());

    let made = m.ensure(id("w"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    assert_eq!(made.outcome, EnsureOutcome::Created);
    assert_eq!(
        m.ensure(id("w"), shell(SERVER, 256), Precondition::None, None).await.unwrap().outcome,
        EnsureOutcome::Unchanged
    );
    assert!(m.start(id("w"), None).await.unwrap().changed);
    assert!(!m.start(id("w"), None).await.unwrap().changed);

    // The isolation the host enforces, as the process inside sees it.
    let out = m
        .exec(
            id("w"),
            ExecRequest {
                command: vec![
                    "sh".into(),
                    "-c".into(),
                    "id -u; grep -E '^(CapEff|NoNewPrivs)' /proc/1/status; touch /etc/x 2>/dev/null && echo rootfs-writable || echo rootfs-readonly; cat /sys/fs/cgroup/pids.max 2>/dev/null || cat /sys/fs/cgroup/pids/pids.max".into(),
                ],
                timeout_seconds: 10,
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(out.exit_code, Some(0), "{out:?}");
    let lines: Vec<&str> = out.stdout.lines().collect();
    assert_eq!(lines[0], "1000", "never root");
    assert!(out.stdout.contains("CapEff:\t0000000000000000"), "no capabilities");
    assert!(out.stdout.contains("NoNewPrivs:\t1"));
    assert!(out.stdout.contains("rootfs-readonly"));
    assert!(out.stdout.contains("4096"), "pids limit");

    let started = Instant::now();
    let stopped = m.stop(id("w"), None, None).await.unwrap();
    assert!(stopped.changed && !stopped.forced, "{stopped:?}");
    assert_eq!(stopped.workload.state, WorkloadState::Stopped);
    assert!(started.elapsed() < Duration::from_secs(5), "SIGTERM was handled, not waited out");
    assert!(m.store.data_dir(&id("w")).join("saved").exists(), "the workload saved before it exited");
    assert!(!m.stop(id("w"), None, None).await.unwrap().changed);
    cleanup(&m, &["w"]).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn crashes_restart_by_policy_then_stay_crashed() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let m = daemon(dir.path(), &dep);
    m.reconcile(Records::FromDisk).await;
    // Fails three seconds in, every time: the host retries twice, then gives up.
    m.ensure(id("c"), shell("echo up; sleep 3; echo failing; exit 3", 256), Precondition::None, None).await.unwrap();
    m.start(id("c"), None).await.unwrap();
    eventually(&m, "c", "a policy restart", |v| v.restart_count >= 1 && v.last_failure_at.is_some()).await;
    eventually(&m, "c", "the policy to run out", |v| v.state == WorkloadState::Crashed).await;
    let view = m.view(&id("c")).unwrap();
    assert_eq!(view.exit.as_ref().unwrap().code, 3);
    assert_eq!(view.restart_count, 2);
    // A requested start begins a fresh run, with a fresh count.
    assert!(m.start(id("c"), None).await.unwrap().changed);
    assert_eq!(m.view(&id("c")).unwrap().restart_count, 0);
    cleanup(&m, &["c"]).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn the_memory_limit_is_hard_and_an_oom_kill_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let m = daemon(dir.path(), &dep);
    m.reconcile(Records::FromDisk).await;
    let mut spec = shell("sleep 1; head -c 400m /dev/zero | tail > /dev/null; sleep 60", 64);
    spec.restart.max_retries = 0;
    m.ensure(id("oom"), spec, Precondition::None, None).await.unwrap();
    m.start(id("oom"), None).await.unwrap();
    // `tail` buffers its whole input; the cgroup kills it at 64 MB. Whether the shell survives
    // decides running vs crashed; either way the kill is visible.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut saw_oom = false;
    while Instant::now() < deadline && !saw_oom {
        m.stats(&id("oom")).await.ok();
        let view = m.view(&id("oom")).unwrap();
        saw_oom = view.exit.as_ref().is_some_and(|e| e.oom_killed) || oom_events(&view.locate.container_name);
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(saw_oom, "the kernel's OOM kill inside the limit was observed");
    cleanup(&m, &["oom"]).await;
}

/// Docker counts OOM kills of any process in the container's cgroup as `oom` events; the
/// container itself only shows OOMKilled when its main process died of it.
fn oom_events(container: &str) -> bool {
    let out = std::process::Command::new("docker")
        .args([
            "events",
            "--since",
            "2m",
            "--until",
            "0s",
            "--filter",
            &format!("container={container}"),
            "--filter",
            "event=oom",
        ])
        .output();
    out.is_ok_and(|o| !o.stdout.is_empty())
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn an_exec_past_its_timeout_is_killed_inside_the_container() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let m = daemon(dir.path(), &dep);
    m.reconcile(Records::FromDisk).await;
    m.ensure(id("e"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    m.start(id("e"), None).await.unwrap();
    let started = Instant::now();
    let out = m
        .exec(
            id("e"),
            ExecRequest { command: vec!["sh".into(), "-c".into(), "sleep 60; echo never".into()], timeout_seconds: 2 },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(out.timed_out && out.killed, "{out:?}");
    assert!(started.elapsed() < Duration::from_secs(6));
    let left = m
        .exec(
            id("e"),
            ExecRequest {
                command: vec!["sh".into(), "-c".into(), "ps -o args | grep -c '^sleep 60' || true".into()],
                timeout_seconds: 5,
            },
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(left.stdout.trim(), "0", "the sleep went with its shell");
    cleanup(&m, &["e"]).await;
}

/// The shortest timeout there is. Until Docker has started an exec's process it reports pid 0, and
/// the process tree under 0 is the whole host: the kill must find the exec, never the workload.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_short_exec_timeout_kills_the_exec_and_leaves_the_workload_running() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let m = daemon(dir.path(), &dep);
    m.reconcile(Records::FromDisk).await;
    m.ensure(id("s"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    m.start(id("s"), None).await.unwrap();
    let before = m.view(&id("s")).unwrap();
    let out = m
        .exec(id("s"), ExecRequest { command: vec!["sleep".into(), "30".into()], timeout_seconds: 1 }, None, None)
        .await
        .unwrap();
    assert!(out.timed_out && out.killed, "{out:?}");
    m.stats(&id("s")).await.unwrap();
    let after = m.view(&id("s")).unwrap();
    assert_eq!(after.state, WorkloadState::Running, "{after:#?}");
    assert_eq!(after.locate.container_id, before.locate.container_id);
    assert_eq!(after.started_at, before.started_at, "the workload was never killed or restarted");
    cleanup(&m, &["s"]).await;
}

/// Like a Minecraft server: it holds its world in memory (64 MB in its tmpfs, counted against its
/// limit), and answers on the game port with a token it drew when it booted.
fn memory_server() -> String {
    format!(
        "head -c {WORLD_BYTES} /dev/zero > /tmp/heap; \
         head -c 8 /dev/urandom | od -An -tx1 | tr -d ' \\n' > /tmp/token; \
         nc -lk -p {} -e cat /tmp/token & trap 'exit 0' TERM; while true; do sleep 0.2; done",
        blocklyd::minecraft::GAME_PORT
    )
}

const WORLD_BYTES: u64 = 64 * 1024 * 1024;

/// What the workload answers on the game port, read within `within`; None if it doesn't.
async fn answer(port: u16, within: Duration) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        let read = async {
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.ok()?;
            let mut token = String::new();
            stream.read_to_string(&mut token).await.ok()?;
            Some(token).filter(|t| !t.is_empty())
        };
        if let Ok(Some(token)) = tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), read).await {
            return Some(token);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    None
}

async fn memory_bytes(m: &Arc<Manager>, w: &str) -> u64 {
    let stats = serde_json::to_value(m.stats(&id(w)).await.unwrap()).unwrap();
    stats["memoryBytes"].as_u64().unwrap_or_default()
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_suspended_server_keeps_its_memory_and_answers_within_a_second_of_start() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let m = daemon(dir.path(), &dep);
    assert!(m.reconcile(Records::FromDisk).await.error.is_none());
    m.ensure(id("z"), shell(&memory_server(), 256), Precondition::None, None).await.unwrap();
    let before = m.start(id("z"), None).await.unwrap().workload;
    let port = before.ports[0].host_port;
    let token = answer(port, Duration::from_secs(20)).await.expect("the server answers once booted");
    let held = memory_bytes(&m, "z").await;
    assert!(held >= WORLD_BYTES, "the world is in memory: {held}");

    let r = m.suspend(id("z"), None).await.unwrap();
    assert_eq!(r.workload.state, WorkloadState::Suspended);
    assert!(answer(port, Duration::from_millis(500)).await.is_none(), "frozen: it answers nothing");

    let asked = Instant::now();
    let r = m.start(id("z"), None).await.unwrap();
    assert_eq!(r.resumed, Some(true));
    let again = answer(port, Duration::from_secs(1)).await;
    assert!(asked.elapsed() < Duration::from_secs(1), "answered {:?} after the start", asked.elapsed());
    assert_eq!(again.as_deref(), Some(token.as_str()), "the same process, with what it held in memory");
    assert_eq!(r.workload.started_at, before.started_at, "thawed, never started again");
    assert_eq!(r.workload.locate.container_id, before.locate.container_id);
    assert!(memory_bytes(&m, "z").await >= WORLD_BYTES, "its world still in memory");

    // A stop still reaches it frozen, and it saves on the way out.
    m.suspend(id("z"), None).await.unwrap();
    let r = m.stop(id("z"), None, None).await.unwrap();
    assert!(!r.forced, "it handled the stop signal: {r:?}");
    assert_eq!(r.workload.state, WorkloadState::Stopped);
    cleanup(&m, &["z"]).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_restarted_daemon_adopts_running_workloads_without_touching_them() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let first = daemon(dir.path(), &dep);
    first.reconcile(Records::FromDisk).await;
    first.ensure(id("r"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    first.start(id("r"), None).await.unwrap();
    let before = first.view(&id("r")).unwrap();
    drop(first);

    let second = daemon(dir.path(), &dep);
    let report = second.reconcile(Records::FromDisk).await;
    assert!(report.error.is_none());
    let after = second.view(&id("r")).unwrap();
    assert_eq!(after.state, WorkloadState::Running);
    assert_eq!(after.locate.container_id, before.locate.container_id, "the same container, never restarted");
    assert_eq!(after.started_at, before.started_at);
    assert_eq!(after.ports, before.ports);
    cleanup(&second, &["r"]).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn workloads_cannot_reach_each_other() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let m = daemon(dir.path(), &dep);
    m.reconcile(Records::FromDisk).await;
    m.ensure(id("a"), shell("nc -lk -p 8080 -e echo hi & while true; do sleep 1; done", 256), Precondition::None, None)
        .await
        .unwrap();
    m.ensure(id("b"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    m.start(id("a"), None).await.unwrap();
    m.start(id("b"), None).await.unwrap();
    let ip = m
        .exec(id("a"), ExecRequest { command: vec!["hostname".into(), "-i".into()], timeout_seconds: 5 }, None, None)
        .await
        .unwrap()
        .stdout
        .trim()
        .to_owned();
    let own = m
        .exec(
            id("a"),
            ExecRequest { command: vec!["sh".into(), "-c".into(), format!("nc -w 2 {ip} 8080")], timeout_seconds: 10 },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(own.stdout.contains("hi"), "a reaches itself: the listener works ({own:?})");
    let across = m
        .exec(
            id("b"),
            ExecRequest { command: vec!["sh".into(), "-c".into(), format!("nc -w 2 {ip} 8080")], timeout_seconds: 10 },
            None,
            None,
        )
        .await
        .unwrap();
    assert!(!across.stdout.contains("hi"), "b must not reach a: {across:?}");
    cleanup(&m, &["a", "b"]).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn a_port_another_process_holds_is_skipped() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    // A port the kernel hands out: a fixed one can already be some outbound connection's.
    let squatter = TcpListener::bind("127.0.0.1:0").unwrap();
    let held = squatter.local_addr().unwrap().port();
    let probe = blocklyd::ports::bind_probe(vec!["127.0.0.1".parse().unwrap()]);
    let text = format!(
        "deployment_id = \"{dep}\"\n[network]\nport_range = [{held}, {}]\nport_quarantine_seconds = 0\n{HOST_OWNERSHIP}",
        held + 10
    );
    let m = manager_probed(dir.path(), docker(), &text, probe);
    m.reconcile(Records::FromDisk).await;
    let made = m.ensure(id("p"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    assert!(made.workload.ports[0].host_port > held, "{held} is someone else's");
    assert!(m.start(id("p"), None).await.unwrap().changed);
    drop(squatter);
    cleanup(&m, &["p"]).await;
}

/// Docker's own refusals, not the fake's: a host port another daemon's container holds, and one
/// another process holds, which no probe looked for. Each is the workload's port conflict.
#[tokio::test]
#[ignore = "needs Docker"]
async fn a_start_docker_refuses_for_a_taken_port_is_a_port_conflict() {
    let refused = |e: NodeError| matches!(e, NodeError::Conflict { code: ConflictCode::PortConflict, .. });
    let (dir, other_dir) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let (dep, other) = (deployment(), deployment());
    let (_cleanup, _other_cleanup) = (Cleanup(dep.clone()), Cleanup(other.clone()));
    // Two daemons over one range, the mistake tests/docker.rs's port slots exist to avoid.
    let first = daemon(dir.path(), &dep);
    first.reconcile(Records::FromDisk).await;
    first.ensure(id("a"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    assert!(first.start(id("a"), None).await.unwrap().changed);
    let second = manager_on(other_dir.path(), docker(), &extra(&dep).replace(&dep, &other));
    second.reconcile(Records::FromDisk).await;
    second.ensure(id("b"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    let err = second.start(id("b"), None).await.unwrap_err();
    assert!(refused(err), "a port another container holds");
    cleanup(&second, &["b"]).await;
    cleanup(&first, &["a"]).await;

    let squatter = TcpListener::bind("127.0.0.1:0").unwrap();
    let held = squatter.local_addr().unwrap().port();
    let text = format!(
        "deployment_id = \"{dep}\"\n[network]\nport_range = [{held}, {}]\nport_quarantine_seconds = 0\n{HOST_OWNERSHIP}",
        held + 10
    );
    let unprobed = manager_on(dir.path(), docker(), &text);
    unprobed.reconcile(Records::FromDisk).await;
    unprobed.ensure(id("p"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    let err = unprobed.start(id("p"), None).await.unwrap_err();
    assert!(refused(err), "a port another process holds");
    drop(squatter);
    cleanup(&unprobed, &["p"]).await;
}

#[tokio::test]
#[ignore = "needs Docker"]
async fn with_docker_gone_the_daemon_answers_and_says_why() {
    let dir = tempfile::tempdir().unwrap();
    let dep = deployment();
    let _cleanup = Cleanup(dep.clone());
    let missing = Arc::new(DockerRuntime::new(std::path::Path::new("/nonexistent/docker.sock")).unwrap());
    let m = manager_on(dir.path(), missing, &extra(&dep));
    let report = m.reconcile(Records::FromDisk).await;
    assert!(report.error.is_some());
    assert_eq!(m.health().await.status, "degraded");
    let started = Instant::now();
    let err = m.ensure(id("x"), shell(SERVER, 256), Precondition::None, None).await.unwrap_err();
    assert!(matches!(err, NodeError::RuntimeUnavailable(_)), "{err:?}");
    assert!(started.elapsed() < Duration::from_secs(2), "fails fast, doesn't hang");
}
