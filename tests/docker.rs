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

use blocklyd::manager::{Manager, NodeError, Precondition, Records};
use blocklyd::protocol::{DataDisposition, EnsureOutcome, ExecRequest, WorkloadSpec, WorkloadState};
use blocklyd::runtime::ContainerRuntime;
use blocklyd::runtime::docker::DockerRuntime;
use support::{id, manager_on, manager_probed};

const SOCKET: &str = "/var/run/docker.sock";

fn docker() -> Arc<dyn ContainerRuntime> {
    Arc::new(DockerRuntime::new(std::path::Path::new(SOCKET)).unwrap())
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
        let filter = format!("label=blocklyd.deployment={}", self.0);
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
        "ports": [{ "name": "game", "containerPort": 25565, "audience": ["edge", "control"] }],
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
    // Above `extra()`'s 44000..47999, so no other test's daemon can be holding these ports.
    let squatter = TcpListener::bind("127.0.0.1:48000").unwrap();
    let probe = blocklyd::ports::bind_probe(vec!["127.0.0.1".parse().unwrap()]);
    let text = format!(
        "deployment_id = \"{dep}\"\n[network]\nport_range = [48000, 48010]\nport_quarantine_seconds = 0\n{HOST_OWNERSHIP}"
    );
    let m = manager_probed(dir.path(), docker(), &text, probe);
    m.reconcile(Records::FromDisk).await;
    let made = m.ensure(id("p"), shell(SERVER, 256), Precondition::None, None).await.unwrap();
    assert_eq!(made.workload.ports[0].host_port, 48001, "48000 is someone else's");
    assert!(m.start(id("p"), None).await.unwrap().changed);
    drop(squatter);
    cleanup(&m, &["p"]).await;
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
