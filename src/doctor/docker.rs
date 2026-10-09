//! Whether Docker is reachable and set up the way blocklyd expects: cgroup v2, live-restore on,
//! workloads' network isolated, logs bounded, and `/etc/docker/daemon.json` matching Blockly's
//! (`deploy/daemon.json`). All warnings: blocklyd starts without Docker, outlives its restarts and
//! reports it as down.

use std::path::Path;
use std::time::Duration;

use serde_json::{Map, Value};

use super::check::Check;
use crate::runtime::{ContainerRuntime, RuntimeError, RuntimeInfo};

/// How long doctor waits on Docker. Short: a hung daemon shouldn't hold up blocklyd's start.
const DOCKER_WAIT: Duration = Duration::from_secs(5);

/// Blockly's Docker daemon settings, as `deploy/` ships them.
pub(crate) const DEPLOY_DAEMON_JSON: &str = include_str!("../../deploy/daemon.json");

pub(crate) fn deploy_daemon_json() -> Map<String, Value> {
    serde_json::from_str(DEPLOY_DAEMON_JSON).expect("deploy/daemon.json is a JSON object")
}

/// `daemon.json` as it is on disk: Ok(None) when there is none.
pub(crate) fn read_daemon_json(path: &Path) -> Result<Option<Map<String, Value>>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            serde_json::from_str(&text).map(Some).map_err(|e| format!("{} isn't a JSON object: {e}", path.display()))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(format!("can't read {}: {e}", path.display())),
    }
}

async fn waited<T>(call: impl Future<Output = Result<T, RuntimeError>>) -> Result<T, RuntimeError> {
    tokio::time::timeout(DOCKER_WAIT, call)
        .await
        .unwrap_or_else(|_| Err(RuntimeError::Timeout(format!("Docker didn't answer in {}s", DOCKER_WAIT.as_secs()))))
}

pub(crate) async fn checks(runtime: &dyn ContainerRuntime, network: &str, daemon_json: &Path) -> Vec<Check> {
    let on_disk = read_daemon_json(daemon_json);
    let mut checks = Vec::new();
    match waited(runtime.info()).await {
        Err(e) => checks.push(Check::warn(
            "docker",
            format!("can't reach Docker ({e}); blocklyd starts anyway and reports Docker as down"),
            "Start Docker (systemctl start docker), or install it from Docker's apt repository",
        )),
        Ok(info) => {
            checks.push(Check::pass(
                "docker",
                format!(
                    "Docker {} (API {})",
                    info.version.as_deref().unwrap_or("?"),
                    info.api_version.as_deref().unwrap_or("?")
                ),
            ));
            checks.push(cgroup(&info));
            checks.push(live_restore(&info));
            checks.push(isolation(network, waited(runtime.network_isolated(network)).await));
            checks.push(log_driver(&info, on_disk.as_ref().ok().and_then(Option::as_ref)));
        }
    }
    checks.push(compare(daemon_json, &on_disk));
    checks
}

fn cgroup(info: &RuntimeInfo) -> Check {
    match info.cgroup_version.as_deref() {
        Some("2") => Check::pass("docker.cgroup", "cgroup v2"),
        other => Check::warn(
            "docker.cgroup",
            format!(
                "Docker reports cgroup {}: blocklyd's memory figures assume v2",
                other.map_or("version unknown".to_owned(), |v| format!("v{v}"))
            ),
            "Boot with cgroup v2 (the default on current distributions; systemd.unified_cgroup_hierarchy=1)",
        ),
    }
}

fn live_restore(info: &RuntimeInfo) -> Check {
    if info.live_restore == Some(true) {
        return Check::pass("docker.live_restore", "live-restore is on: servers outlive a Docker restart");
    }
    Check::warn(
        "docker.live_restore",
        "live-restore is off: every server stops when Docker restarts",
        "Run blocklyd doctor --fix on a host with no servers yet, or set \"live-restore\": true in /etc/docker/daemon.json \
         and reload Docker (systemctl reload docker)",
    )
}

fn isolation(network: &str, isolated: Result<Option<bool>, RuntimeError>) -> Check {
    match isolated {
        Ok(Some(true)) => Check::pass("docker.network", format!("{network} keeps servers from reaching each other")),
        Ok(None) => Check::pass(
            "docker.network",
            format!("{network} doesn't exist yet; blocklyd makes it with inter-container traffic off"),
        ),
        Ok(Some(false)) => Check::warn(
            "docker.network",
            format!("{network} lets servers reach each other (enable_icc is not false)"),
            format!(
                "Once no server is on it, remove it (docker network rm {network}); blocklyd makes it again, isolated"
            ),
        ),
        Err(e) => Check::warn(
            "docker.network",
            format!("couldn't look at {network}: {e}"),
            "Check that Docker answers (docker network ls)",
        ),
    }
}

/// blocklyd's own containers always rotate their logs; this is for everything else on the host.
fn log_driver(info: &RuntimeInfo, on_disk: Option<&Map<String, Value>>) -> Check {
    let driver = info.log_driver.as_deref().unwrap_or("unknown");
    // daemon.json's log-opts are Docker's only while the driver it names is the one running: a
    // file written since Docker started isn't applied yet.
    let applied = on_disk.is_some_and(|d| d.get("log-driver").and_then(Value::as_str).unwrap_or("json-file") == driver);
    let max_size = applied && on_disk.and_then(|d| d.get("log-opts")).and_then(|o| o.get("max-size")).is_some();
    match driver {
        "local" | "journald" | "none" => Check::pass("docker.log_driver", format!("log driver {driver} is bounded")),
        "json-file" if max_size => Check::pass("docker.log_driver", "log driver json-file, with a max-size"),
        _ => Check::warn(
            "docker.log_driver",
            format!("log driver {driver} may not be bounded: containers blocklyd didn't make can fill the disk"),
            "Set \"log-driver\": \"local\" in /etc/docker/daemon.json (blocklyd doctor --fix does)",
        ),
    }
}

/// Settings in Blockly's daemon.json that the host's lacks or sets otherwise.
pub(crate) fn differences(on_disk: &Map<String, Value>) -> Vec<String> {
    deploy_daemon_json().into_iter().filter(|(k, v)| on_disk.get(k) != Some(v)).map(|(k, _)| k).collect()
}

fn compare(path: &Path, on_disk: &Result<Option<Map<String, Value>>, String>) -> Check {
    let fix = format!(
        "Run blocklyd doctor --fix on a host with no servers yet; it merges deploy/daemon.json into {}",
        path.display()
    );
    match on_disk {
        Err(e) => Check::warn("docker.daemon_json", e.clone(), format!("Correct {}", path.display())),
        Ok(None) => Check::warn(
            "docker.daemon_json",
            format!("{} doesn't exist: Docker runs on its defaults", path.display()),
            fix,
        ),
        Ok(Some(settings)) => match differences(settings).as_slice() {
            [] => Check::pass(
                "docker.daemon_json",
                format!("{} has every setting deploy/daemon.json has", path.display()),
            ),
            keys => Check::warn(
                "docker.daemon_json",
                format!("{} differs from deploy/daemon.json in {}", path.display(), keys.join(", ")),
                fix,
            ),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doctor::Status;
    use crate::runtime::fake::FakeRuntime;

    fn by_name<'a>(checks: &'a [Check], name: &str) -> &'a Check {
        checks.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no {name} in {checks:?}"))
    }

    fn healthy() -> FakeRuntime {
        let fake = FakeRuntime::new();
        *fake.runtime_info.lock().unwrap() = RuntimeInfo {
            version: Some("29.0".into()),
            api_version: Some("1.52".into()),
            cgroup_version: Some("2".into()),
            live_restore: Some(true),
            log_driver: Some("local".into()),
            ..Default::default()
        };
        fake
    }

    #[tokio::test]
    async fn a_host_set_up_like_deploy_passes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.json");
        std::fs::write(&path, DEPLOY_DAEMON_JSON).unwrap();
        let checks = checks(&healthy(), "blockly-workloads", &path).await;
        assert!(checks.iter().all(|c| c.status == Status::Pass), "{checks:?}");
        assert_eq!(checks.len(), 6);
    }

    #[tokio::test]
    async fn docker_down_is_a_warning_and_daemon_json_is_still_read() {
        let fake = healthy();
        fake.set_available(false);
        let dir = tempfile::tempdir().unwrap();
        let checks = checks(&fake, "blockly-workloads", &dir.path().join("daemon.json")).await;
        let names: Vec<_> = checks.iter().map(|c| (c.name, c.status)).collect();
        assert_eq!(names, [("docker", Status::Warn), ("docker.daemon_json", Status::Warn)]);
        assert!(checks.iter().all(|c| c.fix.is_some()));
    }

    #[tokio::test]
    async fn live_restore_off_cgroup_v1_and_shared_network_warn() {
        let fake = healthy();
        {
            let mut info = fake.runtime_info.lock().unwrap();
            info.live_restore = Some(false);
            info.cgroup_version = Some("1".into());
            info.log_driver = Some("json-file".into());
        }
        *fake.network_isolated.lock().unwrap() = Some(false);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.json");
        std::fs::write(&path, r#"{"live-restore": false, "log-driver": "json-file"}"#).unwrap();
        let checks = checks(&fake, "blockly-workloads", &path).await;
        for name in
            ["docker.live_restore", "docker.cgroup", "docker.network", "docker.log_driver", "docker.daemon_json"]
        {
            assert_eq!(by_name(&checks, name).status, Status::Warn, "{name}");
        }
        assert!(by_name(&checks, "docker.daemon_json").detail.contains("live-restore"));
        assert!(
            by_name(&checks, "docker.network").fix.as_deref().unwrap().contains("docker network rm blockly-workloads")
        );
    }

    #[tokio::test]
    async fn a_network_blocklyd_hasnt_made_yet_passes_and_json_file_with_max_size_is_bounded() {
        let fake = healthy();
        *fake.network_isolated.lock().unwrap() = None;
        fake.runtime_info.lock().unwrap().log_driver = Some("json-file".into());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.json");
        std::fs::write(&path, r#"{"log-opts": {"max-size": "10m"}}"#).unwrap();
        let checks = checks(&fake, "blockly-workloads", &path).await;
        assert_eq!(by_name(&checks, "docker.network").status, Status::Pass);
        assert_eq!(by_name(&checks, "docker.log_driver").status, Status::Pass);
    }

    #[test]
    fn an_unreadable_daemon_json_says_so() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("daemon.json");
        std::fs::write(&path, "{not json").unwrap();
        let check = compare(&path, &read_daemon_json(&path));
        assert_eq!(check.status, Status::Warn);
        assert!(check.detail.contains("isn't a JSON object"));
    }
}
