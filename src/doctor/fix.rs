//! `doctor --fix`: the fixes that are safe to make without asking, on a host with no blocklyd
//! servers yet. It writes Docker's daemon.json from `deploy/daemon.json` (merged over the one there,
//! which it keeps as a `.bak`) and makes the state directory. Each change says how to undo it.
//!
//! Docker is restarted only while no container at all exists; with containers it is reloaded,
//! which applies live-restore and stops nothing, and the rest waits for a restart the operator
//! chooses.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::{Map, Value};

use super::docker::{deploy_daemon_json, differences, read_daemon_json};
use super::state::has_workloads;
use crate::config::Config;
use crate::manager::LABEL_MANAGED;
use crate::runtime::ContainerRuntime;
use crate::store::Store;

/// Something `--fix` did, and how to take it back.
#[derive(Clone, Debug, Serialize)]
pub struct Change {
    pub done: String,
    pub undo: String,
}

/// What `--fix` did, and what is left for the operator.
#[derive(Clone, Debug, Default, Serialize)]
pub struct Fixed {
    pub changes: Vec<Change>,
    pub next: Vec<String>,
}

pub async fn apply(config: &Config, runtime: &dyn ContainerRuntime, daemon_json: &Path) -> Fixed {
    let mut fixed = Fixed::default();
    let servers = runtime.list(&[format!("{LABEL_MANAGED}=true")]).await;
    if has_workloads(&config.state_dir) || servers.as_ref().is_ok_and(|servers| !servers.is_empty()) {
        fixed.next.push(
            "Nothing changed: this host already runs blocklyd servers, and --fix changes only a host without any"
                .into(),
        );
        return fixed;
    }
    match daemon_settings(daemon_json) {
        Err(e) => fixed.next.push(format!("{e}; daemon.json left as it is")),
        Ok(None) => {}
        Ok(Some(change)) => {
            fixed.changes.push(change);
            apply_to_docker(runtime, servers.is_ok(), &mut fixed).await;
        }
    }
    if !config.state_dir.exists() {
        let dir = &config.state_dir;
        match Store::open(dir) {
            Ok(_) => fixed.changes.push(Change {
                done: format!("made {} (mode 0700)", dir.display()),
                undo: format!("rm -r {}", dir.display()),
            }),
            Err(e) => fixed.next.push(format!("Couldn't make the state directory: {e}")),
        }
    }
    fixed
}

/// `on_disk` with every setting of Blockly's daemon.json laid over it.
pub fn merged(on_disk: Option<Map<String, Value>>) -> Map<String, Value> {
    let mut settings = on_disk.unwrap_or_default();
    settings.extend(deploy_daemon_json());
    settings
}

/// Writes the merged daemon.json when it differs, keeping the old one beside it.
fn daemon_settings(path: &Path) -> Result<Option<Change>, String> {
    let on_disk = read_daemon_json(path)?;
    let keys = match &on_disk {
        Some(settings) => differences(settings),
        None => deploy_daemon_json().keys().cloned().collect(),
    };
    if keys.is_empty() {
        return Ok(None);
    }
    let backup = on_disk.as_ref().map(|_| backup_path(path));
    let text = serde_json::to_string_pretty(&Value::Object(merged(on_disk))).expect("serializes") + "\n";
    let write = || -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if let Some(backup) = &backup {
            std::fs::copy(path, backup)?;
        }
        // Whole and on disk before Docker is restarted to read it, at the mode Docker's own is.
        crate::durable::write_atomic(path, text.as_bytes(), 0o644).map_err(|e| e.source)
    };
    write().map_err(|e| format!("couldn't write {}: {e}", path.display()))?;
    Ok(Some(match backup {
        Some(backup) => Change {
            done: format!("set {} in {} (the old file is {})", keys.join(", "), path.display(), backup.display()),
            undo: format!("mv {} {}", backup.display(), path.display()),
        },
        None => Change {
            done: format!("wrote {} from deploy/daemon.json", path.display()),
            undo: format!("rm {}", path.display()),
        },
    }))
}

/// `daemon.json.bak`, or a dated one beside it when that is taken: an earlier backup is the
/// original, and is never overwritten.
fn backup_path(path: &Path) -> PathBuf {
    let plain = path.with_extension("json.bak");
    if !plain.exists() {
        return plain;
    }
    path.with_extension(format!("json.bak.{}", crate::manager::now().unix_timestamp()))
}

/// Docker reads daemon.json when it starts. With no container at all a restart costs nothing;
/// with any, a reload applies live-restore and the rest waits.
async fn apply_to_docker(runtime: &dyn ContainerRuntime, reachable: bool, fixed: &mut Fixed) {
    if !reachable {
        fixed.next.push("Start Docker (systemctl start docker); it reads the new daemon.json when it starts".into());
        return;
    }
    let containers = runtime.list(&[]).await.map(|all| all.len());
    let (verb, then) = match containers {
        Ok(0) => ("restart", None),
        Ok(n) => (
            "reload",
            Some(format!(
                "{n} container(s) are running, so Docker was reloaded, not restarted: live-restore is on now, and the \
                 other settings apply at Docker's next restart. Restart it (systemctl restart docker) once no \
                 container runs"
            )),
        ),
        Err(_) => ("reload", Some("Restart Docker (systemctl restart docker) once no container runs".to_owned())),
    };
    let done = std::process::Command::new("systemctl")
        .args([verb, "docker"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if done {
        fixed.changes.push(Change {
            done: format!("ran systemctl {verb} docker to apply it"),
            undo: format!("undo daemon.json as above, then systemctl {verb} docker"),
        });
    } else {
        fixed.next.push(format!("systemctl {verb} docker didn't work; {verb} Docker yourself to apply daemon.json"));
    }
    fixed.next.extend(then);
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;
    use crate::runtime::fake::FakeRuntime;

    fn config(state_dir: &Path) -> Config {
        toml::from_str(&format!(
            "node_id = \"n1\"\ndeployment_id = \"dev\"\nstate_dir = \"{}\"\n[api]\nlisten = \"127.0.0.1:7443\"\n\
             [api.tls]\ncert = \"c\"\nkey = \"k\"\nclient_ca = \"ca\"\nallowed_clients = [\"x\"]\n",
            state_dir.display()
        ))
        .unwrap()
    }

    #[test]
    fn merging_keeps_the_hosts_own_settings_and_lays_blocklys_over_them() {
        let on_disk: Map<String, Value> =
            serde_json::from_str(r#"{"live-restore": false, "data-root": "/srv/docker"}"#).unwrap();
        let settings = merged(Some(on_disk));
        assert_eq!(settings["live-restore"], Value::Bool(true));
        assert_eq!(settings["data-root"], "/srv/docker");
        assert!(differences(&settings).is_empty());
    }

    #[test]
    fn daemon_json_is_written_with_a_backup_and_left_alone_once_it_matches() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docker/daemon.json");
        let change = daemon_settings(&path).unwrap().expect("written");
        assert_eq!(change.undo, format!("rm {}", path.display()));
        assert_eq!(std::fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o644);
        assert!(daemon_settings(&path).unwrap().is_none(), "nothing left to change");

        std::fs::write(&path, r#"{"live-restore": false}"#).unwrap();
        let change = daemon_settings(&path).unwrap().expect("written");
        let backup = dir.path().join("docker/daemon.json.bak");
        assert_eq!(std::fs::read_to_string(&backup).unwrap(), r#"{"live-restore": false}"#);
        assert!(change.done.contains("live-restore"), "{}", change.done);
        assert!(change.undo.starts_with("mv "));
        std::fs::write(&path, "{}").unwrap();
        daemon_settings(&path).unwrap();
        assert_eq!(std::fs::read_to_string(&backup).unwrap(), r#"{"live-restore": false}"#, "the first backup stays");
        let mut beside: Vec<_> = std::fs::read_dir(dir.path().join("docker"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| !name.starts_with("daemon.json.bak."))
            .collect();
        beside.sort();
        assert_eq!(beside, ["daemon.json", "daemon.json.bak"], "no file it staged is left");
    }

    #[tokio::test]
    async fn a_host_with_servers_is_left_alone() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        Store::open(&state).unwrap();
        std::fs::create_dir(state.join("workloads/w")).unwrap();
        let daemon_json = dir.path().join("daemon.json");
        let fixed = apply(&config(&state), &FakeRuntime::new(), &daemon_json).await;
        assert!(fixed.changes.is_empty());
        assert!(fixed.next[0].starts_with("Nothing changed"));
        assert!(!daemon_json.exists());
    }

    #[tokio::test]
    async fn a_fresh_host_gets_its_state_dir_and_daemon_json() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let fake = FakeRuntime::new();
        fake.set_available(false); // so nothing tries to restart this machine's Docker
        let fixed = apply(&config(&state), &fake, &dir.path().join("daemon.json")).await;
        assert_eq!(fixed.changes.len(), 2, "{fixed:?}");
        assert!(state.is_dir());
        assert!(fixed.next[0].starts_with("Start Docker"));
    }
}
