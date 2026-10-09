//! `blocklyd doctor`: looks at this host the way blocklyd will use it, and says what is wrong and
//! what to do about it. `check-config` only reads the file; doctor also asks Docker, the state
//! directory, the ports, the machine id and the clock.
//!
//! Every check passes, warns or fails, and anything short of a pass carries one instruction. A
//! failure is something blocklyd can't work without; a warning is something it survives. Doctor
//! never stops blocklyd from starting for anything blocklyd survives today: `--preflight` (the
//! unit's `ExecStartPre`) refuses only what `check-config` refused, plus a state directory blocklyd
//! can't own, and reports every other failure as a warning in the journal.
//!
//! Parts (`doctor/`):
//! - `check.rs`: one verdict: pass, warn or fail, with what to do.
//! - `config.rs`: whether the configuration file (and its certificates) is valid.
//! - `docker.rs`: whether Docker is reachable and configured the way blocklyd expects.
//! - `state.rs`: whether the state directory can be owned, locked, cloned into and has room.
//! - `network.rs`: whether the game ports are free and the listeners stay off public addresses.
//! - `host.rs`: whether the host has a machine id and a synchronised clock.
//! - `fix.rs`: the safe fixes `--fix` applies on a host with no workloads yet.
//!
//! Not here: anything that changes a host with workloads on it, and inferring a configuration
//! (`blocklyd join` writes one).

mod check;
mod config;
mod docker;
mod fix;
mod host;
mod network;
mod state;

use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::runtime::ContainerRuntime;
use crate::runtime::docker::DockerRuntime;

pub(crate) use check::{Check, Status};

#[derive(Clone, Debug, Default, Serialize)]
pub struct Report {
    pub checks: Vec<Check>,
    /// What `--fix` did; absent without it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fixed: Option<fix::Fixed>,
}

impl Report {
    /// What `--preflight` reports: a failure blocklyd survives becomes a warning.
    pub(crate) fn for_preflight(mut self) -> Self {
        for check in &mut self.checks {
            if check.status == Status::Fail && !check.gates_start {
                check.status = Status::Warn;
            }
        }
        self
    }

    pub fn ok(&self) -> bool {
        self.checks.iter().all(|c| c.status != Status::Fail)
    }

    fn count(&self, status: Status) -> usize {
        self.checks.iter().filter(|c| c.status == status).count()
    }

    /// For a person reading a terminal or the journal.
    pub fn text(&self, preflight: bool) -> String {
        let mut out = String::new();
        for change in self.fixed.iter().flat_map(|f| &f.changes) {
            out.push_str(&format!("fixed  {}\n       undo: {}\n", change.done, change.undo));
        }
        for next in self.fixed.iter().flat_map(|f| &f.next) {
            out.push_str(&format!("next   {next}\n"));
        }
        let width = self.checks.iter().map(|c| c.name.len()).max().unwrap_or(0);
        for check in &self.checks {
            let status = match check.status {
                Status::Pass => "pass",
                Status::Warn => "warn",
                Status::Fail => "FAIL",
            };
            out.push_str(&format!("{status}   {:width$}  {}\n", check.name, check.detail));
            if let Some(fix) = &check.fix {
                out.push_str(&format!("       {:width$}  fix: {fix}\n", ""));
            }
        }
        let (warnings, failures) = (self.count(Status::Warn), self.count(Status::Fail));
        out.push_str(&match (failures, preflight) {
            (0, true) => format!("{warnings} warning(s); blocklyd starts.\n"),
            (_, true) => format!("{failures} failure(s); blocklyd can't start until they are fixed.\n"),
            (0, false) => format!("{warnings} warning(s), no failures.\n"),
            (_, false) => format!("{failures} failure(s), {warnings} warning(s).\n"),
        });
        out
    }
}

pub struct Options {
    pub config: PathBuf,
    pub preflight: bool,
    pub fix: bool,
}

/// Docker's own configuration file, which `--fix` writes.
pub(crate) const DAEMON_JSON: &str = "/etc/docker/daemon.json";

/// The configuration check alone (`check-config`), and the configuration when it loads.
pub fn check_config(path: &Path) -> (Check, Option<crate::config::Config>) {
    config::check(path)
}

/// Runs every check; with `fix`, applies the safe fixes first.
pub async fn run(options: &Options) -> Report {
    let (config_check, config) = config::check(&options.config);
    let Some(config) = config else {
        let fixed = options.fix.then(|| fix::Fixed {
            changes: Vec::new(),
            next: vec!["Nothing changed: --fix needs a valid configuration first".into()],
        });
        let report = Report { checks: vec![config_check], fixed };
        return if options.preflight { report.for_preflight() } else { report };
    };
    let runtime: Option<Box<dyn ContainerRuntime>> =
        DockerRuntime::new(&config.docker.socket).ok().map(|r| Box::new(r) as Box<dyn ContainerRuntime>);
    let daemon_json = Path::new(DAEMON_JSON);
    let fixed = match (options.fix, &runtime) {
        (true, Some(runtime)) => Some(fix::apply(&config, runtime.as_ref(), daemon_json).await),
        _ => None,
    };
    let mut checks = vec![config_check];
    match &runtime {
        Some(runtime) => checks.extend(docker::checks(runtime.as_ref(), &config.docker.network, daemon_json).await),
        None => checks.push(Check::warn(
            "docker",
            format!("docker.socket {} isn't a path blocklyd can use", config.docker.socket.display()),
            "Set docker.socket to Docker's unix socket, usually /var/run/docker.sock",
        )),
    }
    checks.extend(state::checks(&config, &options.config));
    checks.push(network::ports(&config, &state::held_ports(&config.state_dir)));
    checks.extend(network::listeners(&config));
    checks.push(host::machine_id(std::fs::read_to_string("/etc/machine-id").ok().as_deref()));
    checks.push(host::clock());
    let report = Report { checks, fixed };
    if options.preflight { report.for_preflight() } else { report }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn report() -> Report {
        Report {
            checks: vec![
                Check::fail("config", "broken", "fix it").gating(),
                Check::fail("listen.api", "public", "bind it privately"),
                Check::warn("docker", "down", "start it"),
                Check::pass("clock", "synchronised"),
            ],
            fixed: None,
        }
    }

    #[test]
    fn preflight_fails_only_on_what_stops_blocklyd_today() {
        let mut gated = report();
        assert!(!gated.ok());
        gated = gated.for_preflight();
        assert!(!gated.ok(), "an invalid config still stops it");
        assert_eq!(gated.checks[1].status, Status::Warn, "a public api.listen is a warning in preflight");

        let mut survivable = report();
        survivable.checks.remove(0);
        assert!(!survivable.ok(), "without --preflight, a failure is a failure");
        let survivable = survivable.for_preflight();
        assert!(survivable.ok(), "blocklyd starts with Docker down and a public api.listen, as it always has");
        assert!(survivable.text(true).ends_with("2 warning(s); blocklyd starts.\n"));
    }

    #[test]
    fn json_has_name_status_detail_and_fix() {
        let json = serde_json::to_value(report()).unwrap();
        assert_eq!(
            json["checks"][2],
            serde_json::json!({"name": "docker", "status": "warn", "detail": "down", "fix": "start it"})
        );
        assert_eq!(json["checks"][3]["fix"], serde_json::Value::Null);
        assert!(json.get("fixed").is_none());
    }
}
