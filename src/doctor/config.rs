//! Whether the configuration file is valid, with the certificates it names: what `check-config`
//! has always checked, and the one check (besides owning the state directory) that stops blocklyd
//! from starting.

use std::path::Path;

use anyhow::Context;

use super::check::Check;
use crate::config::{Config, ConfigError};
use crate::fleet::identity::Identity;

/// The verdict, and the configuration when it loads.
pub fn check(path: &Path) -> (Check, Option<Config>) {
    let config = match Config::load(path) {
        Ok(config) => config,
        Err(e) => {
            let fix = match &e {
                ConfigError::Read { .. } => {
                    format!("Write {} (examples/blocklyd.toml lists every setting), or pass --config", path.display())
                }
                _ => format!("Correct {}; examples/blocklyd.toml lists every setting", path.display()),
            };
            return (Check::fail("config", e.to_string(), fix).gating(), None);
        }
    };
    let check = match material(path, &config) {
        // What infer.rs filled in is listed, so an operator sees what the host said about itself.
        Ok(detail) if !config.inferred.is_empty() => {
            let inferred: Vec<String> =
                config.inferred.iter().map(|i| format!("{} = {} ({})", i.key, i.value, i.from)).collect();
            Check::pass("config", format!("{detail}; worked out from this host: {}", inferred.join("; ")))
        }
        Ok(detail) => Check::pass("config", detail),
        Err(e) => Check::fail("config", format!("{e:#}"), format!("Correct {}", path.display())).gating(),
    };
    (check, Some(config))
}

/// What the file names exists and loads: TLS material standalone, the fleet CA and either an
/// identity or a token in fleet mode.
fn material(path: &Path, config: &Config) -> anyhow::Result<String> {
    match (&config.fleet, &config.api.tls) {
        (None, Some(tls)) => {
            crate::api::tls::server_config(tls)?;
            Ok(format!("{} is valid (node {}, deployment {})", path.display(), config.node_id, config.deployment_id))
        }
        (Some(fleet), _) => {
            crate::api::tls::load_certs(&fleet.ca).context("fleet.ca")?;
            match Identity::load(&config.state_dir)? {
                Some(identity) => Ok(format!(
                    "{} is valid (fleet mode, enrolled as node {}, deployment {})",
                    path.display(),
                    identity.node_id(),
                    config.deployment_id
                )),
                None => {
                    let token = fleet
                        .enrollment_token_file
                        .as_deref()
                        .context("not enrolled yet and fleet.enrollment_token_file is not set: this node can't join")?;
                    anyhow::ensure!(token.exists(), "not enrolled yet and {} doesn't exist", token.display());
                    Ok(format!("{} is valid (fleet mode, will enroll with {})", path.display(), fleet.url))
                }
            }
        }
        (None, None) => unreachable!("validated"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doctor::Status;

    #[test]
    fn a_missing_or_invalid_file_fails_and_stops_blocklyd() {
        let dir = tempfile::tempdir().unwrap();
        let (verdict, config) = check(&dir.path().join("absent.toml"));
        assert_eq!((verdict.status, verdict.gates_start, config.is_none()), (Status::Fail, true, true));
        assert!(verdict.fix.unwrap().contains("examples/blocklyd.toml"));

        let path = dir.path().join("blocklyd.toml");
        std::fs::write(&path, "deployment_id = \"Staging\"\n[api]\nlisten = \"127.0.0.1:7443\"\n").unwrap();
        let (verdict, config) = check(&path);
        assert_eq!((verdict.status, verdict.gates_start, config.is_none()), (Status::Fail, true, true));
    }

    #[test]
    fn fleet_mode_without_identity_or_token_fails_but_loads() {
        let dir = tempfile::tempdir().unwrap();
        let ca = crate::certs::generate(dir.path(), &["localhost".into()], &[], "control", &[]).unwrap().ca_cert;
        let path = dir.path().join("blocklyd.toml");
        let toml = format!(
            "deployment_id = \"staging\"\nstate_dir = \"{}\"\n[api]\nlisten = \"10.0.0.5:7443\"\n\
             [fleet]\nurl = \"https://control:8443\"\nca = \"{}\"\nenrollment_token_file = \"{}\"\n",
            dir.path().join("state").display(),
            ca.display(),
            dir.path().join("token").display()
        );
        std::fs::write(&path, toml).unwrap();
        let (verdict, config) = check(&path);
        assert_eq!((verdict.status, verdict.gates_start), (Status::Fail, true));
        assert!(verdict.detail.contains("doesn't exist"), "{}", verdict.detail);
        assert!(config.is_some(), "the other checks still run");

        std::fs::write(dir.path().join("token"), "bk1.x").unwrap();
        let (verdict, _) = check(&path);
        assert_eq!(verdict.status, Status::Pass, "{}", verdict.detail);
    }
}
