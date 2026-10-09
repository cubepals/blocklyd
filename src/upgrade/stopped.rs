//! What `blocklyd.prev upgrade --exited`, the unit's `ExecStopPost`, goes by after the service
//! stopped: how systemd says the stop went, and the state directory the trial is in. That command
//! runs the binary from before the upgrade, so it reads the configuration for `state_dir` alone
//! rather than loading it whole.
//!
//! Not here: putting the binary back, which is `exited` in upgrade.rs.

use std::path::{Path, PathBuf};

use serde::Deserialize;

/// How a binary on trial failed, from what systemd gives `ExecStopPost`, or None when it stopped
/// cleanly. systemd's own verdict, `result` (`SERVICE_RESULT`, systemd 232 and later), decides
/// when there is one: anything but `success` is a failure. That covers an `ExecStartPre` that
/// failed, where the main process never ran and `code` and `status` (`EXIT_CODE`, `EXIT_STATUS`)
/// are unset, and keeps what systemd counts as clean: a stop's SIGTERM, `SuccessExitStatus=`.
/// Without it, `code` and `status` decide, and saying nothing is a clean exit.
pub(crate) fn failure(result: Option<&str>, code: Option<&str>, status: Option<&str>) -> Option<String> {
    let (exit_code, exit_status) = (code.unwrap_or("exited"), status.unwrap_or("0"));
    if result.map_or(exit_code == "exited" && exit_status == "0", |result| result == "success") {
        return None;
    }
    Some(match (code, result) {
        (None, Some(result)) => format!("didn't start ({result})"),
        _ if exit_code == "exited" => format!("exited with status {exit_status}"),
        _ => format!("was {exit_code} ({exit_status})"),
    })
}

/// The state directory `config` names: its `state_dir` key alone, or the default Config gives it
/// (`/var/lib/blocklyd`) when the file has none, or can't be read or parsed. Not `Config::load`,
/// which holds the file to this older binary's whole schema and works out addresses from the
/// host: when that failed, the unit's `-` would hide it, and nothing would be put back.
pub fn state_dir(config: &Path) -> PathBuf {
    #[derive(Deserialize)]
    struct StateDir {
        state_dir: Option<PathBuf>,
    }
    let text = std::fs::read_to_string(config).unwrap_or_default();
    toml::from_str::<StateDir>(&text).ok().and_then(|c| c.state_dir).unwrap_or_else(|| "/var/lib/blocklyd".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn systemd_says_whether_a_stop_failed_and_its_exit_says_how() {
        // (SERVICE_RESULT, EXIT_CODE, EXIT_STATUS) and what went wrong, if anything.
        let cases = [
            // A clean exit, and a stop (systemctl stop, the host shutting down), whose SIGTERM
            // systemd counts as clean.
            ((Some("success"), Some("exited"), Some("0")), None),
            ((Some("success"), Some("killed"), Some("TERM")), None),
            // Its ExecStartPre (doctor --preflight) failed: the main process never ran.
            ((Some("exit-code"), None, None), Some("didn't start (exit-code)")),
            // With EXIT_CODE, it reads as it always has.
            ((Some("exit-code"), Some("exited"), Some("1")), Some("exited with status 1")),
            ((Some("signal"), Some("killed"), Some("SEGV")), Some("was killed (SEGV)")),
            ((Some("timeout"), Some("killed"), Some("KILL")), Some("was killed (KILL)")),
            // Without SERVICE_RESULT (systemd before 232), as before: saying nothing is clean.
            ((None, None, None), None),
            ((None, Some("exited"), Some("0")), None),
            ((None, Some("exited"), Some("1")), Some("exited with status 1")),
            ((None, Some("killed"), Some("TERM")), Some("was killed (TERM)")),
            ((None, Some("dumped"), Some("ABRT")), Some("was dumped (ABRT)")),
        ];
        for ((result, code, status), how) in cases {
            assert_eq!(failure(result, code, status).as_deref(), how, "{result:?} {code:?} {status:?}");
        }
    }

    #[test]
    fn the_state_directory_is_read_from_its_key_alone() {
        let dir = tempfile::tempdir().unwrap();
        let config = dir.path().join("blocklyd.toml");
        // What the configuration itself makes of it, default included.
        let minimal = "node_id = \"host-1\"\ndeployment_id = \"staging\"\n[api]\nlisten = \"10.0.0.5:7443\"\n\
                       [api.tls]\ncert = \"/c\"\nkey = \"/k\"\nclient_ca = \"/ca\"\nallowed_clients = [\"cp\"]\n";
        for text in [minimal.to_owned(), format!("state_dir = \"/srv/blocklyd\"\n{minimal}")] {
            std::fs::write(&config, &text).unwrap();
            assert_eq!(state_dir(&config), toml::from_str::<crate::config::Config>(&text).unwrap().state_dir);
        }
        // Keys this binary doesn't know, or an address it couldn't work out, don't matter here.
        std::fs::write(&config, "state_dir = \"/srv/b\"\nnewer_key = 1\n[fleet]\nurl = \"https://cp\"\n").unwrap();
        assert_eq!(state_dir(&config), Path::new("/srv/b"));
        // A file that isn't TOML, or none: the default.
        std::fs::write(&config, "state_dir = [").unwrap();
        assert_eq!(state_dir(&config), Path::new("/var/lib/blocklyd"));
        assert_eq!(state_dir(&dir.path().join("missing.toml")), Path::new("/var/lib/blocklyd"));
    }
}
