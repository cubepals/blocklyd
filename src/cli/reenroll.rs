//! Re-enrolling a node under its own id: what `blocklyd join` does with a token that names the
//! node this host already is, as `bun scripts/fleet.ts token --node <id>` and `rotate-ca` mint
//! (docs/fleet-operations.md §8). It
//!
//! 1. stops the unit;
//! 2. moves `identity` to `identity.old-<unix seconds>`;
//! 3. writes the token, and the fleet CA when the token names another one than the host has;
//! 4. starts the unit, which enrolls with the token, waits for the first heartbeat the control
//!    plane accepts, and only then removes `identity.old-*`.
//!
//! When a step fails before the new identity is enrolled, it stops the unit, keeps whatever the
//! attempt left as `identity.failed-<unix seconds>`, puts the old identity and CA back, and starts
//! the unit on them. Once the new identity is enrolled (its `node.json`, written last, exists) the
//! old certificates no longer work, so a slow first heartbeat leaves the new identity in place, the
//! old one aside, and says so: putting the old one back would only cut the node off. The node's id, its placements and its servers are the control
//! plane's to keep, and it does (registry/enrollment.ts, `reenroll`).
//!
//! Not here: deciding that this host is that node, or checking the CA against the token. join.rs
//! does both before anything here runs.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, bail};

use super::service::Service;
use crate::fleet::identity::{Identity, write_atomic};

pub(crate) struct Reenroll<'a> {
    pub(crate) node: &'a str,
    /// The state directory, the CA file and the token file, as this process reaches them.
    pub(crate) state_dir: PathBuf,
    pub(crate) ca_file: PathBuf,
    pub(crate) token_file: PathBuf,
    /// The fleet CA the token names, already checked against its hash, and the token as pasted.
    pub(crate) ca: &'a [u8],
    pub(crate) token: &'a str,
    /// Where the restarted blocklyd answers for its metrics, and how long its first beat may take.
    pub(crate) ops: SocketAddr,
    pub(crate) within: Duration,
}

/// Swaps the identity, and returns what to tell the operator; puts the old one back on failure.
pub(crate) async fn reenroll(swap: Reenroll<'_>, service: &impl Service) -> anyhow::Result<String> {
    let identity = Identity::dir_for(&swap.state_dir);
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let old = swap.state_dir.join(format!("identity.old-{stamp}"));
    let old_ca = std::fs::read(&swap.ca_file).with_context(|| format!("reading {}", swap.ca_file.display()))?;
    let new_ca = old_ca != swap.ca;

    service.systemctl(&["stop", "blocklyd"])?;
    let attempt = async {
        std::fs::rename(&identity, &old).with_context(|| format!("moving {} aside", identity.display()))?;
        if new_ca {
            write_atomic(&swap.ca_file, swap.ca, 0o644)?;
        }
        write_atomic(&swap.token_file, swap.token.trim().as_bytes(), 0o600)?;
        service.systemctl(&["start", "blocklyd"])?;
        println!("Re-enrolling node {}: waiting for its first heartbeat.", swap.node);
        anyhow::Ok(service.first_beat(swap.ops, swap.within).await)
    };
    let why = match attempt.await {
        Ok(true) => {
            for left in old_identities(&swap.state_dir)? {
                std::fs::remove_dir_all(&left).with_context(|| format!("removing {}", left.display()))?;
            }
            let mut said = format!(
                "Re-enrolled this host as node {}: the control plane accepted its first heartbeat, and the old \
                 identity is removed.\n",
                swap.node
            );
            if new_ca {
                said += &format!("Replaced {} with the fleet CA the token names.\n", swap.ca_file.display());
            }
            return Ok(said);
        }
        Ok(false) => format!("no heartbeat was accepted within {} s", swap.within.as_secs()),
        Err(e) => format!("{e:#}"),
    };

    // `old` exists only once this attempt moved the identity aside, so a node.json now is the new one.
    if old.exists() && identity.join("node.json").exists() {
        bail!(
            "node {} re-enrolled, but {why}. Its old certificates stopped working when it enrolled, so it keeps \
             the new identity (the old one is in {}) and blocklyd keeps running and beating on its own. See why \
             with `journalctl -u blocklyd`; `bun scripts/fleet.ts node {}` shows when it is healthy",
            swap.node,
            old.display(),
            swap.node
        );
    }

    let failed = swap.state_dir.join(format!("identity.failed-{stamp}"));
    service.systemctl(&["stop", "blocklyd"])?;
    if old.exists() {
        if identity.exists() {
            std::fs::rename(&identity, &failed).with_context(|| format!("moving {} aside", identity.display()))?;
        }
        std::fs::rename(&old, &identity).with_context(|| format!("putting {} back", old.display()))?;
    }
    if new_ca {
        write_atomic(&swap.ca_file, &old_ca, 0o644)?;
    }
    match std::fs::remove_file(&swap.token_file) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            return Err(e).with_context(|| format!("removing {}", swap.token_file.display()));
        }
        _ => {}
    }
    service.systemctl(&["start", "blocklyd"])?;
    let kept =
        if failed.exists() { format!(" What the attempt enrolled is in {}.", failed.display()) } else { String::new() };
    bail!(
        "re-enrolling node {} didn't finish: {why}. This host is back on its old identity{}, and blocklyd started \
         with it.{kept} See why with `journalctl -u blocklyd`, then paste the line again, or a new one from \
         `bun scripts/fleet.ts token --node {}` if this one was used",
        swap.node,
        if new_ca { " and fleet CA" } else { "" },
        swap.node
    )
}

/// What earlier re-enrollments moved aside, this one's included.
fn old_identities(state_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let entries = std::fs::read_dir(state_dir).with_context(|| format!("reading {}", state_dir.display()))?;
    Ok(entries
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("identity.old-"))
        .map(|entry| entry.path())
        .collect())
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    /// The host as a re-enrollment sees it: what systemctl was asked, and a blocklyd that enrolls
    /// as `enrolls` on start and has a beat accepted, or never does.
    struct Host {
        asked: RefCell<Vec<String>>,
        identity: PathBuf,
        enrolls: Option<&'static str>,
        /// Whether a heartbeat is accepted once it has enrolled.
        beats: bool,
    }

    impl Service for Host {
        fn systemctl(&self, args: &[&str]) -> anyhow::Result<()> {
            self.asked.borrow_mut().push(args.join(" "));
            if args[0] == "start" && !self.identity.exists() {
                std::fs::create_dir_all(&self.identity).unwrap();
                if let Some(node) = self.enrolls {
                    std::fs::write(self.identity.join("node.json"), node).unwrap();
                }
            }
            Ok(())
        }

        async fn first_beat(&self, _: SocketAddr, _: Duration) -> bool {
            self.enrolls.is_some() && self.beats
        }
    }

    const OLD_CA: &[u8] = b"the old fleet CA";
    const NEW_CA: &[u8] = b"the new fleet CA";

    /// An enrolled host under `root`: node n1's identity, its CA and nothing else.
    fn enrolled(root: &Path, enrolls: Option<&'static str>) -> Host {
        let identity = root.join("var/lib/blocklyd/identity");
        std::fs::create_dir_all(&identity).unwrap();
        std::fs::write(identity.join("node.json"), "n1, as first enrolled").unwrap();
        std::fs::create_dir_all(root.join("etc/blocklyd")).unwrap();
        std::fs::write(root.join("etc/blocklyd/fleet-ca.pem"), OLD_CA).unwrap();
        Host { asked: RefCell::default(), identity, enrolls, beats: true }
    }

    fn swap<'a>(root: &Path, ca: &'a [u8]) -> Reenroll<'a> {
        Reenroll {
            node: "n1",
            state_dir: root.join("var/lib/blocklyd"),
            ca_file: root.join("etc/blocklyd/fleet-ca.pem"),
            token_file: root.join("var/lib/blocklyd/enrollment-token"),
            ca,
            token: "bk1.token\n",
            ops: "127.0.0.1:7444".parse().unwrap(),
            within: Duration::from_secs(120),
        }
    }

    fn state(root: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(root.join("var/lib/blocklyd"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[tokio::test]
    async fn a_node_re_enrolls_under_its_own_id_and_drops_the_old_identity_once_it_beats() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let host = enrolled(root, Some("n1, enrolled again"));
        let said = reenroll(swap(root, OLD_CA), &host).await.unwrap();
        assert!(said.contains("Re-enrolled this host as node n1"), "{said}");
        assert!(!said.contains("Replaced"), "the same CA stays as it was: {said}");
        assert_eq!(*host.asked.borrow(), ["stop blocklyd", "start blocklyd"]);
        assert_eq!(state(root), ["enrollment-token", "identity"], "no identity.old-* left");
        let token = root.join("var/lib/blocklyd/enrollment-token");
        assert_eq!(std::fs::read_to_string(&token).unwrap(), "bk1.token");
        assert_eq!(std::fs::metadata(&token).unwrap().permissions().mode() & 0o777, 0o600);
        assert_eq!(std::fs::read_to_string(host.identity.join("node.json")).unwrap(), "n1, enrolled again");
    }

    #[tokio::test]
    async fn a_token_naming_a_new_ca_replaces_the_hosts() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let host = enrolled(root, Some("n1, under the new CA"));
        let said = reenroll(swap(root, NEW_CA), &host).await.unwrap();
        assert!(said.contains("Replaced") && said.contains("fleet-ca.pem"), "{said}");
        assert_eq!(std::fs::read(root.join("etc/blocklyd/fleet-ca.pem")).unwrap(), NEW_CA);
    }

    #[tokio::test]
    async fn without_a_beat_the_old_identity_and_ca_come_back() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let host = enrolled(root, None);
        let refused = reenroll(swap(root, NEW_CA), &host).await.unwrap_err().to_string();
        assert!(refused.contains("no heartbeat was accepted within 120 s"), "{refused}");
        assert!(refused.contains("back on its old identity and fleet CA"), "{refused}");
        assert!(refused.contains("bun scripts/fleet.ts token --node n1"), "{refused}");
        assert_eq!(*host.asked.borrow(), ["stop blocklyd", "start blocklyd", "stop blocklyd", "start blocklyd"]);
        assert_eq!(std::fs::read_to_string(host.identity.join("node.json")).unwrap(), "n1, as first enrolled");
        assert_eq!(std::fs::read(root.join("etc/blocklyd/fleet-ca.pem")).unwrap(), OLD_CA);
        let left = state(root);
        assert_eq!(left.len(), 2, "{left:?}");
        assert_eq!(left[0], "identity");
        assert!(left[1].starts_with("identity.failed-") && refused.contains(&left[1]), "kept, and said: {left:?}");
    }

    #[tokio::test]
    async fn an_enrolled_identity_is_kept_when_its_first_beat_is_slow() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path();
        let host = Host { beats: false, ..enrolled(root, Some("n1, enrolled again")) };
        let said = reenroll(swap(root, NEW_CA), &host).await.unwrap_err().to_string();
        assert!(said.contains("node n1 re-enrolled, but no heartbeat was accepted"), "{said}");
        assert_eq!(*host.asked.borrow(), ["stop blocklyd", "start blocklyd"], "left running");
        assert_eq!(std::fs::read_to_string(host.identity.join("node.json")).unwrap(), "n1, enrolled again");
        assert_eq!(std::fs::read(root.join("etc/blocklyd/fleet-ca.pem")).unwrap(), NEW_CA, "the CA it enrolled under");
        let left = state(root);
        assert!(left.iter().any(|n| n.starts_with("identity.old-") && said.contains(n.as_str())), "{left:?}");
    }
}
