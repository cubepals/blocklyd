//! blocklyd upgrading itself to the blocklyd its control plane serves (`GET /fleet/v1/blocklyd`,
//! docs/fleet-operations.md §9): when a heartbeat's answer offers it, which the control plane does
//! for one node of a region at a time, or at once with `blocklyd upgrade` (`cli/upgrade.rs`).
//!
//! ```text
//! <state_dir>/bin/blocklyd          the binary systemd runs: /usr/local/bin/blocklyd links here
//! <state_dir>/bin/blocklyd.prev     the one before it, kept to go back to
//! <state_dir>/upgrade/trial.json    an upgrade that hasn't come up yet: from, to, sha256
//! <state_dir>/upgrade/failed.json   the last upgrade given up on, which heartbeats report
//! ```
//!
//! The binary lives under the state directory because the unit's `ProtectSystem=strict` leaves
//! blocklyd nothing else to write, and the unit needs no new writable path for it. A
//! `ReadWritePaths=` naming /usr/local/bin/blocklyd would bind-mount that one file, which a rename
//! can't replace, and naming /usr/local/bin would open every binary there.
//!
//! 1. The offered binary is downloaded over the node's mutual TLS to `blocklyd.next`, and kept only
//!    if its sha256 is the one offered and it runs here, saying it is the version offered.
//! 2. The current binary is kept as `blocklyd.prev`, the trial is written down, and
//!    `blocklyd.next` is renamed over `blocklyd`: one rename, so a crash leaves one or the other.
//! 3. blocklyd exits cleanly and systemd (`Restart=always`) starts the new one. Servers keep
//!    running: they are Docker's, not blocklyd's children.
//! 4. The new one is on trial while `trial.json` names its version: it must reconcile and have a
//!    heartbeat accepted within TRIAL_SECONDS, which ends the trial. Otherwise it puts
//!    `blocklyd.prev` back and exits. One that can't even start is put back by the unit's
//!    `ExecStopPost`, `blocklyd.prev upgrade --exited`, after a stop systemd doesn't call a
//!    success, its `ExecStartPre` failing included. One that hangs is ended by its backstop.
//! 5. The blocklyd put back says why in its heartbeats, until the next upgrade is tried.
//!
//! Parts (`upgrade/`):
//! - `stopped.rs`: what `--exited` goes by: systemd's word on the stop, and the state directory.
//! - `backstop.rs`: ends a start still on trial long after it began, for `--exited` to put back.

pub mod backstop;
pub mod stopped;

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use http_body_util::BodyExt;
use hyper::Method;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::durable::write_atomic;
use crate::http_client as client;
use crate::protocol::wire::{UpgradeFailure, UpgradeOffer};

/// How long a new blocklyd has to reconcile and get a heartbeat accepted before it is put back.
pub(crate) const TRIAL_SECONDS: u64 = 120;
/// No blocklyd is this big; a download that is stops.
const MAX_BYTES: u64 = 512 * 1024 * 1024;
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(600);
/// After a failed attempt, an offer is taken up again only this long after: by then the control
/// plane has heard of a failure, and offers again only once an operator retries.
const RETRY_AFTER: Duration = Duration::from_secs(60);
const LINK: &str = "/usr/local/bin/blocklyd";

/// The version this blocklyd reports to the control plane, which offers an upgrade to an older one.
/// A debug build reports BLOCKLYD_TEST_VERSION instead when it is set, so an end-to-end test can
/// run a node that looks older than the blocklyd it is offered.
pub fn daemon_version() -> String {
    #[cfg(debug_assertions)]
    if let Some(version) = std::env::var("BLOCKLYD_TEST_VERSION").ok().filter(|v| !v.is_empty()) {
        return version;
    }
    env!("CARGO_PKG_VERSION").to_owned()
}

/// Where the binaries and the upgrade's own state are.
#[derive(Clone, Debug)]
pub struct Layout {
    pub(crate) bin: PathBuf,
    /// The path the unit runs, which links to `bin`.
    pub(crate) link: PathBuf,
    dir: PathBuf,
}

impl Layout {
    /// The layout for `state_dir`, with the link under `root` (`/` but in tests).
    pub fn new(state_dir: &Path, root: &Path) -> Self {
        Self {
            bin: state_dir.join("bin").join("blocklyd"),
            link: root.join(LINK.trim_start_matches('/')),
            dir: state_dir.join("upgrade"),
        }
    }

    pub(crate) fn next(&self) -> PathBuf {
        self.bin.with_file_name("blocklyd.next")
    }

    pub(crate) fn prev(&self) -> PathBuf {
        self.bin.with_file_name("blocklyd.prev")
    }

    fn trial(&self) -> PathBuf {
        self.dir.join("trial.json")
    }

    fn failed(&self) -> PathBuf {
        self.dir.join("failed.json")
    }
}

/// An upgrade that hasn't come up yet.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Trial {
    pub from: String,
    pub to: String,
    pub sha256: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum UpgradeError {
    /// Worth trying again: the control plane didn't answer, or not all of it.
    #[error("{0}")]
    Transient(String),
    /// Told to the control plane, which stops the region's rollout.
    #[error("{0}")]
    Failed(String),
}

fn failed(what: &str, path: &Path) -> impl FnOnce(std::io::Error) -> UpgradeError {
    let what = format!("{what} {}", path.display());
    move |e| UpgradeError::Failed(format!("{what}: {e}"))
}

/// Steps 1 and 2: the binary the control plane serves, checked against `sha256` (and `version`,
/// when one was offered), put in place with the current one kept and the trial written down.
/// `running` is the binary this process runs from, which must be the one it replaces. Returns
/// the version installed.
pub(crate) async fn install(
    layout: &Layout,
    control_plane: &str,
    tls: Option<Arc<rustls::ClientConfig>>,
    offer: (&str, Option<&str>),
    running: Option<&Path>,
) -> Result<String, UpgradeError> {
    let (sha256, version) = offer;
    let _ = fs::remove_file(layout.failed());
    settle(layout, running)?;
    download(layout, control_plane, tls, sha256).await?;
    let next = layout.next();
    let printed = tokio::task::spawn_blocking(move || version_of(&next))
        .await
        .map_err(|e| UpgradeError::Failed(e.to_string()))?
        .map_err(|e| UpgradeError::Failed(format!("the blocklyd downloaded doesn't run here: {e}")))?;
    if let Some(offered) = version.filter(|v| *v != printed) {
        return Err(UpgradeError::Failed(format!(
            "the blocklyd downloaded says it is {printed}, not the {offered} offered"
        )));
    }
    let trial = Trial { from: daemon_version(), to: printed.clone(), sha256: sha256.to_owned() };
    swap(layout, &trial)?;
    Ok(printed)
}

/// The binary at `bin`, where an upgrade can replace it. A host that has /usr/local/bin/blocklyd as
/// a plain file, from before blocklyd upgraded itself, has it moved there and linked, which only
/// works outside the unit's sandbox: as root, from `blocklyd upgrade`.
fn settle(layout: &Layout, running: Option<&Path>) -> Result<(), UpgradeError> {
    let canonical = |path: &Path| fs::canonicalize(path).ok();
    if let Some(exe) = running
        && canonical(exe) != canonical(&layout.bin)
        && canonical(exe) != canonical(&layout.link)
    {
        return Err(UpgradeError::Failed(format!(
            "this blocklyd runs from {}, not {}: only the blocklyd the service runs upgrades itself",
            exe.display(),
            layout.link.display()
        )));
    }
    if layout.bin.is_file() {
        return Ok(());
    }
    if !fs::symlink_metadata(&layout.link).is_ok_and(|m| m.file_type().is_file()) {
        return Err(UpgradeError::Failed(format!("there is no blocklyd at {} to upgrade", layout.bin.display())));
    }
    let moved = || -> std::io::Result<()> {
        // The link first, where it can't be made the rest isn't either.
        let link = layout.link.with_file_name("blocklyd.link");
        let _ = fs::remove_file(&link);
        std::os::unix::fs::symlink(&layout.bin, &link)?;
        fs::create_dir_all(layout.bin.parent().unwrap_or(Path::new("/")))?;
        fs::copy(&layout.link, layout.next())?;
        fs::set_permissions(layout.next(), fs::Permissions::from_mode(0o755))?;
        fs::rename(layout.next(), &layout.bin)?;
        fs::rename(&link, &layout.link)
    };
    moved().map_err(|e| {
        UpgradeError::Failed(format!(
            "blocklyd is a plain file at {}, which can't be moved to {} from here ({e}): run `sudo blocklyd upgrade` \
             on the host once",
            layout.link.display(),
            layout.bin.display()
        ))
    })
}

/// `blocklyd.next`, from the control plane, if its sha256 is `sha256`.
async fn download(
    layout: &Layout,
    control_plane: &str,
    tls: Option<Arc<rustls::ClientConfig>>,
    sha256: &str,
) -> Result<(), UpgradeError> {
    let url = format!("{}/fleet/v1/blocklyd", control_plane.trim_end_matches('/'));
    let next = layout.next();
    let transient = |e: &dyn std::fmt::Display| UpgradeError::Transient(format!("{url}: {e}"));
    let fetch = async {
        let response = client::send(Method::GET, &url, &[], client::empty(), tls, Duration::from_secs(30))
            .await
            .map_err(|e| transient(&e))?;
        let status = response.status();
        if !status.is_success() {
            let said = client::read_body(response, 4096).await.unwrap_or_default();
            let said = format!("HTTP {status}: {}", String::from_utf8_lossy(&said).trim());
            return Err(if status.is_server_error() { transient(&said) } else { UpgradeError::Failed(said) });
        }
        let mut file = tokio::fs::File::create(&next).await.map_err(failed("writing", &next))?;
        let (mut hasher, mut size) = (Sha256::new(), 0u64);
        let mut body = response.into_body();
        while let Some(frame) = body.frame().await {
            let Ok(data) = frame.map_err(|e| transient(&e))?.into_data() else { continue };
            size += data.len() as u64;
            if size > MAX_BYTES {
                return Err(UpgradeError::Failed(format!("{url}: more than {MAX_BYTES} bytes, which no blocklyd is")));
            }
            hasher.update(&data);
            file.write_all(&data).await.map_err(failed("writing", &next))?;
        }
        file.sync_all().await.map_err(failed("writing", &next))?;
        Ok(hex::encode(hasher.finalize()))
    };
    let got = tokio::time::timeout(DOWNLOAD_TIMEOUT, fetch)
        .await
        .map_err(|_| transient(&format!("not downloaded within {DOWNLOAD_TIMEOUT:?}")))?;
    let got = got.inspect_err(|_| {
        let _ = fs::remove_file(&next);
    })?;
    if !got.eq_ignore_ascii_case(sha256) {
        let _ = fs::remove_file(&next);
        return Err(UpgradeError::Failed(format!(
            "the blocklyd downloaded has sha256 {got}, not the {sha256} offered, so it wasn't installed"
        )));
    }
    fs::set_permissions(&next, fs::Permissions::from_mode(0o755)).map_err(failed("making executable", &next))
}

/// What `<bin> --version` says it is: `blocklyd 0.3.0` is 0.3.0.
pub(crate) fn version_of(bin: &Path) -> Result<String, String> {
    use std::process::{Command, Stdio};
    let spawn =
        || Command::new(bin).arg("--version").stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::null()).spawn();
    // A file just written may still be open in a child another thread forked: try again then.
    let mut child = spawn();
    for _ in 0..5 {
        match &child {
            Err(e) if rustix::io::Errno::from_io_error(e) == Some(rustix::io::Errno::TXTBSY) => {
                std::thread::sleep(Duration::from_millis(50))
            }
            _ => break,
        }
        child = spawn();
    }
    let mut child = child.map_err(|e| e.to_string())?;
    let deadline = Instant::now() + Duration::from_secs(10);
    while child.try_wait().map_err(|e| e.to_string())?.is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err("--version didn't finish within 10 s".into());
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    let output = child.wait_with_output().map_err(|e| e.to_string())?;
    let text = String::from_utf8_lossy(&output.stdout);
    match text.trim().strip_prefix("blocklyd ") {
        Some(version) if output.status.success() && !version.contains(char::is_whitespace) => Ok(version.to_owned()),
        _ if !output.status.success() => Err(format!("--version failed ({})", output.status)),
        _ => Err(format!("--version printed {:?}, not a blocklyd version", text.trim())),
    }
}

/// Step 2. The trial is written before the rename: a crash between them leaves the old binary with
/// a trial for another version, which it drops (`on_trial`), rather than an untried new one.
fn swap(layout: &Layout, trial: &Trial) -> Result<(), UpgradeError> {
    let (prev, kept) = (layout.prev(), layout.bin.with_file_name("blocklyd.prev.tmp"));
    let _ = fs::remove_file(&kept);
    fs::hard_link(&layout.bin, &kept).map_err(failed("keeping", &layout.bin))?;
    fs::rename(&kept, &prev).map_err(failed("keeping", &prev))?;
    fs::create_dir_all(&layout.dir).map_err(failed("making", &layout.dir))?;
    fs::set_permissions(&layout.dir, fs::Permissions::from_mode(0o700)).map_err(failed("making", &layout.dir))?;
    let text = serde_json::to_vec_pretty(trial).map_err(|e| UpgradeError::Failed(e.to_string()))?;
    write_atomic(&layout.trial(), &text, 0o600).map_err(|e| UpgradeError::Failed(e.to_string()))?;
    fs::rename(layout.next(), &layout.bin).map_err(failed("installing", &layout.bin))?;
    let dir = layout.bin.parent().unwrap_or(Path::new("/"));
    fs::File::open(dir).and_then(|d| d.sync_all()).map_err(failed("syncing", dir))
}

/// The trial this binary, `version`, is on. A trial for another version is left from an upgrade
/// that never ran, or was put back, and is dropped.
pub(crate) fn on_trial(layout: &Layout, version: &str) -> Option<Trial> {
    let text = fs::read(layout.trial()).ok()?;
    match serde_json::from_slice::<Trial>(&text) {
        Ok(trial) if trial.to == version => Some(trial),
        _ => {
            let _ = fs::remove_file(layout.trial());
            None
        }
    }
}

/// The trial passed: the new binary stays, and `blocklyd.prev` with it, for going back by hand.
pub(crate) fn confirm(layout: &Layout) {
    let _ = fs::remove_file(layout.trial());
}

/// Puts `blocklyd.prev` back for a trial that failed, and writes down why, for the heartbeats.
/// Returns whether there was one to put back.
pub(crate) fn roll_back(layout: &Layout, trial: &Trial, reason: &str) -> anyhow::Result<bool> {
    let prev = layout.prev();
    let restored = prev.is_file();
    if restored {
        fs::rename(&prev, &layout.bin).with_context(|| format!("putting {} back", prev.display()))?;
    }
    record_failure(layout, &trial.to, reason)?;
    confirm(layout);
    Ok(restored)
}

fn record_failure(layout: &Layout, version: &str, reason: &str) -> anyhow::Result<()> {
    fs::create_dir_all(&layout.dir).with_context(|| format!("making {}", layout.dir.display()))?;
    let failure = UpgradeFailure { version: version.to_owned(), reason: reason.to_owned() };
    write_atomic(&layout.failed(), &serde_json::to_vec_pretty(&failure)?, 0o600)?;
    Ok(())
}

/// The last upgrade given up on, which every heartbeat reports until the next one is tried.
pub(crate) fn failure(layout: &Layout) -> Option<UpgradeFailure> {
    serde_json::from_slice(&fs::read(layout.failed()).ok()?).ok()
}

/// `blocklyd.prev upgrade --exited`, the unit's `ExecStopPost`, after any stop: a binary on trial
/// that stopped other than cleanly is put back. `result`, `code` and `status` are systemd's
/// `SERVICE_RESULT`, `EXIT_CODE` and `EXIT_STATUS`, which `stopped::failure` reads.
pub fn exited(
    layout: &Layout,
    result: Option<&str>,
    code: Option<&str>,
    status: Option<&str>,
) -> anyhow::Result<String> {
    let Some(trial) = fs::read(layout.trial()).ok().and_then(|t| serde_json::from_slice::<Trial>(&t).ok()) else {
        return Ok(String::new());
    };
    let Some(how) = stopped::failure(result, code, status) else {
        return Ok(String::new());
    };
    let reason = format!("blocklyd {} {how} before it came up", trial.to);
    let said = match roll_back(layout, &trial, &reason)? {
        true => format!("{reason}: put blocklyd {} back\n", trial.from),
        false => format!("{reason}, and there is no blocklyd before it to put back\n"),
    };
    Ok(said)
}

/// The daemon's side: offers in heartbeat answers, and the trial a start after an upgrade is on.
pub struct Upgrader {
    layout: Layout,
    control_plane: String,
    version: String,
    trial: Mutex<Option<Trial>>,
    confirmed: tokio::sync::Notify,
    busy: AtomicBool,
    retry_at: Mutex<Option<Instant>>,
    rolled_back: Mutex<Option<String>>,
    trial_for: Duration,
    /// Set when a heartbeat ends the trial, which disarms the start's backstop (backstop.rs).
    trial_ended: Arc<AtomicBool>,
    /// Stops the daemon: after an upgrade, so systemd starts the new binary.
    cancel: CancellationToken,
}

impl Upgrader {
    pub fn new(state_dir: &Path, control_plane: &str, trial_ended: Arc<AtomicBool>, cancel: CancellationToken) -> Self {
        let layout = Layout::new(state_dir, Path::new("/"));
        let version = daemon_version();
        let trial = on_trial(&layout, &version);
        if let Some(trial) = &trial {
            info!(
                from = %trial.from,
                to = %trial.to,
                seconds = TRIAL_SECONDS,
                "upgraded: on trial until reconciled and a heartbeat is accepted"
            );
        }
        Self {
            layout,
            control_plane: control_plane.to_owned(),
            version,
            trial: Mutex::new(trial),
            confirmed: tokio::sync::Notify::new(),
            busy: AtomicBool::new(false),
            retry_at: Mutex::new(None),
            rolled_back: Mutex::new(None),
            trial_for: Duration::from_secs(TRIAL_SECONDS),
            trial_ended,
            cancel,
        }
    }

    /// What heartbeats report: the last upgrade given up on.
    pub(crate) fn failure(&self) -> Option<UpgradeFailure> {
        failure(&self.layout)
    }

    /// The control plane accepted a heartbeat. One that said the node had reconciled ends a trial.
    pub(crate) fn accepted(&self, reconciled: bool) {
        if !reconciled {
            return;
        }
        let Some(trial) = self.trial.lock().unwrap().take() else { return };
        confirm(&self.layout);
        self.trial_ended.store(true, Ordering::SeqCst);
        info!(from = %trial.from, to = %trial.to, "the upgrade came up: reconciled, and the control plane hears it");
        self.confirmed.notify_one();
    }

    /// Why this start put the previous binary back and stopped, if it did.
    pub fn rolled_back(&self) -> Option<String> {
        self.rolled_back.lock().unwrap().clone()
    }

    /// Waits out a trial. One that doesn't pass in time puts the previous binary back and stops
    /// the daemon, so systemd starts that one.
    pub async fn watch_trial(self: Arc<Self>) {
        if self.trial.lock().unwrap().is_none() {
            return;
        }
        tokio::select! {
            () = self.confirmed.notified() => return,
            () = self.cancel.cancelled() => return,
            () = tokio::time::sleep(self.trial_for) => {}
        }
        let Some(trial) = self.trial.lock().unwrap().take() else { return };
        let reason =
            format!("blocklyd {} didn't reconcile and get a heartbeat accepted within {TRIAL_SECONDS} s", trial.to);
        match roll_back(&self.layout, &trial, &reason) {
            Ok(true) => {
                error!(from = %trial.from, to = %trial.to, "{reason}: put {} back; stopping to run it", trial.from);
                *self.rolled_back.lock().unwrap() = Some(format!("{reason}; put {} back", trial.from));
                self.cancel.cancel();
            }
            Ok(false) => warn!(to = %trial.to, "{reason}, and there is no blocklyd before it to put back"),
            Err(e) => error!(error = format!("{e:#}"), "{reason}, and putting the one before back failed"),
        }
    }

    /// A heartbeat's answer offered `offer`: it is installed in the background, and blocklyd
    /// stops once it is, unless an attempt runs or failed a moment ago.
    pub(crate) fn offered(self: &Arc<Self>, offer: UpgradeOffer, tls: Arc<rustls::ClientConfig>) {
        if offer.version == self.version || self.trial.lock().unwrap().is_some() || self.cancel.is_cancelled() {
            return;
        }
        if self.retry_at.lock().unwrap().is_some_and(|at| Instant::now() < at) || self.busy.swap(true, Ordering::SeqCst)
        {
            return;
        }
        let this = self.clone();
        tokio::spawn(async move {
            info!(from = %this.version, to = %offer.version, "upgrading blocklyd, as the control plane offers");
            let running = std::env::current_exe().ok();
            let offered = (offer.sha256.as_str(), Some(offer.version.as_str()));
            match install(&this.layout, &this.control_plane, Some(tls), offered, running.as_deref()).await {
                Ok(to) => {
                    info!(from = %this.version, to = %to, "installed; restarting into it, servers keep running");
                    this.cancel.cancel();
                    return;
                }
                Err(UpgradeError::Transient(e)) => {
                    warn!(error = %e, "couldn't download the blocklyd offered; trying again in a minute")
                }
                Err(UpgradeError::Failed(e)) => {
                    warn!(error = %e, to = %offer.version, "gave up on the upgrade offered; the control plane is told");
                    if let Err(e) = record_failure(&this.layout, &offer.version, &e) {
                        warn!(error = format!("{e:#}"), "couldn't write the failure down");
                    }
                }
            }
            *this.retry_at.lock().unwrap() = Some(Instant::now() + RETRY_AFTER);
            this.busy.store(false, Ordering::SeqCst);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in blocklyd: a script that says it is `version` and does nothing else.
    fn script(path: &Path, version: &str) {
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, format!("#!/bin/sh\n[ \"$1\" = --version ] && echo 'blocklyd {version}'\n")).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn layout(root: &Path) -> Layout {
        let layout = Layout::new(&root.join("var/lib/blocklyd"), root);
        script(&layout.bin, "0.2.0");
        layout
    }

    fn trial() -> Trial {
        Trial { from: "0.2.0".into(), to: "0.3.0".into(), sha256: "ab".repeat(32) }
    }

    #[test]
    fn a_swap_keeps_the_binary_before_and_puts_the_new_one_on_trial() {
        let root = tempfile::tempdir().unwrap();
        let layout = layout(root.path());
        script(&layout.next(), "0.3.0");
        swap(&layout, &trial()).unwrap();
        assert_eq!(version_of(&layout.bin).unwrap(), "0.3.0");
        assert_eq!(version_of(&layout.prev()).unwrap(), "0.2.0");
        assert!(!layout.next().exists());
        assert_eq!(on_trial(&layout, "0.3.0"), Some(trial()));
        // The binary before it, started instead, drops a trial that isn't its own.
        assert_eq!(on_trial(&layout, "0.2.0"), None);
        assert_eq!(on_trial(&layout, "0.3.0"), None);
    }

    #[test]
    fn a_failed_trial_puts_the_binary_before_back_and_says_why() {
        let root = tempfile::tempdir().unwrap();
        let layout = layout(root.path());
        script(&layout.next(), "0.3.0");
        swap(&layout, &trial()).unwrap();
        assert!(roll_back(&layout, &trial(), "it didn't come up").unwrap());
        assert_eq!(version_of(&layout.bin).unwrap(), "0.2.0");
        assert!(!layout.prev().exists());
        assert_eq!(on_trial(&layout, "0.3.0"), None);
        assert_eq!(
            failure(&layout),
            Some(UpgradeFailure { version: "0.3.0".into(), reason: "it didn't come up".into() })
        );
    }

    #[test]
    fn after_a_stop_only_a_trial_that_exited_badly_is_put_back() {
        let root = tempfile::tempdir().unwrap();
        let layout = layout(root.path());
        // Nothing on trial: whatever the exit, nothing changes.
        assert_eq!(exited(&layout, Some("exit-code"), Some("exited"), Some("1")).unwrap(), "");
        script(&layout.next(), "0.3.0");
        swap(&layout, &trial()).unwrap();
        // The old binary's own clean exit, right after the swap; and a stop (systemctl stop, the
        // host shutting down) before the trial ended, whose SIGTERM systemd counts as clean.
        assert_eq!(exited(&layout, Some("success"), Some("exited"), Some("0")).unwrap(), "");
        assert_eq!(exited(&layout, None, Some("exited"), Some("0")).unwrap(), "");
        assert_eq!(exited(&layout, Some("success"), Some("killed"), Some("TERM")).unwrap(), "");
        assert_eq!(on_trial(&layout, "0.3.0"), Some(trial()), "the trial carries on at the next start");
        let said = exited(&layout, None, Some("exited"), Some("1")).unwrap();
        assert_eq!(said, "blocklyd 0.3.0 exited with status 1 before it came up: put blocklyd 0.2.0 back\n");
        assert_eq!(version_of(&layout.bin).unwrap(), "0.2.0");
        assert_eq!(failure(&layout).unwrap().reason, "blocklyd 0.3.0 exited with status 1 before it came up");
        // Its ExecStartPre failed, so it never ran: systemd says so, with no EXIT_CODE or EXIT_STATUS.
        script(&layout.next(), "0.3.0");
        swap(&layout, &trial()).unwrap();
        let said = exited(&layout, Some("exit-code"), None, None).unwrap();
        assert_eq!(said, "blocklyd 0.3.0 didn't start (exit-code) before it came up: put blocklyd 0.2.0 back\n");
        assert_eq!(version_of(&layout.bin).unwrap(), "0.2.0");
    }

    #[test]
    fn a_version_is_what_the_binary_prints() {
        let dir = tempfile::tempdir().unwrap();
        let good = dir.path().join("good");
        script(&good, "0.3.0");
        assert_eq!(version_of(&good).unwrap(), "0.3.0");
        let broken = dir.path().join("broken");
        fs::write(&broken, "#!/bin/sh\nexit 1\n").unwrap();
        fs::set_permissions(&broken, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(version_of(&broken).unwrap_err().contains("failed"));
        let chatty = dir.path().join("chatty");
        fs::write(&chatty, "#!/bin/sh\necho hello there\n").unwrap();
        fs::set_permissions(&chatty, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(version_of(&chatty).unwrap_err().contains("not a blocklyd version"));
        assert!(version_of(&dir.path().join("missing")).is_err());
    }

    #[test]
    fn a_plain_binary_is_moved_under_the_state_directory_and_linked() {
        let root = tempfile::tempdir().unwrap();
        let layout = Layout::new(&root.path().join("var/lib/blocklyd"), root.path());
        script(&layout.link, "0.2.0");
        settle(&layout, None).unwrap();
        assert_eq!(fs::read_link(&layout.link).unwrap(), layout.bin);
        assert_eq!(version_of(&layout.link).unwrap(), "0.2.0");
        // Only the binary the service runs upgrades itself.
        let elsewhere = root.path().join("target/debug/blocklyd");
        script(&elsewhere, "0.2.0");
        let refused = settle(&layout, Some(&elsewhere)).unwrap_err();
        assert!(refused.to_string().contains("only the blocklyd the service runs"), "{refused}");
        settle(&layout, Some(&layout.link)).unwrap();
        let empty = Layout::new(&root.path().join("elsewhere"), &root.path().join("none"));
        assert!(settle(&empty, None).unwrap_err().to_string().contains("no blocklyd"));
    }

    /// A control plane that serves `binary` at /fleet/v1/blocklyd, over plain HTTP.
    async fn serving(binary: Option<Vec<u8>>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route(
            "/fleet/v1/blocklyd",
            axum::routing::get(move || {
                let binary = binary.clone();
                async move {
                    match binary {
                        Some(bytes) => (axum::http::StatusCode::OK, bytes),
                        None => (axum::http::StatusCode::NOT_FOUND, b"no blocklyd here".to_vec()),
                    }
                }
            }),
        );
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{address}")
    }

    #[tokio::test]
    async fn an_offer_is_installed_only_when_its_sha256_and_version_match() {
        let root = tempfile::tempdir().unwrap();
        let layout = layout(root.path());
        let new = root.path().join("new");
        script(&new, "0.3.0");
        let bytes = fs::read(&new).unwrap();
        let sha = hex::encode(Sha256::digest(&bytes));
        let url = serving(Some(bytes)).await;

        let wrong = install(&layout, &url, None, (&"0".repeat(64), Some("0.3.0")), None).await.unwrap_err();
        assert!(matches!(wrong, UpgradeError::Failed(ref e) if e.contains("not the")), "{wrong}");
        assert!(!layout.next().exists());
        let other = install(&layout, &url, None, (&sha, Some("0.4.0")), None).await.unwrap_err();
        assert!(other.to_string().contains("not the 0.4.0 offered"), "{other}");
        assert_eq!(version_of(&layout.bin).unwrap(), "0.2.0");
        assert_eq!(on_trial(&layout, "0.3.0"), None);

        assert_eq!(install(&layout, &url, None, (&sha, Some("0.3.0")), None).await.unwrap(), "0.3.0");
        assert_eq!(version_of(&layout.bin).unwrap(), "0.3.0");
        assert_eq!(version_of(&layout.prev()).unwrap(), "0.2.0");
        assert_eq!(on_trial(&layout, "0.3.0").unwrap().to, "0.3.0");
    }

    #[tokio::test]
    async fn a_control_plane_without_a_binary_fails_the_upgrade_and_one_that_is_down_is_tried_again() {
        let root = tempfile::tempdir().unwrap();
        let layout = layout(root.path());
        let missing = install(&layout, &serving(None).await, None, (&"0".repeat(64), None), None).await.unwrap_err();
        assert!(matches!(missing, UpgradeError::Failed(ref e) if e.contains("404")), "{missing}");
        let closed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap();
        let down = install(&layout, &format!("http://{closed}"), None, (&"0".repeat(64), None), None).await;
        assert!(matches!(down, Err(UpgradeError::Transient(_))));
    }

    #[tokio::test]
    async fn a_trial_ends_with_a_heartbeat_once_reconciled_or_puts_the_binary_before_back() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("var/lib/blocklyd");
        let layout = layout(root.path());
        let ours = Trial { to: daemon_version(), ..trial() };
        script(&layout.next(), &ours.to);
        swap(&layout, &ours).unwrap();

        let ended = Arc::new(AtomicBool::new(false));
        let upgrader = Arc::new(Upgrader::new(&state, "https://control", ended.clone(), CancellationToken::new()));
        upgrader.accepted(false);
        assert!(on_trial(&layout, &ours.to).is_some());
        assert!(!ended.load(Ordering::SeqCst));
        upgrader.accepted(true);
        assert!(on_trial(&layout, &ours.to).is_none());
        assert!(ended.load(Ordering::SeqCst), "the backstop is disarmed");
        upgrader.clone().watch_trial().await;
        assert_eq!(upgrader.rolled_back(), None);

        script(&layout.bin, "0.2.0");
        script(&layout.next(), &ours.to);
        swap(&layout, &ours).unwrap();
        let cancel = CancellationToken::new();
        let ended = Arc::new(AtomicBool::new(false));
        let mut upgrader = Upgrader::new(&state, "https://control", ended.clone(), cancel.clone());
        upgrader.trial_for = Duration::from_millis(10);
        let upgrader = Arc::new(upgrader);
        upgrader.clone().watch_trial().await;
        assert!(cancel.is_cancelled());
        assert!(!ended.load(Ordering::SeqCst), "the backstop still bounds the stop");
        assert!(upgrader.rolled_back().unwrap().contains("didn't reconcile"));
        assert_eq!(version_of(&layout.bin).unwrap(), "0.2.0");
        assert_eq!(upgrader.failure().unwrap().version, ours.to);
    }
}
