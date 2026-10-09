//! `blocklyd upgrade`: the control plane's blocklyd at once, as root on the host, rather than when
//! a heartbeat's answer offers it. How an upgrade is installed, tried and put back is
//! `crate::upgrade`.

use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use hyper::Method;
use sha2::{Digest, Sha256};

use crate::config::Config;
use crate::fleet::identity::Identity;
use crate::http_client as client;
pub use crate::upgrade::Trial;
use crate::upgrade::{Layout, TRIAL_SECONDS, install};

/// `blocklyd upgrade`: the control plane's blocklyd now, then a restart into it, which goes back by
/// itself if the new one doesn't come up.
pub async fn upgrade(config: &Path, restart: bool) -> anyhow::Result<String> {
    if !rustix::process::geteuid().is_root() {
        bail!("blocklyd upgrade replaces the service's binary: run it as root");
    }
    let config = Config::load(config)?;
    let Some(fleet) = &config.fleet else {
        bail!(
            "this blocklyd runs without [fleet]: it upgrades from a control plane only, so replace the binary by hand"
        );
    };
    let Some(identity) = Identity::load(&config.state_dir)? else {
        bail!("this node hasn't enrolled yet: start blocklyd first, and upgrade once it has");
    };
    let tls = identity.client_tls()?;
    let url = format!("{}/fleet/v1/blocklyd.sha256", fleet.url.trim_end_matches('/'));
    let response = client::send(Method::GET, &url, &[], client::empty(), Some(tls.clone()), Duration::from_secs(30))
        .await
        .with_context(|| format!("asking {url}"))?;
    let status = response.status();
    let body = client::read_body(response, 64 * 1024).await.map_err(anyhow::Error::msg)?;
    let text = String::from_utf8_lossy(&body);
    anyhow::ensure!(status.is_success(), "{url}: HTTP {status}: {}", text.trim());
    let sha256 = text.split_whitespace().next().filter(|s| s.len() == 64).context("an answer with no sha256")?;

    let layout = Layout::new(&config.state_dir, Path::new("/"));
    let current = crate::fleet::daemon_version();
    let installed = if layout.bin.is_file() { &layout.bin } else { &layout.link };
    if fs::read(installed).is_ok_and(|b| hex::encode(Sha256::digest(&b)).eq_ignore_ascii_case(sha256)) {
        return Ok(format!("This is already the control plane's blocklyd ({current}): nothing changed.\n"));
    }
    let to = install(&layout, &fleet.url, Some(tls), (sha256, None), None).await?;
    let mut said = format!(
        "Installed blocklyd {to} (was {current}) at {}, and kept the one before as {}.\n",
        layout.bin.display(),
        layout.prev().display()
    );
    if restart {
        let status = std::process::Command::new("systemctl").args(["restart", "blocklyd"]).status();
        anyhow::ensure!(
            status.is_ok_and(|s| s.success()),
            "{said}systemctl restart blocklyd failed: restart it yourself"
        );
        said += &format!(
            "Restarted blocklyd; servers keep running. If {to} doesn't reconcile and reach the control plane \
             within {TRIAL_SECONDS} s, blocklyd puts {current} back by itself.\n"
        );
    } else {
        said += "It runs from the next restart: `systemctl restart blocklyd`.\n";
    }
    Ok(said)
}
