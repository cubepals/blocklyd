//! `blocklyd join <token>`: makes this host a node of the fleet a `bk1.` token names
//! (fleet/token.rs), with nothing else to hand. `join.sh`, which the node endpoint serves, runs it
//! once Docker and the binary are in place (docs/fleet-operations.md, "Adding a node").
//!
//! 1. It fetches the fleet CA from the token's endpoint and checks it against the token's hash,
//!    the only thing that vouches for it: nothing is written if it doesn't match.
//! 2. It writes `/etc/blocklyd/blocklyd.toml` with only what the host can't work out (the
//!    deployment and the endpoint), `/etc/blocklyd/fleet-ca.pem`, the token as
//!    `/var/lib/blocklyd/enrollment-token` (0600) and the systemd unit, each whole or not at all.
//!    A configuration already there is kept: what an operator set wins.
//! 3. It runs `blocklyd doctor --preflight`, shows what it found and stops on a failure, then
//!    enables and starts the unit. blocklyd enrolls on its first start, as it always has.
//!
//! What `Config::load` works out from the host (infer.rs) is worked out here first, so a host
//! with two private addresses is asked which before anything is written, and it is printed with
//! how to override it. On a host already enrolled into the token's deployment it changes nothing,
//! unless the token re-enrolls the node this host is: then reenroll.rs swaps its identity, and its
//! fleet CA when the token names a new one. A token for another node, or a host enrolled in
//! another deployment, is refused.

use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use hyper::Method;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};

use super::reenroll::{Reenroll, reenroll};
use super::service::{Service, Systemd};
use crate::config::Config;
use crate::doctor;
use crate::fleet::identity::{Identity, write_atomic};
use crate::fleet::token::JoinToken;
use crate::http_client as client;
use crate::infer::API_PORT;

/// The paths as the host sees them; under `root` when one is given.
pub const CONFIG: &str = "/etc/blocklyd/blocklyd.toml";
const CA: &str = "/etc/blocklyd/fleet-ca.pem";
const TOKEN: &str = "/var/lib/blocklyd/enrollment-token";
const UNIT: &str = "/etc/systemd/system/blocklyd.service";

const UNIT_FILE: &str = include_str!("../../deploy/blocklyd.service");

/// How long a re-enrolling node has for its first heartbeat (reenroll.rs says what happens after):
/// enrollment retries every 30 s at most, and a heartbeat follows within seconds.
const FIRST_BEAT: Duration = Duration::from_secs(120);

pub struct Join {
    /// The token as pasted.
    pub token: String,
    /// The address to serve the fleet on, where the host's own can't be told (infer.rs).
    pub address: Option<IpAddr>,
    /// The configuration to write, or to keep: `CONFIG`, but elsewhere in tests.
    pub config: PathBuf,
    /// Where `/` is: another directory in tests.
    pub root: PathBuf,
    /// Enables and starts the unit once everything is written.
    pub start: bool,
    /// `systemctl`, or what stands in for it in tests that run blocklyd as a plain process.
    pub systemctl: PathBuf,
}

/// Does it, and returns what to tell the operator.
pub async fn join(options: Join) -> anyhow::Result<String> {
    let token = JoinToken::decode(&options.token)
        .map_err(|e| anyhow!("{e}. blocklyd join takes the bk1. token `bun scripts/fleet.ts token` prints"))?;
    let root = options.root.as_path();
    if root == Path::new("/") && !rustix::process::geteuid().is_root() {
        bail!("blocklyd join writes to /etc and installs a service: run it as root");
    }
    let config_file = options.config.as_path();
    let shown = config_file.display();
    let existing = std::fs::read_to_string(under(root, config_file)).ok();
    let text = existing.clone().unwrap_or_else(|| configuration(&token, options.address));
    let config = Config::parse(&text, config_file, root)?;
    let state_dir = under(root, &config.state_dir);
    let enrolled = Identity::load(&state_dir)?;
    if let Some(identity) = &enrolled {
        let (node, deployment) = (identity.node_id(), &identity.file.deployment_id);
        if *deployment != token.deployment_id {
            bail!(
                "this host is already node {node} of deployment {deployment}, and the token is for {}. To move it, \
                 retire it there (`bun scripts/fleet.ts retire {node}`), remove {} and {shown}, and join again",
                token.deployment_id,
                Identity::dir_for(&config.state_dir).display()
            );
        }
        match token.node.as_deref() {
            None => {
                return Ok(format!(
                    "This host is already node {node} of deployment {deployment}: nothing changed. To enroll it \
                     again under its own id, paste the line `bun scripts/fleet.ts token --node {node}` prints.\n"
                ));
            }
            Some(other) if other != node => bail!(
                "this host is node {node}, and the token re-enrolls node {other}: paste it on that node's host. \
                 For this one, `bun scripts/fleet.ts token --node {node}` prints the line"
            ),
            Some(_) if existing.is_none() => {
                let identity = Identity::dir_for(&config.state_dir);
                bail!(
                    "node {node} has no {shown} to start with: put it back, or remove {} and join afresh",
                    identity.display()
                )
            }
            Some(_) if !options.start => {
                bail!("re-enrolling restarts blocklyd and waits for its first heartbeat: run it without --no-start")
            }
            Some(_) => {}
        }
    }

    let Some(fleet) = &config.fleet else {
        bail!("{shown} runs blocklyd alone, without [fleet]: remove it to join a fleet");
    };
    if config.deployment_id != token.deployment_id {
        bail!(
            "{shown} is for deployment {}, and the token for {}: remove it to join {}",
            config.deployment_id,
            token.deployment_id,
            token.deployment_id
        );
    }
    let token_file = fleet.enrollment_token_file.as_deref().unwrap_or(Path::new(TOKEN));
    let ca = fetch_ca(&token).await?;
    let service = Systemd { program: options.systemctl.clone() };

    if let Some(identity) = &enrolled {
        preflight(root, config_file).await?;
        let swap = Reenroll {
            node: identity.node_id(),
            state_dir,
            ca_file: under(root, &fleet.ca),
            token_file: under(root, token_file),
            ca: &ca,
            token: &options.token,
            ops: config.ops.listen,
            within: FIRST_BEAT,
        };
        return reenroll(swap, &service).await;
    }

    let mut written = Vec::new();
    for (path, contents, mode) in [
        (config_file, existing.is_none().then_some(text.as_bytes()), 0o644),
        (fleet.ca.as_path(), Some(ca.as_slice()), 0o644),
        (token_file, Some(options.token.trim().as_bytes()), 0o600),
        (Path::new(UNIT), Some(UNIT_FILE.as_bytes()), 0o644),
    ] {
        let Some(contents) = contents else { continue };
        let target = under(root, path);
        let dir = target.parent().context("a path with a directory")?;
        std::fs::create_dir_all(dir).with_context(|| format!("making {}", dir.display()))?;
        write_atomic(&target, contents, mode)?;
        written.push(path.display().to_string());
    }
    if options.start {
        preflight(root, config_file)
            .await
            .with_context(|| format!("wrote {}, and left blocklyd stopped", written.join(", ")))?;
        service.systemctl(&["daemon-reload"])?;
        service.systemctl(&["enable", "blocklyd"])?;
        // A start that failed before this join (an old token) is retried with the new one.
        service.systemctl(&["restart", "blocklyd"])?;
    }
    Ok(report(&token, &config, config_file, existing.is_some(), &written, options.start))
}

/// `blocklyd doctor --preflight`, as the unit runs it before each start: what it finds is shown
/// now, and a failure stops the join before blocklyd is started.
async fn preflight(root: &Path, config_file: &Path) -> anyhow::Result<()> {
    let report = doctor::run(&doctor::Options { config: under(root, config_file), preflight: true, fix: false }).await;
    print!("{}", report.text(true));
    anyhow::ensure!(
        report.ok(),
        "doctor found what keeps blocklyd from starting (above): fix it, and run the line again"
    );
    Ok(())
}

fn under(root: &Path, path: &Path) -> PathBuf {
    root.join(path.strip_prefix("/").unwrap_or(path))
}

/// The configuration `join` writes: the deployment and where its control plane is, and the
/// address only when the operator named it. Everything else is a default or worked out.
fn configuration(token: &JoinToken, address: Option<IpAddr>) -> String {
    let quoted = |text: &str| toml::Value::String(text.to_owned()).to_string();
    let mut text = format!(
        "# Written by `blocklyd join`. Every key left out has a default or is worked out from this host\n\
         # at each start, as `blocklyd doctor` lists; a key set here wins.\n\
         # https://github.com/cubepals/blocklyd/blob/main/examples/blocklyd.toml lists every key.\n\
         deployment_id = {}\n",
        quoted(&token.deployment_id)
    );
    if let Some(ip) = address {
        text += &format!(
            "\n[api]\nlisten = \"{}\"\n\n[network]\nedge_ips = [\"{ip}\"]\ncontrol_ips = [\"{ip}\"]\n",
            SocketAddr::new(ip, API_PORT)
        );
    }
    text + &format!("\n[fleet]\nurl = {}\nca = \"{CA}\"\nenrollment_token_file = \"{TOKEN}\"\n", quoted(&token.url))
}

fn report(token: &JoinToken, config: &Config, file: &Path, kept: bool, written: &[String], started: bool) -> String {
    let mut out = format!("Joined this host to deployment {}, at {}.\n", token.deployment_id, token.url);
    if !config.inferred.is_empty() {
        out += &format!("Worked out from this host; set any of them in {} to override:\n", file.display());
        for inferred in &config.inferred {
            out += &format!("  {} = {}    ({})\n", inferred.key, inferred.value, inferred.from);
        }
    }
    if kept {
        out += &format!("Kept {} as it was.\n", file.display());
    }
    out += &format!("Wrote {}.\n", written.join(", "));
    out += if started {
        "blocklyd is running and enrolls on its own: `bun scripts/fleet.ts nodes` shows it. Its log: journalctl -u blocklyd\n"
    } else {
        "Start it with: systemctl enable --now blocklyd\n"
    };
    out
}

/// The fleet CA as the token's endpoint serves it, once it is the one the token names.
async fn fetch_ca(token: &JoinToken) -> anyhow::Result<Vec<u8>> {
    let url = format!("{}/fleet/v1/ca.pem", token.url.trim_end_matches('/'));
    let provider = crate::api::tls::provider();
    let mut tls = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedByHash(provider)))
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"http/1.1".to_vec()];
    let response = client::send(Method::GET, &url, &[], client::empty(), Some(Arc::new(tls)), Duration::from_secs(15))
        .await
        .with_context(|| format!("fetching the fleet CA from {url}"))?;
    let status = response.status();
    let body = client::read_body(response, 64 * 1024).await.map_err(|e| anyhow!("fetching {url}: {e}"))?;
    if !status.is_success() {
        bail!("{url} answered HTTP {status}");
    }
    if !token.names_ca(&body) {
        bail!(
            "the CA at {url} is not the one the token names (sha256 {}), so nothing was written: check that the \
             token's address reaches this deployment's control plane",
            token.ca_sha256
        );
    }
    Ok(body.to_vec())
}

/// Takes whatever certificate the endpoint shows, for the one request that fetches the CA: what
/// it answers is trusted only once its sha256 is the token's (`fetch_ca`). The handshake's own
/// signatures are still checked.
#[derive(Debug)]
struct PinnedByHash(Arc<CryptoProvider>);

impl ServerCertVerifier for PinnedByHash {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
