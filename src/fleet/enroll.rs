//! Joining the fleet. A new host arrives with two things from its provisioning: a one-time
//! enrollment token and the fleet CA's certificate. It makes its own key, sends a certificate
//! request with the token, and gets back a durable node id and two certificates for that key.
//!
//! - The CA certificate is the only trust anchor for the control plane, so a node never trusts
//!   whoever answers first. The answer must name that same CA: the node trusts it from then on.
//! - The token authorizes exactly one enrollment, and a node never sends it again once enrolled.
//!   A pasted `bk1.` token (`token.rs`) must name this node's deployment and its CA file; only its
//!   secret is sent.
//!   Until then every attempt asks for the same key, across restarts too, so an answer lost on the
//!   way can be asked for again: the control plane answers a spent token again for the key that
//!   spent it.
//! - The key never leaves the node.
//! - The control plane, not the request, decides the node's name. A CSR's subject and names are
//!   ignored.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use hyper::Method;
use tracing::{info, warn};

use super::identity::{Identity, IdentityFile, same_certificates};
use crate::api::tls::load_certs;
use crate::config::{Config, FleetConfig};
use crate::http_client::{self as client, JsonError};
use crate::protocol::wire::{EnrollRequest, EnrollResponse, NodeFacts};

/// The node's identity: the one on disk, or a new one from enrollment. Retries while the
/// control plane can't be reached (a host is often up before it is); gives up on a refusal.
pub async fn ensure_identity(config: &Config, fleet: &FleetConfig, facts: NodeFacts) -> anyhow::Result<Identity> {
    if let Some(identity) = Identity::load(&config.state_dir)? {
        if identity.file.deployment_id != config.deployment_id {
            bail!(
                "this node is enrolled as {} in deployment {}, but configured for {}. To move it, retire it there \
                 (`bun scripts/fleet.ts retire {}`), remove {}, and paste the line `bun scripts/fleet.ts token` \
                 prints for {}",
                identity.node_id(),
                identity.file.deployment_id,
                config.deployment_id,
                identity.node_id(),
                identity.dir.display(),
                config.deployment_id
            );
        }
        // Left by a crash between storing the identity and removing it: never asked with again.
        forget_enrollment_key(&config.state_dir);
        return Ok(identity);
    }
    let token_file = fleet.enrollment_token_file.as_deref().with_context(|| {
        format!(
            "not enrolled ({} holds no identity) and fleet.enrollment_token_file is not set",
            Identity::dir_for(&config.state_dir).display()
        )
    })?;
    let token = std::fs::read_to_string(token_file)
        .with_context(|| format!("reading the enrollment token {}", token_file.display()))?
        .trim()
        .to_owned();
    if token.is_empty() {
        bail!("the enrollment token {} is empty", token_file.display());
    }
    let token =
        secret_of(&token, config, fleet).with_context(|| format!("the enrollment token {}", token_file.display()))?;

    let key = Identity::enrollment_key(&config.state_dir).context("the enrollment key")?;
    let csr = rcgen::CertificateParams::new(Vec::<String>::new())
        .context("certificate request")?
        .serialize_request(&key)
        .context("signing the certificate request")?
        .pem()
        .context("encoding the certificate request")?;
    let roots = load_certs(&fleet.ca).context("the fleet CA certificate")?;
    let tls = client::tls_config(roots.clone(), None, true).context("TLS to the control plane")?;
    let url = format!("{}/fleet/v1/enroll", fleet.url.trim_end_matches('/'));
    let request = EnrollRequest { token, csr_pem: csr, facts };

    let mut wait = Duration::from_secs(1);
    let response: EnrollResponse = loop {
        match client::json(Method::POST, &url, &request, tls.clone(), Duration::from_secs(15)).await {
            Ok(response) => break response,
            Err(e) if e.is_transient() => {
                warn!(error = %e, retry_in_s = wait.as_secs(), "enrollment: control plane not reachable yet");
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(Duration::from_secs(30));
            }
            Err(JsonError::Status(code, body)) => bail!("the control plane refused enrollment (HTTP {code}): {body}"),
            Err(e) => bail!("enrollment failed: {e}"),
        }
    };

    if response.deployment_id != config.deployment_id {
        bail!(
            "the control plane enrolled this node into deployment {}, not {}",
            response.deployment_id,
            config.deployment_id
        );
    }
    // The CA the answer names is the one this node trusts from now on, for the control plane and
    // for who may call its API: only the one it was provisioned with, which vouched for this
    // connection, will do.
    if !same_certificates(&response.ca_pem, &roots) {
        bail!(
            "the control plane answered with a CA certificate that isn't the one in fleet.ca ({}), and this node \
             trusts no other, so it wrote no identity. fleet.ca must be the certificate of the CA this control \
             plane issues node certificates with",
            fleet.ca.display()
        );
    }
    let file = IdentityFile {
        node_id: response.node_id.clone(),
        deployment_id: response.deployment_id.clone(),
        control_plane: fleet.url.clone(),
        allowed_clients: response.allowed_clients.clone(),
        enrolled_at: crate::runtime::format_time(crate::manager::now()),
        generation: 0,
    };
    let identity = Identity::save(
        &config.state_dir,
        file,
        &key.serialize_pem(),
        &response.server_cert_pem,
        &response.client_cert_pem,
        &response.ca_pem,
    )?;
    // It is node.key now.
    forget_enrollment_key(&config.state_dir);
    // Spent either way; removing it keeps it from being read off this disk later.
    if let Err(e) = std::fs::remove_file(token_file) {
        warn!(error = %e, path = %token_file.display(), "couldn't remove the spent enrollment token; remove it by hand");
    }
    info!(node = %identity.node_id(), "enrolled");
    Ok(identity)
}

/// What is sent for a token: a bare one whole, a join token's secret once its deployment and CA
/// are the ones this node is configured with.
fn secret_of(text: &str, config: &Config, fleet: &FleetConfig) -> anyhow::Result<String> {
    let Some(join) = super::token::read(text)? else {
        return Ok(text.to_owned());
    };
    if join.deployment_id != config.deployment_id {
        bail!("it is for deployment {}, and this node is configured for {}", join.deployment_id, config.deployment_id);
    }
    let ca = std::fs::read(&fleet.ca).with_context(|| format!("reading fleet.ca {}", fleet.ca.display()))?;
    if !join.names_ca(&ca) {
        bail!(
            "it names a fleet CA other than {} (sha256 {}): the token and the CA come from different control planes",
            fleet.ca.display(),
            join.ca_sha256
        );
    }
    Ok(join.secret)
}

fn forget_enrollment_key(state_dir: &Path) {
    let path = Identity::enrollment_key_path(state_dir);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => warn!(error = %e, path = %path.display(), "couldn't remove the enrollment key; remove it by hand"),
    }
}

/// sha256 of /etc/machine-id: equal on two nodes means they were cloned from one image.
pub fn machine_id_sha256() -> Option<String> {
    use sha2::Digest;
    let id = std::fs::read_to_string(Path::new("/etc/machine-id")).ok()?;
    let id = id.trim();
    (!id.is_empty()).then(|| hex::encode(sha2::Sha256::digest(id.as_bytes())))
}
