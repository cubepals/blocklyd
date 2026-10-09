//! The node's durable identity, as enrollment left it:
//!
//! ```text
//! <state_dir>/identity/     0700
//!   node.json               node id, deployment, control plane, allowed clients, generation
//!   node.key                0600  the private key, made here and never sent anywhere
//!   node.pem                serverAuth certificate for the node's API
//!   client.pem              clientAuth certificate for heartbeats (the same key)
//!   ca.pem                  the fleet CA
//!   enroll-key.pem          0600  only until enrolled: the key every attempt asks with, which
//!                                 becomes node.key
//! ```
//!
//! Renewal (`Credentials::renew`) makes a new key and certificates as generation N, named
//! `node-N.key` and so on, and then rewrites `node.json` to name generation N: that one rename is
//! the switch, so a crash at any point leaves one whole generation in use, never a key of one and
//! a certificate of another.
//!
//! Losing this directory doesn't lose workloads (they are rebuilt from Docker's labels), but the
//! node can't prove who it is any more: it needs a new token and comes back as a new node.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use rustls_pki_types::CertificateDer;
use rustls_pki_types::pem::PemObject;
use serde::{Deserialize, Serialize};

use crate::api::tls::ServerCert;
use crate::config::TlsConfig;
use crate::protocol::wire::{RenewRequest, RenewResponse};
use crate::tls::{TlsSetupError, load_certs, load_key};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct IdentityFile {
    pub node_id: String,
    pub deployment_id: String,
    pub control_plane: String,
    pub allowed_clients: Vec<String>,
    pub enrolled_at: String,
    /// Which key and certificates are current: 0 as enrolled, one more per renewal.
    #[serde(default)]
    pub generation: u32,
}

#[derive(Clone, Debug)]
pub struct Identity {
    pub file: IdentityFile,
    pub dir: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum IdentityError {
    #[error("{path}: {problem}")]
    Io { path: PathBuf, problem: String },
    #[error(transparent)]
    Tls(#[from] TlsSetupError),
    #[error("{0}")]
    Invalid(String),
}

fn io(path: &Path) -> impl FnOnce(std::io::Error) -> IdentityError + '_ {
    move |e| IdentityError::Io { path: path.to_owned(), problem: e.to_string() }
}

/// Written whole or not at all, and on disk once this returns: a crash mid-enrollment leaves no
/// half an identity, and the key an attempt asks with is the key the next start finds.
pub(crate) fn write_atomic(path: &Path, contents: &[u8], mode: u32) -> Result<(), IdentityError> {
    crate::durable::write_atomic(path, contents, mode)
        .map_err(|e| IdentityError::Io { path: e.path, problem: e.source.to_string() })
}

fn make_dir(dir: &Path) -> Result<(), IdentityError> {
    fs::create_dir_all(dir).map_err(io(dir))?;
    fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(io(dir))
}

/// Whether `pem` holds exactly these certificates. Compared as DER: one certificate can be written
/// as PEM in more than one way.
pub fn same_certificates(pem: &str, certs: &[CertificateDer<'_>]) -> bool {
    let written: Result<Vec<CertificateDer<'_>>, _> = CertificateDer::pem_slice_iter(pem.as_bytes()).collect();
    written.is_ok_and(|written| {
        !written.is_empty() && written.iter().map(|c| c.as_ref()).eq(certs.iter().map(|c| c.as_ref()))
    })
}

impl Identity {
    pub fn dir_for(state_dir: &Path) -> PathBuf {
        state_dir.join("identity")
    }

    /// The identity on disk, if this node has enrolled.
    pub fn load(state_dir: &Path) -> Result<Option<Identity>, IdentityError> {
        let dir = Self::dir_for(state_dir);
        let meta = dir.join("node.json");
        if !meta.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(&meta).map_err(io(&meta))?;
        let file: IdentityFile =
            serde_json::from_str(&text).map_err(|e| IdentityError::Invalid(format!("{}: {e}", meta.display())))?;
        let identity = Identity { file, dir };
        // Everything must be there and loadable, or the node has no usable identity.
        identity.server_tls()?;
        identity.client_tls()?;
        Ok(Some(identity))
    }

    /// The key enrollment asks with: the one an earlier attempt made, or a new one, on disk before
    /// any attempt sends a request for it. The control plane answers a spent token again for the
    /// key that spent it, so a node whose answer was lost (a timeout, a reset connection, a
    /// restart) asks again with this key and gets its identity, where a new key would be refused.
    pub fn enrollment_key(state_dir: &Path) -> Result<rcgen::KeyPair, IdentityError> {
        let path = Self::enrollment_key_path(state_dir);
        match fs::read_to_string(&path) {
            Ok(pem) => {
                return rcgen::KeyPair::from_pem(&pem).map_err(|e| {
                    IdentityError::Invalid(format!("{}: {e}; remove it to enroll with a new key", path.display()))
                });
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(io(&path)(e)),
        }
        let key = rcgen::KeyPair::generate().map_err(|e| IdentityError::Invalid(format!("making a key: {e}")))?;
        make_dir(&Self::dir_for(state_dir))?;
        write_atomic(&path, key.serialize_pem().as_bytes(), 0o600)?;
        Ok(key)
    }

    /// Where the enrollment key waits: removed once it is `node.key`, and by a node that has an
    /// identity already, which never asks with it.
    pub fn enrollment_key_path(state_dir: &Path) -> PathBuf {
        Self::dir_for(state_dir).join("enroll-key.pem")
    }

    /// Stores what enrollment returned. The metadata file goes last, once the rest is known to
    /// work: it is what says "enrolled", and a node answered with something it can't use isn't.
    pub fn save(
        state_dir: &Path,
        file: IdentityFile,
        key_pem: &str,
        server_pem: &str,
        client_pem: &str,
        ca_pem: &str,
    ) -> Result<Identity, IdentityError> {
        let dir = Self::dir_for(state_dir);
        make_dir(&dir)?;
        write_atomic(&dir.join("node.key"), key_pem.as_bytes(), 0o600)?;
        write_atomic(&dir.join("node.pem"), server_pem.as_bytes(), 0o644)?;
        write_atomic(&dir.join("client.pem"), client_pem.as_bytes(), 0o644)?;
        write_atomic(&dir.join("ca.pem"), ca_pem.as_bytes(), 0o644)?;
        let identity = Identity { file, dir };
        identity.server_tls()?;
        identity.client_tls()?;
        let json = serde_json::to_vec_pretty(&identity.file).expect("serializes");
        write_atomic(&identity.dir.join("node.json"), &json, 0o644)?;
        Ok(identity)
    }

    fn named(&self, stem: &str, extension: &str) -> PathBuf {
        match self.file.generation {
            0 => self.dir.join(format!("{stem}.{extension}")),
            n => self.dir.join(format!("{stem}-{n}.{extension}")),
        }
    }

    pub fn key_path(&self) -> PathBuf {
        self.named("node", "key")
    }

    pub fn server_cert_path(&self) -> PathBuf {
        self.named("node", "pem")
    }

    pub fn client_cert_path(&self) -> PathBuf {
        self.named("client", "pem")
    }

    /// Stores a renewal as the next generation and switches to it. The old generation's files go
    /// once the switch is on disk.
    pub fn renewed(&self, key_pem: &str, server_pem: &str, client_pem: &str) -> Result<Identity, IdentityError> {
        let mut file = self.file.clone();
        file.generation = self.file.generation + 1;
        let next = Identity { file, dir: self.dir.clone() };
        write_atomic(&next.key_path(), key_pem.as_bytes(), 0o600)?;
        write_atomic(&next.server_cert_path(), server_pem.as_bytes(), 0o644)?;
        write_atomic(&next.client_cert_path(), client_pem.as_bytes(), 0o644)?;
        // Usable before it becomes current, both certificates for the new key: a bad renewal
        // leaves the current one alone.
        next.server_tls()?;
        next.client_tls()?;
        let json = serde_json::to_vec_pretty(&next.file).expect("serializes");
        write_atomic(&self.dir.join("node.json"), &json, 0o644)?;
        for old in [self.key_path(), self.server_cert_path(), self.client_cert_path()] {
            let _ = fs::remove_file(old);
        }
        Ok(next)
    }

    /// The API's TLS material, as a single-node config would name it. Its certificate must be for
    /// its key: one for another key loads just as well, and the API couldn't serve with it.
    pub fn server_tls(&self) -> Result<TlsConfig, IdentityError> {
        let tls = TlsConfig {
            cert: self.server_cert_path(),
            key: self.key_path(),
            client_ca: self.dir.join("ca.pem"),
            allowed_clients: self.file.allowed_clients.clone(),
        };
        ServerCert::load(&tls.cert, &tls.key)?;
        load_certs(&tls.client_ca)?;
        Ok(tls)
    }

    /// Mutual TLS to the control plane: the fleet CA as the only root, this node's client
    /// certificate as its identity. TLS 1.3 only.
    pub fn client_tls(&self) -> Result<Arc<rustls::ClientConfig>, IdentityError> {
        let roots = load_certs(&self.dir.join("ca.pem"))?;
        let chain = load_certs(&self.client_cert_path())?;
        let key = load_key(&self.key_path())?;
        crate::http_client::tls_config(roots, Some((chain, key)), true)
            .map_err(|e| IdentityError::Invalid(format!("client TLS: {e}")))
    }

    pub fn node_id(&self) -> &str {
        &self.file.node_id
    }
}

/// The identity in use, which renewal replaces while the node runs: the heartbeat's client
/// certificate for the next beat, the API's server certificate for the next connection.
pub struct Credentials {
    identity: Mutex<Identity>,
    client: RwLock<Arc<rustls::ClientConfig>>,
    server: Arc<ServerCert>,
    renewing: AtomicBool,
    last_attempt: Mutex<Option<Instant>>,
}

/// A failed renewal is tried again after this long, while the control plane keeps asking.
const RETRY_RENEWAL: Duration = Duration::from_secs(600);

impl Credentials {
    pub fn new(identity: Identity, server: Arc<ServerCert>) -> Result<Arc<Self>, IdentityError> {
        let client = identity.client_tls()?;
        Ok(Arc::new(Self {
            identity: Mutex::new(identity),
            client: RwLock::new(client),
            server,
            renewing: AtomicBool::new(false),
            last_attempt: Mutex::new(None),
        }))
    }

    pub fn client(&self) -> Arc<rustls::ClientConfig> {
        self.client.read().unwrap().clone()
    }

    pub fn node_id(&self) -> String {
        self.identity.lock().unwrap().node_id().to_owned()
    }

    pub fn generation(&self) -> u32 {
        self.identity.lock().unwrap().file.generation
    }

    /// Starts a renewal in the background, unless one runs or one failed recently.
    pub fn renew_soon(self: &Arc<Self>, control_plane: &str, manager: Arc<crate::manager::Manager>) {
        {
            let mut last = self.last_attempt.lock().unwrap();
            if last.is_some_and(|at| at.elapsed() < RETRY_RENEWAL) {
                return;
            }
            if self.renewing.swap(true, Ordering::SeqCst) {
                return;
            }
            *last = Some(Instant::now());
        }
        let this = self.clone();
        let url = control_plane.to_owned();
        tokio::spawn(async move {
            match this.renew(&url).await {
                Ok(generation) => {
                    manager.certificate_renewed();
                    manager.metrics.renewal(true);
                    tracing::info!(generation, "renewed the node's key and certificates");
                }
                Err(e) => {
                    manager.metrics.renewal(false);
                    tracing::warn!(
                        error = format!("{e:#}"),
                        "couldn't renew the node's certificates; the current ones stay"
                    );
                }
            }
            this.renewing.store(false, Ordering::SeqCst);
        });
    }

    /// A new key, certificates for it from the control plane (asked with the current ones), and
    /// the switch to them. Returns the new generation.
    pub async fn renew(&self, control_plane: &str) -> anyhow::Result<u32> {
        use anyhow::Context;
        let current = self.identity.lock().unwrap().clone();
        let key = rcgen::KeyPair::generate().context("making a new key")?;
        let csr = rcgen::CertificateParams::new(Vec::<String>::new())
            .context("certificate request")?
            .serialize_request(&key)
            .context("signing the certificate request")?
            .pem()
            .context("encoding the certificate request")?;
        let url = format!("{}/fleet/v1/nodes/{}/renew", control_plane.trim_end_matches('/'), current.node_id());
        let request = RenewRequest { node_id: current.node_id().to_owned(), csr_pem: csr };
        let answer: RenewResponse =
            crate::http_client::json(hyper::Method::POST, &url, &request, self.client(), Duration::from_secs(15))
                .await
                .context("asking the control plane")?;
        let ca = load_certs(&current.dir.join("ca.pem")).context("reading the fleet CA")?;
        anyhow::ensure!(
            same_certificates(&answer.ca_pem, &ca),
            "the control plane answered with another CA; a CA change needs the node re-provisioned"
        );
        let next = current
            .renewed(&key.serialize_pem(), &answer.server_cert_pem, &answer.client_cert_pem)
            .context("storing the renewed identity")?;
        self.server.replace(&next.server_cert_path(), &next.key_path()).context("switching the API's certificate")?;
        *self.client.write().unwrap() = next.client_tls()?;
        let generation = next.file.generation;
        *self.identity.lock().unwrap() = next;
        Ok(generation)
    }
}
