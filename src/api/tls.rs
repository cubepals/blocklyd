//! Mutual TLS: the only way into the workload API.
//!
//! Why mTLS rather than bearer tokens: blocklyd holds a root-equivalent socket, so its API is
//! worth attacking. With mTLS no secret crosses the wire (a leaked request log reveals nothing),
//! both sides are authenticated, captured traffic can't be replayed (TLS 1.3, no 0-RTT), and it is
//! what node agents with this much power already use (kubelet, Nomad, Docker's own TCP socket).
//!
//! Three checks, all of which must pass:
//! 1. The client's chain leads to the configured client CA (webpki).
//! 2. Its certificate is for client authentication (extended key usage clientAuth, checked by
//!    webpki): a node's server certificate from the same CA can't be used to call another node.
//! 3. One of its DNS names is in `allowed_clients`: a valid certificate for someone else (another
//!    deployment, an edge) is refused with 403.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use rustls::RootCertStore;
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::config::TlsConfig;

/// Who is on the other end of a connection, attached to each request it sends.
#[derive(Clone, Debug, Default)]
pub struct ClientIdentity {
    pub names: Vec<String>,
    /// The allowed name it matched, if any.
    pub allowed: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum TlsSetupError {
    #[error("{path}: {problem}")]
    File { path: String, problem: String },
    #[error("TLS configuration: {0}")]
    Rustls(String),
}

fn file_err(path: &Path, problem: impl ToString) -> TlsSetupError {
    TlsSetupError::File { path: path.display().to_string(), problem: problem.to_string() }
}

pub fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsSetupError> {
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .map_err(|e| file_err(path, e))?
        .collect::<Result<_, _>>()
        .map_err(|e| file_err(path, e))?;
    if certs.is_empty() {
        return Err(file_err(path, "holds no certificate"));
    }
    Ok(certs)
}

pub fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsSetupError> {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path).map_err(|e| file_err(path, e))?.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(file_err(path, format!("is readable by others (mode {:o}); chmod 600 it", mode & 0o777)));
    }
    PrivateKeyDer::from_pem_file(path).map_err(|e| file_err(path, e))
}

pub fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The API's own certificate, replaceable while serving: a renewed one is used for new
/// connections, and connections already open keep the one they started with.
#[derive(Debug)]
pub struct ServerCert(std::sync::RwLock<Arc<CertifiedKey>>);

impl ServerCert {
    pub fn load(cert: &Path, key: &Path) -> Result<Self, TlsSetupError> {
        Ok(Self(std::sync::RwLock::new(Arc::new(certified_key(cert, key)?))))
    }

    /// Switches to the certificate and key in these files, once they are known to match.
    pub fn replace(&self, cert: &Path, key: &Path) -> Result<(), TlsSetupError> {
        let next = Arc::new(certified_key(cert, key)?);
        *self.0.write().unwrap() = next;
        Ok(())
    }
}

impl ResolvesServerCert for ServerCert {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.0.read().unwrap().clone())
    }
}

fn certified_key(cert: &Path, key: &Path) -> Result<CertifiedKey, TlsSetupError> {
    CertifiedKey::from_der(load_certs(cert)?, load_key(key)?, &provider())
        .map_err(|e| TlsSetupError::Rustls(format!("{}: {e}", cert.display())))
}

/// TLS 1.3 only, client certificates required, no early data (0-RTT is replayable).
pub fn server_config(tls: &TlsConfig) -> Result<Arc<rustls::ServerConfig>, TlsSetupError> {
    Ok(reloadable_server_config(tls)?.0)
}

/// `server_config`, with a handle that replaces its certificate (fleet mode renews it).
pub fn reloadable_server_config(
    tls: &TlsConfig,
) -> Result<(Arc<rustls::ServerConfig>, Arc<ServerCert>), TlsSetupError> {
    let provider = provider();
    let cert = Arc::new(ServerCert::load(&tls.cert, &tls.key)?);
    let mut roots = RootCertStore::empty();
    for ca in load_certs(&tls.client_ca)? {
        roots.add(ca).map_err(|e| TlsSetupError::Rustls(e.to_string()))?;
    }
    let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .build()
        .map_err(|e| TlsSetupError::Rustls(e.to_string()))?;
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| TlsSetupError::Rustls(e.to_string()))?
        .with_client_cert_verifier(verifier)
        .with_cert_resolver(cert.clone());
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    config.max_early_data_size = 0;
    Ok((Arc::new(config), cert))
}

/// The identity a verified client certificate proves. `allowed` is matched against the DNS
/// names webpki considers valid for the certificate.
pub fn identify(certs: Option<&[CertificateDer<'static>]>, allowed: &[String]) -> ClientIdentity {
    let Some(leaf) = certs.and_then(|c| c.first()) else { return ClientIdentity::default() };
    let Ok(cert) = webpki::EndEntityCert::try_from(leaf) else { return ClientIdentity::default() };
    let names: Vec<String> = cert.valid_dns_names().map(str::to_owned).collect();
    let allowed = allowed.iter().find(|a| names.iter().any(|n| n.eq_ignore_ascii_case(a))).cloned();
    ClientIdentity { names, allowed }
}
