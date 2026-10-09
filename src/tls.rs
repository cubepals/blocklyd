//! Certificates and keys, read from their PEM files, and the crypto provider every TLS setting is
//! built on: the API's server side (`api::tls`), the node's calls out (`http_client`), enrollment,
//! `blocklyd join` and doctor.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};

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

pub(crate) fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsSetupError> {
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .map_err(|e| file_err(path, e))?
        .collect::<Result<_, _>>()
        .map_err(|e| file_err(path, e))?;
    if certs.is_empty() {
        return Err(file_err(path, "holds no certificate"));
    }
    Ok(certs)
}

pub(crate) fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsSetupError> {
    use std::os::unix::fs::PermissionsExt;
    let mode = fs::metadata(path).map_err(|e| file_err(path, e))?.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(file_err(path, format!("is readable by others (mode {:o}); chmod 600 it", mode & 0o777)));
    }
    PrivateKeyDer::from_pem_file(path).map_err(|e| file_err(path, e))
}

pub(crate) fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}
