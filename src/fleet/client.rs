//! A small HTTP/1.1 client for the node's own calls out: to the control plane over mutual TLS,
//! and to presigned object-store URLs. One connection per call: a heartbeat every few seconds
//! and an archive now and then don't need a pool.
//!
//! A presigned URL carries its signature in the query string, so errors and logs name only the
//! scheme, host and path.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, Limited, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{CONTENT_LENGTH, HOST};
use hyper::{Method, Request, Response, Uri};
use hyper_util::rt::TokioIo;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::protocol::redact;

pub type Body = BoxBody<Bytes, std::io::Error>;

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("{0} is not an http(s) URL")]
    BadUrl(String),
    #[error("{0}: {1}")]
    Connect(String, std::io::Error),
    #[error("{0}: no answer within {1:?}")]
    Timeout(String, Duration),
    #[error("{0}: TLS: {1}")]
    Tls(String, std::io::Error),
    #[error("{0}: {1}")]
    Http(String, String),
    #[error("{0}: https needs TLS settings")]
    NoTls(String),
}

impl ClientError {
    /// Worth retrying: the far side may simply not be up yet.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Connect(..) | Self::Timeout(..) | Self::Http(..) => true,
            // What rustls refused (a certificate, the protocol) tokio-rustls reports as invalid
            // data, and rustls' own errors are what a retry would meet again. Anything else is the
            // connection failing mid-handshake (a reset, an early close), as it may at any time.
            Self::Tls(_, e) => {
                e.kind() != std::io::ErrorKind::InvalidData
                    && !e.get_ref().is_some_and(|inner| inner.is::<rustls::Error>())
            }
            Self::BadUrl(_) | Self::NoTls(_) => false,
        }
    }
}

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

pub fn full(bytes: impl Into<Bytes>) -> Body {
    Full::new(bytes.into()).map_err(|never| match never {}).boxed()
}

pub fn empty() -> Body {
    full(Bytes::new())
}

/// A file as a request body, streamed rather than read into memory.
pub async fn file_body(path: &Path) -> std::io::Result<(Body, u64)> {
    let file = tokio::fs::File::open(path).await?;
    let length = file.metadata().await?.len();
    let stream = tokio_util::io::ReaderStream::with_capacity(file, 256 * 1024).map(|chunk| chunk.map(Frame::data));
    Ok((BodyExt::boxed(StreamBody::new(stream)), length))
}

/// `length` bytes of a file from `offset` as a streaming body: one part of an upload in parts.
pub async fn file_range_body(path: &Path, offset: u64, length: u64) -> std::io::Result<Body> {
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut file = tokio::fs::File::open(path).await?;
    file.seek(std::io::SeekFrom::Start(offset)).await?;
    let stream =
        tokio_util::io::ReaderStream::with_capacity(file.take(length), 256 * 1024).map(|chunk| chunk.map(Frame::data));
    Ok(BodyExt::boxed(StreamBody::new(stream)))
}

/// Client TLS settings: these roots, and a client identity for mutual TLS if given.
pub fn tls_config(
    roots: Vec<CertificateDer<'static>>,
    identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    tls13_only: bool,
) -> Result<Arc<rustls::ClientConfig>, rustls::Error> {
    let mut store = rustls::RootCertStore::empty();
    for root in roots {
        store.add(root)?;
    }
    config_with(store, identity, tls13_only)
}

/// Client TLS settings over roots already chosen.
fn config_with(
    store: rustls::RootCertStore,
    identity: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
    tls13_only: bool,
) -> Result<Arc<rustls::ClientConfig>, rustls::Error> {
    let versions: &[&rustls::SupportedProtocolVersion] =
        if tls13_only { &[&rustls::version::TLS13] } else { rustls::DEFAULT_VERSIONS };
    let builder = rustls::ClientConfig::builder_with_provider(crate::api::tls::provider())
        .with_protocol_versions(versions)?
        .with_root_certificates(store);
    let mut config = match identity {
        Some((chain, key)) => builder.with_client_auth_cert(chain, key)?,
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(Arc::new(config))
}

/// TLS to public endpoints (a presigned object-store URL): the system's CA bundle, loaded once it
/// is found. Until then each transfer looks again, so a bundle installed after blocklyd started is
/// used without a restart.
pub fn public_tls() -> Result<Arc<rustls::ClientConfig>, String> {
    static CONFIG: std::sync::OnceLock<Arc<rustls::ClientConfig>> = std::sync::OnceLock::new();
    if let Some(config) = CONFIG.get() {
        return Ok(config.clone());
    }
    let bundle = ["/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/cert.pem", "/etc/pki/tls/certs/ca-bundle.crt"]
        .into_iter()
        .map(Path::new)
        .find(|p| p.exists())
        .ok_or("no system CA bundle found")?;
    let config = bundle_tls(bundle)?;
    Ok(CONFIG.get_or_init(|| config).clone())
}

/// TLS settings that trust the roots in `bundle`. A root that doesn't parse is left out, as rustls
/// advises for a system's store, rather than failing every transfer; the fleet CA, one root that
/// must parse, keeps the strict `tls_config`.
fn bundle_tls(bundle: &Path) -> Result<Arc<rustls::ClientConfig>, String> {
    let certs = crate::api::tls::load_certs(bundle).map_err(|e| e.to_string())?;
    let mut store = rustls::RootCertStore::empty();
    let (added, ignored) = store.add_parsable_certificates(certs);
    if added == 0 {
        return Err(format!("{}: holds no root that parses", bundle.display()));
    }
    if ignored > 0 {
        tracing::warn!(bundle = %bundle.display(), ignored, "left out roots of the system CA bundle that don't parse");
    }
    config_with(store, None, false).map_err(|e| e.to_string())
}

/// Sends one request on a fresh connection and returns once the response head arrives.
pub async fn send(
    method: Method,
    url: &str,
    headers: &[(&str, String)],
    body: Body,
    tls: Option<Arc<rustls::ClientConfig>>,
    timeout: Duration,
) -> Result<Response<Incoming>, ClientError> {
    let shown = redact(url);
    let uri: Uri = url.parse().map_err(|_| ClientError::BadUrl(shown.clone()))?;
    let https = match uri.scheme_str() {
        Some("https") => true,
        Some("http") => false,
        _ => return Err(ClientError::BadUrl(shown)),
    };
    let authority = uri.authority().ok_or_else(|| ClientError::BadUrl(shown.clone()))?.as_str().to_owned();
    let host = uri.host().ok_or_else(|| ClientError::BadUrl(shown.clone()))?;
    let bare_host = host.trim_start_matches('[').trim_end_matches(']').to_owned();
    let port = uri.port_u16().unwrap_or(if https { 443 } else { 80 });
    let target = if bare_host.contains(':') { format!("[{bare_host}]:{port}") } else { format!("{bare_host}:{port}") };

    let exchange = async {
        let tcp = TcpStream::connect(&target).await.map_err(|e| ClientError::Connect(shown.clone(), e))?;
        let _ = tcp.set_nodelay(true);
        let stream: Box<dyn Io> = if https {
            let tls = tls.ok_or_else(|| ClientError::NoTls(shown.clone()))?;
            let name =
                ServerName::try_from(bare_host.clone()).map_err(|_| ClientError::BadUrl(shown.clone()))?.to_owned();
            Box::new(TlsConnector::from(tls).connect(name, tcp).await.map_err(|e| ClientError::Tls(shown.clone(), e))?)
        } else {
            Box::new(tcp)
        };
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|e| ClientError::Http(shown.clone(), e.to_string()))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let path = uri.path_and_query().map_or("/", |p| p.as_str());
        let mut request = Request::builder().method(method).uri(path).header(HOST, authority);
        for (name, value) in headers {
            request = request.header(*name, value);
        }
        let request = request.body(body).map_err(|e| ClientError::Http(shown.clone(), e.to_string()))?;
        sender.send_request(request).await.map_err(|e| ClientError::Http(shown.clone(), e.to_string()))
    };
    tokio::time::timeout(timeout, exchange).await.map_err(|_| ClientError::Timeout(redact(url), timeout))?
}

/// Reads a whole (small) response body, refusing more than `limit` bytes.
pub async fn read_body(response: Response<Incoming>, limit: usize) -> Result<Bytes, String> {
    Limited::new(response.into_body(), limit)
        .collect()
        .await
        .map(|collected| collected.to_bytes())
        .map_err(|e| e.to_string())
}

/// A JSON request and its JSON answer, over mutual TLS. Non-2xx answers become an error that
/// carries the status and body. `timeout` bounds the whole exchange, the answer's body included:
/// a body that stops coming after its head would otherwise hold the caller (a heartbeat, an
/// enrollment) forever.
pub async fn json<Req: serde::Serialize, Resp: serde::de::DeserializeOwned>(
    method: Method,
    url: &str,
    request: &Req,
    tls: Arc<rustls::ClientConfig>,
    timeout: Duration,
) -> Result<Resp, JsonError> {
    let body = serde_json::to_vec(request).map_err(|e| JsonError::Local(e.to_string()))?;
    let headers = [("content-type", "application/json".to_owned()), (CONTENT_LENGTH.as_str(), body.len().to_string())];
    let exchange = async {
        let response =
            send(method, url, &headers, full(body), Some(tls), timeout).await.map_err(JsonError::Transport)?;
        let status = response.status();
        // A connection lost after the head is lost like any other: asked again.
        let bytes = read_body(response, 4 * 1024 * 1024)
            .await
            .map_err(|e| JsonError::Transport(ClientError::Http(redact(url), e)))?;
        Ok((status, bytes))
    };
    let (status, bytes) = tokio::time::timeout(timeout, exchange)
        .await
        .map_err(|_| JsonError::Transport(ClientError::Timeout(redact(url), timeout)))??;
    if !status.is_success() {
        return Err(JsonError::Status(status.as_u16(), String::from_utf8_lossy(&bytes).chars().take(500).collect()));
    }
    serde_json::from_slice(&bytes).map_err(|e| JsonError::Local(format!("unexpected answer: {e}")))
}

#[derive(Debug, thiserror::Error)]
pub enum JsonError {
    #[error(transparent)]
    Transport(ClientError),
    #[error("HTTP {0}: {1}")]
    Status(u16, String),
    #[error("{0}")]
    Local(String),
}

impl JsonError {
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Transport(e) => e.is_transient(),
            Self::Status(code, _) => *code >= 500 || *code == 429,
            Self::Local(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A server that reads one request and answers every connection with `answer`, raw, then
    /// writes nothing more: it closes the connection if `close`, and otherwise holds it open.
    async fn answering(answer: &'static [u8], close: bool) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/fleet/v1/nodes/n1/heartbeat", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut socket, _)) = listener.accept().await {
                let _ = socket.read(&mut [0u8; 4096]).await;
                let _ = socket.write_all(answer).await;
                if !close {
                    held.push(socket);
                }
            }
        });
        url
    }

    /// Calls `url` as a heartbeat does, under `limit`; the test fails rather than hangs.
    async fn ask(url: &str, limit: Duration) -> Result<serde_json::Value, JsonError> {
        let tls = tls_config(Vec::new(), None, true).unwrap();
        tokio::time::timeout(limit + Duration::from_secs(5), json(Method::POST, url, &(), tls, limit))
            .await
            .expect("waited past the caller's limit")
    }

    #[tokio::test]
    async fn an_answer_whose_body_stops_coming_ends_within_the_callers_limit() {
        let head = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\n\r\n{\"lifecycle\"";
        let url = answering(head, false).await;
        let err = ask(&url, Duration::from_millis(300)).await.unwrap_err();
        assert!(matches!(err, JsonError::Transport(ClientError::Timeout(..))), "{err}");
        assert!(err.is_transient(), "asked again, as any answer that didn't come");
    }

    #[tokio::test]
    async fn an_answer_cut_short_is_asked_again_and_one_that_isnt_json_is_not() {
        let cut = b"HTTP/1.1 200 OK\r\ncontent-length: 100\r\n\r\n{\"lifecycle\"";
        let err = ask(&answering(cut, true).await, Duration::from_secs(5)).await.unwrap_err();
        assert!(matches!(err, JsonError::Transport(ClientError::Http(..))), "{err}");
        assert!(err.is_transient());
        let garbled = b"HTTP/1.1 200 OK\r\ncontent-length: 9\r\n\r\nnot json!";
        let err = ask(&answering(garbled, false).await, Duration::from_secs(5)).await.unwrap_err();
        assert!(matches!(err, JsonError::Local(_)), "{err}");
        assert!(!err.is_transient(), "the same answer would come again");
    }

    #[test]
    fn a_root_that_doesnt_parse_leaves_the_rest_of_the_system_bundle_trusted() {
        let dir = tempfile::tempdir().unwrap();
        let bundle = dir.path().join("ca-certificates.crt");
        // Three bytes where a certificate should be.
        let broken = "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n";
        let good = crate::certs::authority("a public CA").unwrap().pem;
        std::fs::write(&bundle, format!("{broken}{good}")).unwrap();
        bundle_tls(&bundle).expect("the root that parses is trusted");
        let roots = crate::api::tls::load_certs(&bundle).unwrap();
        assert!(tls_config(roots, None, true).is_err(), "a fleet CA that doesn't parse is still refused");
        std::fs::write(&bundle, broken).unwrap();
        let none = bundle_tls(&bundle).unwrap_err();
        assert!(none.contains("holds no root that parses"), "{none}");
    }

    #[test]
    fn a_tls_failure_is_final_only_when_rustls_refused() {
        use std::io::{Error, ErrorKind};
        let tls = |e: Error| ClientError::Tls("https://cp.example".into(), e);
        let unknown = rustls::Error::InvalidCertificate(rustls::CertificateError::UnknownIssuer);
        assert!(!tls(Error::new(ErrorKind::InvalidData, unknown)).is_transient(), "a certificate from another CA");
        assert!(
            !tls(Error::other(rustls::Error::General("no versions".into()))).is_transient(),
            "settings rustls refused"
        );
        assert!(tls(Error::new(ErrorKind::UnexpectedEof, "tls handshake eof")).is_transient(), "closed mid-handshake");
        assert!(tls(Error::from(ErrorKind::ConnectionReset)).is_transient());
        assert!(!ClientError::BadUrl("ftp://cp.example".into()).is_transient());
        assert!(!ClientError::NoTls("https://cp.example".into()).is_transient());
    }
}
