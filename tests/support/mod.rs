//! Shared fixtures: a manager over the fake runtime, certificates, an mTLS HTTP client, and bounded
//! waits.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use blocklyd::api::{self, AppState};
use blocklyd::certs;
use blocklyd::config::Config;
use blocklyd::ids::WorkloadId;
use blocklyd::manager::{Manager, Records};
use blocklyd::metrics::Metrics;
use blocklyd::protocol::{WorkloadSpec, WorkloadView};
use blocklyd::runtime::ContainerRuntime;
use blocklyd::runtime::fake::FakeRuntime;
use blocklyd::store::Store;
use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper_util::rt::TokioIo;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_util::sync::CancellationToken;

pub(crate) const CLIENT: &str = "control-plane.test.blockly.internal";

pub(crate) fn id(s: &str) -> WorkloadId {
    WorkloadId::parse(s).unwrap()
}

/// A config over `dir`, with test-friendly capacity; `extra` is appended TOML.
pub(crate) fn config(dir: &Path, extra: &str) -> Config {
    // Data goes to the test's own user, so these tests run without root. On a host it goes to the
    // workloads' user, never root: tests/docker.rs says so, and checks it, as root. Root may not own
    // a workload's data, so a test run as root gives it to the workloads' user, as a host does.
    let (uid, gid) = (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw());
    let owner = if uid == 0 { String::new() } else { format!("data_owner = \"{uid}:{gid}\"") };
    let text = format!(
        r#"
node_id = "test-node"
deployment_id = "test"
state_dir = "{state}"
[api]
listen = "127.0.0.1:0"
[api.tls]
cert = "{tls}/node.pem"
key = "{tls}/node.key"
client_ca = "{tls}/ca.pem"
allowed_clients = ["{CLIENT}"]
[workloads]
allowed_images = ["alpine:", "itzg/minecraft-server:"]
min_memory_mb = 64
{owner}
[capacity]
min_free_disk_mb = 64
[network]
port_quarantine_seconds = 600
"#,
        state = dir.join("state").display(),
        tls = dir.join("tls").display(),
        // The free-disk floor is a host's (5 GB); tests keep a small one, whatever disk they run on.
    );
    // A test's own TOML is merged over the base, table by table.
    let mut base: toml::Table = toml::from_str(&text).unwrap();
    let extra: toml::Table = toml::from_str(extra).unwrap_or_else(|e| panic!("test config: {e}\n{extra}"));
    merge(&mut base, extra);
    let config: Config = base.try_into().unwrap_or_else(|e| panic!("test config: {e}"));
    config.validate().unwrap();
    config
}

fn merge(into: &mut toml::Table, from: toml::Table) {
    for (key, value) in from {
        match (into.get_mut(&key), value) {
            (Some(toml::Value::Table(a)), toml::Value::Table(b)) => merge(a, b),
            (_, value) => {
                into.insert(key, value);
            }
        }
    }
}

pub(crate) struct Fixture {
    pub dir: tempfile::TempDir,
    pub fake: Arc<FakeRuntime>,
    pub manager: Arc<Manager>,
}

/// A manager that has reconciled once, as blocklyd does before it serves anyone.
pub(crate) async fn fixture(extra: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let fake = Arc::new(FakeRuntime::new());
    let manager = manager_on(dir.path(), fake.clone(), extra);
    let report = manager.reconcile(Records::FromDisk).await;
    assert!(report.error.is_none(), "{report:?}");
    Fixture { dir, fake, manager }
}

/// A manager over an existing directory and runtime: a "restarted daemon" is another one.
pub(crate) fn manager_on(dir: &Path, runtime: Arc<dyn ContainerRuntime>, extra: &str) -> Arc<Manager> {
    manager_probed(dir, runtime, extra, Arc::new(|_, _| true))
}

pub(crate) fn manager_probed(
    dir: &Path,
    runtime: Arc<dyn ContainerRuntime>,
    extra: &str,
    probe: blocklyd::ports::Probe,
) -> Arc<Manager> {
    let config = Arc::new(config(dir, extra));
    let store = Store::open(&config.state_dir).unwrap();
    Manager::new(config, runtime, store, Arc::new(Metrics::new()), probe)
}

pub(crate) fn spec_json() -> serde_json::Value {
    serde_json::json!({
        "image": "alpine:3.22",
        "env": { "EULA": "TRUE" },
        "secrets": { "RCON_PASSWORD": "first-secret" },
        "resources": { "memoryMb": 1024 },
        "storage": { "mountPath": "/data", "sizeGb": 1 },
        "ports": [
            { "name": "game", "containerPort": 25565, "audience": ["edge", "control"] },
            { "name": "rcon", "containerPort": 25575, "audience": ["control"] }
        ],
        "stop": { "timeoutSeconds": 5 },
        "labels": { "blockly.server": "s1" }
    })
}

pub(crate) fn spec() -> WorkloadSpec {
    serde_json::from_value(spec_json()).unwrap()
}

pub(crate) fn spec_with(f: impl FnOnce(&mut serde_json::Value)) -> WorkloadSpec {
    let mut v = spec_json();
    f(&mut v);
    serde_json::from_value(v).unwrap()
}

// ─── waiting ────────────────────────────────────────────────────────────────────────────────

/// Looks every 25 ms until `look` says what a test waits for has happened, for up to 30 s: time
/// enough for a slow CI runner. A test that waits in vain fails with what it last saw rather than
/// hanging. `look` answers whether it has, and what it saw.
pub(crate) async fn eventually<T: std::fmt::Debug>(what: &str, mut look: impl AsyncFnMut() -> (bool, T)) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let (happened, seen) = look().await;
        if happened {
            return seen;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}; last saw {seen:#?}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// A workload's view once `ok` holds, looked at as blocklyd looks (`stats` observes the runtime).
pub(crate) async fn until(
    manager: &Arc<Manager>,
    workload: &str,
    what: &str,
    ok: impl Fn(&WorkloadView) -> bool,
) -> WorkloadView {
    eventually(what, async || {
        manager.stats(&id(workload)).await.ok();
        let view = manager.view(&id(workload)).unwrap();
        (ok(&view), view)
    })
    .await
}

// ─── TLS ────────────────────────────────────────────────────────────────────────────────────

pub(crate) struct Pki {
    pub dir: PathBuf,
    pub ca: certs::Authority,
}

pub(crate) fn pki(dir: &Path) -> Pki {
    let tls = dir.join("tls");
    std::fs::create_dir_all(&tls).unwrap();
    let ca = certs::authority("test CA").unwrap();
    std::fs::write(tls.join("ca.pem"), &ca.pem).unwrap();
    let (cert, key) =
        certs::leaf(&ca, &["localhost".into()], &["127.0.0.1".parse().unwrap()], certs::Role::Server).unwrap();
    std::fs::write(tls.join("node.pem"), &cert).unwrap();
    write_key(&tls.join("node.key"), &key);
    Pki { dir: tls, ca }
}

fn write_key(path: &Path, key: &str) {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut f = std::fs::OpenOptions::new().create(true).truncate(true).write(true).mode(0o600).open(path).unwrap();
    f.write_all(key.as_bytes()).unwrap();
}

/// A client identity: certificate chain and key, as PEM.
pub(crate) struct Identity {
    pub cert: String,
    pub key: String,
}

pub(crate) fn client(ca: &certs::Authority, name: &str) -> Identity {
    let (cert, key) = certs::leaf(ca, &[name.into()], &[], certs::Role::Client).unwrap();
    Identity { cert, key }
}

/// A server (serverAuth) certificate presented as a client: must be refused.
pub(crate) fn server_cert_as_client(ca: &certs::Authority) -> Identity {
    let (cert, key) = certs::leaf(ca, &[CLIENT.into()], &[], certs::Role::Server).unwrap();
    Identity { cert, key }
}

pub(crate) struct Server {
    pub addr: SocketAddr,
    pub cancel: CancellationToken,
    pub task: tokio::task::JoinHandle<()>,
}

pub(crate) async fn serve(manager: Arc<Manager>) -> Server {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let config = manager.config.clone();
    let tls_material = config.api.tls.clone().expect("tests run single-node");
    let tls = api::tls::server_config(&tls_material).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let app = api::router(AppState { manager: manager.clone() }, config.api.max_body_bytes);
    let task = tokio::spawn(api::serve_tls(
        listener,
        tls,
        tls_material.allowed_clients.clone(),
        app,
        manager,
        cancel.clone(),
        Duration::from_secs(2),
    ));
    Server { addr, cancel, task }
}

pub(crate) struct Response {
    pub status: u16,
    pub headers: hyper::HeaderMap,
    pub body: Vec<u8>,
}

impl Response {
    pub(crate) fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| panic!("{e}: {}", String::from_utf8_lossy(&self.body)))
    }
    pub(crate) fn code(&self) -> String {
        self.json()["error"]["code"].as_str().unwrap_or_default().to_owned()
    }
}

/// One HTTPS request as `identity` (or anonymously). Err is a refused handshake.
pub(crate) async fn request(
    addr: SocketAddr,
    ca_pem: &str,
    identity: Option<&Identity>,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: Option<Vec<u8>>,
) -> Result<Response, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
        roots.add(cert.unwrap()).unwrap();
    }
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots);
    let config = match identity {
        Some(id) => {
            let chain: Vec<_> = CertificateDer::pem_slice_iter(id.cert.as_bytes()).map(Result::unwrap).collect();
            let key = PrivateKeyDer::from_pem_slice(id.key.as_bytes()).unwrap();
            builder.with_client_auth_cert(chain, key).unwrap()
        }
        None => builder.with_no_client_auth(),
    };
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let tcp = tokio::net::TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
    let tls = connector.connect(ServerName::try_from("localhost").unwrap(), tcp).await.map_err(|e| e.to_string())?;
    let (mut sender, conn) =
        hyper::client::conn::http1::handshake(TokioIo::new(tls)).await.map_err(|e| e.to_string())?;
    tokio::spawn(conn);
    let mut req = hyper::Request::builder().method(method).uri(path).header("host", "localhost");
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    if body.is_some() {
        req = req.header("content-type", "application/json");
    }
    let req = req.body(Full::new(Bytes::from(body.unwrap_or_default()))).unwrap();
    // With TLS 1.3 a refused client certificate surfaces on the first read, not the handshake.
    let response = sender.send_request(req).await.map_err(|e| e.to_string())?;
    let status = response.status().as_u16();
    let headers = response.headers().clone();
    let body = response.into_body().collect().await.map_err(|e| e.to_string())?.to_bytes().to_vec();
    Ok(Response { status, headers, body })
}
