//! The node's side of the control plane's endpoint (docs/protocol.md, "The control plane's
//! endpoint"): joining with a pasted token, enrollment, and renewal when a heartbeat answer asks
//! for it. Against a control
//! plane served in-process over TLS: a fleet CA of its own vouches for it and signs the
//! certificates it answers with, for the key in the node's request.

mod support;

use std::collections::HashMap;
use std::future::Future;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use blocklyd::cli::upgrade::Upgrader;
use blocklyd::config::{Config, FleetConfig};
use blocklyd::fleet::enroll::ensure_identity;
use blocklyd::fleet::heartbeat;
use blocklyd::fleet::identity::{Credentials, Identity, IdentityFile};
use blocklyd::protocol::wire::{
    EnrollRequest, EnrollResponse, HeartbeatResponse, NodeCapacity, NodeFacts, RenewRequest, RenewResponse,
};
use blocklyd::runtime::fake::FakeRuntime;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use rcgen::{
    BasicConstraints, CertificateParams, CertificateSigningRequestParams, DnType, ExtendedKeyUsagePurpose, IsCa,
    Issuer, KeyPair, KeyUsagePurpose, PublicKeyData,
};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_util::sync::CancellationToken;

/// A fleet CA, as the control plane holds it: its key signs every node's certificates.
struct Ca {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

impl Ca {
    fn new(name: &str) -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::DigitalSignature];
        params.distinguished_name.push(DnType::CommonName, name);
        let pem = params.self_signed(&key).unwrap().pem();
        Self { issuer: Issuer::new(params, key), pem }
    }

    /// A certificate for `key` under `name`: serverAuth, or clientAuth.
    fn issue(&self, key: &impl PublicKeyData, name: &str, server: bool) -> String {
        let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
        params.extended_key_usages =
            vec![if server { ExtendedKeyUsagePurpose::ServerAuth } else { ExtendedKeyUsagePurpose::ClientAuth }];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.signed_by(key, &self.issuer).unwrap().pem()
    }
}

/// How the control plane answers an enrollment.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Answer {
    /// As a control plane does.
    Issue,
    /// Spends the token and never answers: the node waits until it is restarted.
    Never,
    /// Spends the token and drops the connection: the answer is lost on the way.
    Lost,
    /// Names a CA the node wasn't provisioned with.
    OtherCa,
    /// Names the fleet CA, its PEM written with CRLF line ends.
    Crlf,
    /// A server certificate for another key than the one asked for.
    WrongKey,
}

/// How the control plane answers a renewal.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
enum Renewal {
    /// As a control plane does.
    #[default]
    Issue,
    /// As a control plane does, the fleet CA's PEM written with CRLF line ends.
    Crlf,
    /// Refuses: the control plane is down, say.
    Refuse,
    /// Names a CA the node wasn't provisioned with.
    OtherCa,
    /// A server certificate for another key than the one asked for.
    WrongKey,
}

/// What the control plane holds, and what it was asked.
#[derive(Default)]
struct State {
    /// Each token, and who spent it: the key its request was for, and the node it made.
    tokens: HashMap<String, Option<(Vec<u8>, String)>>,
    /// How the next enrollments are answered, in order; then `Issue`.
    answers: Vec<Answer>,
    /// Every enrollment asked for: its token, and the key its request was for.
    enrollments: Vec<(String, Vec<u8>)>,
    /// Whether heartbeat answers ask the node to renew, and how its renewal is answered.
    renew: bool,
    renewal: Renewal,
    /// Every heartbeat: the client certificate it came with.
    beats: Vec<Vec<u8>>,
    /// Every renewal asked for: the client certificate it came with, and the key its request was
    /// for.
    renewals: Vec<(Vec<u8>, Vec<u8>)>,
}

struct Shared {
    ca: Ca,
    state: Mutex<State>,
}

struct ControlPlane {
    url: String,
    shared: Arc<Shared>,
}

impl ControlPlane {
    async fn start() -> Self {
        let shared = Arc::new(Shared { ca: Ca::new("test fleet CA"), state: Mutex::default() });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("https://{}", listener.local_addr().unwrap());
        let acceptor = tokio_rustls::TlsAcceptor::from(endpoint_tls(&shared.ca));
        let serving = shared.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let (acceptor, shared) = (acceptor.clone(), serving.clone());
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(tcp).await else { return };
                    let peer = tls.get_ref().1.peer_certificates().and_then(|c| c.first()).map(|c| c.to_vec());
                    let service =
                        hyper::service::service_fn(move |request| answer(shared.clone(), peer.clone(), request));
                    let _ = http1::Builder::new().serve_connection(TokioIo::new(tls), service).await;
                });
            }
        });
        Self { url, shared }
    }

    fn ca_pem(&self) -> &str {
        &self.shared.ca.pem
    }

    /// A new one-time token.
    fn token(&self) -> String {
        let token = uuid::Uuid::new_v4().to_string();
        self.shared.state.lock().unwrap().tokens.insert(token.clone(), None);
        token
    }

    fn answer_next(&self, answer: Answer) {
        self.shared.state.lock().unwrap().answers.push(answer);
    }

    fn enrollments(&self) -> Vec<(String, Vec<u8>)> {
        self.shared.state.lock().unwrap().enrollments.clone()
    }

    /// The node a token made, once spent.
    fn node_of(&self, token: &str) -> Option<String> {
        self.shared.state.lock().unwrap().tokens.get(token).cloned().flatten().map(|(_, node)| node)
    }

    /// From now on heartbeat answers ask the node to renew, and its renewal is answered so.
    fn ask_to_renew(&self, renewal: Renewal) {
        let mut state = self.shared.state.lock().unwrap();
        (state.renew, state.renewal) = (true, renewal);
    }

    fn beats(&self) -> Vec<Vec<u8>> {
        self.shared.state.lock().unwrap().beats.clone()
    }

    fn renewals(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.shared.state.lock().unwrap().renewals.clone()
    }

    async fn until(&self, what: &str, done: impl Fn(&State) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let finished = done(&self.shared.state.lock().unwrap());
            if finished {
                return;
            }
            assert!(Instant::now() < deadline, "the control plane never saw {what}");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The endpoint's own TLS: a certificate for 127.0.0.1 from the fleet CA, and client certificates
/// from it checked when one is presented (an enrollment presents none).
fn endpoint_tls(ca: &Ca) -> Arc<rustls::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let key = KeyPair::generate().unwrap();
    let chain = vec![CertificateDer::from_pem_slice(ca.issue(&key, "127.0.0.1", true).as_bytes()).unwrap()];
    let mut roots = rustls::RootCertStore::empty();
    roots.add(CertificateDer::from_pem_slice(ca.pem.as_bytes()).unwrap()).unwrap();
    let clients = rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider.clone())
        .allow_unauthenticated()
        .build()
        .unwrap();
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_client_cert_verifier(clients)
        .with_single_cert(chain, PrivateKeyDer::from_pem_slice(key.serialize_pem().as_bytes()).unwrap())
        .unwrap();
    Arc::new(config)
}

type Answered = Result<Response<Full<Bytes>>, std::io::Error>;

fn json(status: u16, body: &impl serde::Serialize) -> Answered {
    let body = Full::new(Bytes::from(serde_json::to_vec(body).unwrap()));
    Ok(Response::builder().status(status).header("content-type", "application/json").body(body).unwrap())
}

fn refused(status: u16, code: &str) -> Answered {
    json(status, &serde_json::json!({ "error": { "code": code, "message": code } }))
}

/// `peer` is the client certificate the connection came with, if any.
async fn answer(shared: Arc<Shared>, peer: Option<Vec<u8>>, request: Request<Incoming>) -> Answered {
    let path = request.uri().path().to_owned();
    let body = request.into_body().collect().await.map_err(std::io::Error::other)?.to_bytes();
    match (path.as_str(), peer) {
        ("/fleet/v1/enroll", _) => enroll(&shared, serde_json::from_slice(&body).unwrap()).await,
        ("/fleet/v1/ca.pem", _) => Ok(Response::new(Full::new(Bytes::from(shared.ca.pem.clone())))),
        (path, None) if path.starts_with("/fleet/v1/nodes/") => refused(401, "certificate_required"),
        (path, Some(peer)) if path.ends_with("/heartbeat") => {
            let mut state = shared.state.lock().unwrap();
            state.beats.push(peer);
            let answer = HeartbeatResponse {
                lifecycle: "active".into(),
                fences: Vec::new(),
                heartbeat_seconds: Some(1),
                lease_seconds: Some(120),
                renew: state.renew,
                upgrade: None,
            };
            json(200, &answer)
        }
        (path, Some(peer)) if path.ends_with("/renew") => renew(&shared, peer, serde_json::from_slice(&body).unwrap()),
        _ => refused(404, "no_route"),
    }
}

/// As the control plane enrolls: a token is spent once, and a spent one is answered again only
/// for the key that spent it, with fresh certificates for the same node.
async fn enroll(shared: &Shared, request: EnrollRequest) -> Answered {
    let key = CertificateSigningRequestParams::from_pem(&request.csr_pem).expect("a valid request").public_key;
    let (node, answer) = {
        let mut state = shared.state.lock().unwrap();
        state.enrollments.push((request.token.clone(), key.der_bytes().to_vec()));
        let node = match state.tokens.get(&request.token) {
            None => return refused(403, "invalid_token"),
            Some(None) => uuid::Uuid::new_v4().to_string(),
            Some(Some((spender, node))) if spender.as_slice() == key.der_bytes() => node.clone(),
            Some(Some(_)) => return refused(403, "invalid_token"),
        };
        state.tokens.insert(request.token, Some((key.der_bytes().to_vec(), node.clone())));
        let answer = if state.answers.is_empty() { Answer::Issue } else { state.answers.remove(0) };
        (node, answer)
    };
    let ca_pem = match answer {
        Answer::Never => return std::future::pending().await,
        Answer::Lost => return Err(std::io::Error::other("the answer is lost on the way")),
        Answer::OtherCa => Ca::new("another CA").pem,
        Answer::Crlf => shared.ca.pem.replace('\n', "\r\n"),
        Answer::Issue | Answer::WrongKey => shared.ca.pem.clone(),
    };
    let name = format!("{node}.nodes.test.fleet");
    let server_cert_pem = match answer {
        Answer::WrongKey => shared.ca.issue(&KeyPair::generate().unwrap(), &name, true),
        _ => shared.ca.issue(&key, &name, true),
    };
    json(
        200,
        &EnrollResponse {
            node_id: node,
            deployment_id: "test".into(),
            server_cert_pem,
            client_cert_pem: shared.ca.issue(&key, &name, false),
            ca_pem,
            allowed_clients: vec![support::CLIENT.into()],
            heartbeat_seconds: 5,
        },
    )
}

/// New certificates for the key in the request, asked for with the node's current certificate.
fn renew(shared: &Shared, peer: Vec<u8>, request: RenewRequest) -> Answered {
    let key = CertificateSigningRequestParams::from_pem(&request.csr_pem).expect("a valid request").public_key;
    let renewal = {
        let mut state = shared.state.lock().unwrap();
        state.renewals.push((peer, key.der_bytes().to_vec()));
        state.renewal
    };
    let name = format!("{}.nodes.test.fleet", request.node_id);
    let (server_cert_pem, ca_pem) = match renewal {
        Renewal::Refuse => return refused(503, "unavailable"),
        Renewal::Issue => (shared.ca.issue(&key, &name, true), shared.ca.pem.clone()),
        Renewal::Crlf => (shared.ca.issue(&key, &name, true), shared.ca.pem.replace('\n', "\r\n")),
        Renewal::OtherCa => (shared.ca.issue(&key, &name, true), Ca::new("another CA").pem),
        Renewal::WrongKey => (shared.ca.issue(&KeyPair::generate().unwrap(), &name, true), shared.ca.pem.clone()),
    };
    json(200, &RenewResponse { server_cert_pem, client_cert_pem: shared.ca.issue(&key, &name, false), ca_pem })
}

/// A host provisioned to join: its configuration, the fleet CA's certificate and a token.
struct Node {
    dir: tempfile::TempDir,
    config: Config,
    fleet: FleetConfig,
    token: String,
}

impl Node {
    fn new(cp: &ControlPlane) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = support::config(dir.path(), "");
        let token = cp.token();
        let token_file = dir.path().join("enrollment-token");
        std::fs::write(&token_file, &token).unwrap();
        let ca = dir.path().join("fleet-ca.pem");
        std::fs::write(&ca, cp.ca_pem()).unwrap();
        let fleet = FleetConfig {
            url: cp.url.clone(),
            ca,
            enrollment_token_file: Some(token_file),
            heartbeat_seconds: 1,
            api_address: None,
            labels: Default::default(),
            restart_requires_contact_seconds: 120,
        };
        Self { dir, config, fleet, token }
    }

    /// One start of blocklyd, as far as its identity: `serve` runs this before anything else.
    fn enrolling(&self) -> impl Future<Output = anyhow::Result<Identity>> + use<> {
        let (config, fleet) = (self.config.clone(), self.fleet.clone());
        async move { ensure_identity(&config, &fleet, facts()).await }
    }

    async fn enroll(&self) -> anyhow::Result<Identity> {
        tokio::time::timeout(Duration::from_secs(20), self.enrolling()).await.expect("enrollment never ended")
    }

    fn identity_dir(&self) -> PathBuf {
        Identity::dir_for(&self.config.state_dir)
    }

    fn token_file(&self) -> PathBuf {
        self.dir.path().join("enrollment-token")
    }

    /// blocklyd serving under this identity, as `serve` sets it up: the API's TLS, and a
    /// heartbeat every second.
    async fn serve(&self, identity: Identity, cp: &ControlPlane) -> Serving {
        let (api, certificate) = blocklyd::api::tls::reloadable_server_config(&identity.server_tls().unwrap()).unwrap();
        let credentials = Credentials::new(identity, certificate).unwrap();
        let manager = support::manager_on(self.dir.path(), Arc::new(FakeRuntime::new()), "");
        manager.reconcile(true).await;
        let stop = CancellationToken::new();
        let upgrader = Upgrader::new(&self.config.state_dir, &cp.url, Arc::default(), stop.clone());
        tokio::spawn(heartbeat::run(manager, credentials.clone(), upgrader, cp.url.clone(), 1, stop.clone()));
        Serving { api, credentials, stop }
    }
}

struct Serving {
    api: Arc<rustls::ServerConfig>,
    credentials: Arc<Credentials>,
    stop: CancellationToken,
}

impl Serving {
    /// The certificate the API presents to a new connection, from the control plane.
    async fn presents(&self, cp: &ControlPlane, name: &str) -> Vec<u8> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(self.api.clone());
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let _ = acceptor.accept(tcp).await;
        });
        let key = KeyPair::generate().unwrap();
        let chain =
            vec![CertificateDer::from_pem_slice(cp.shared.ca.issue(&key, support::CLIENT, false).as_bytes()).unwrap()];
        let mut roots = rustls::RootCertStore::empty();
        roots.add(CertificateDer::from_pem_slice(cp.ca_pem().as_bytes()).unwrap()).unwrap();
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_client_auth_cert(chain, PrivateKeyDer::from_pem_slice(key.serialize_pem().as_bytes()).unwrap())
            .unwrap();
        let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
        let name = ServerName::try_from(name.to_owned()).unwrap();
        let tls = tokio_rustls::TlsConnector::from(Arc::new(config)).connect(name, tcp).await.unwrap();
        tls.get_ref().1.peer_certificates().unwrap()[0].to_vec()
    }
}

fn facts() -> NodeFacts {
    NodeFacts {
        hostname: "test-host".into(),
        boot_id: None,
        machine_id_sha256: None,
        daemon_version: env!("CARGO_PKG_VERSION").into(),
        protocol: blocklyd::protocol::ProtocolVersions { current: 1, supported: vec![1] },
        features: Vec::new(),
        api_address: "127.0.0.1:7443".parse().unwrap(),
        edge_ips: Vec::new(),
        control_ips: Vec::new(),
        capacity: NodeCapacity::default(),
        labels: Default::default(),
    }
}

/// The public key of the private key in a PEM file.
fn public_key(path: &Path) -> Vec<u8> {
    KeyPair::from_pem(&std::fs::read_to_string(path).unwrap()).unwrap().der_bytes().to_vec()
}

fn certificates(pem: &[u8]) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_slice_iter(pem).map(Result::unwrap).collect()
}

/// The certificate in a PEM file, as DER: what a TLS peer sees.
fn certificate(path: &Path) -> Vec<u8> {
    certificates(&std::fs::read(path).unwrap()).remove(0).to_vec()
}

#[tokio::test]
async fn only_the_ca_a_node_was_provisioned_with_is_trusted() {
    let cp = ControlPlane::start().await;
    // An answer naming another CA: the node would trust it for the control plane, and for who may
    // call its API. Refused, and nothing written.
    let node = Node::new(&cp);
    cp.answer_next(Answer::OtherCa);
    let refused = node.enroll().await.expect_err("enrolled under another CA").to_string();
    assert!(refused.contains("fleet.ca"), "{refused}");
    assert!(Identity::load(&node.config.state_dir).unwrap().is_none());
    assert!(!node.identity_dir().join("ca.pem").exists());
    assert!(node.token_file().exists(), "not enrolled, so the token stays");

    // The fleet CA written another way (CRLF line ends) is the same certificate.
    let other = Node::new(&cp);
    cp.answer_next(Answer::Crlf);
    let identity = other.enroll().await.unwrap();
    let written = std::fs::read(identity.dir.join("ca.pem")).unwrap();
    assert_eq!(certificates(&written), certificates(cp.ca_pem().as_bytes()));
}

#[tokio::test]
async fn an_answer_lost_on_the_way_is_asked_for_again_with_the_same_key() {
    let cp = ControlPlane::start().await;
    let node = Node::new(&cp);
    // The first attempt spends the token and its answer never comes: blocklyd is restarted while
    // it waits.
    cp.answer_next(Answer::Never);
    tokio::select! {
        result = node.enrolling() => panic!("enrolled without an answer: {result:?}"),
        () = cp.until("the first attempt", |s| s.enrollments.len() == 1) => {}
    }
    // Its key was on disk before it was asked for, readable by root alone.
    let waiting = node.identity_dir().join("enroll-key.pem");
    assert_eq!(public_key(&waiting), cp.enrollments()[0].1);
    assert_eq!(std::fs::metadata(&waiting).unwrap().permissions().mode() & 0o777, 0o600);
    // The next start asks again with the spent token, and loses that answer too: the connection
    // drops once the control plane has answered. It asks once more.
    cp.answer_next(Answer::Lost);
    let identity = node.enroll().await.expect("enrolled with the token the first attempt spent");

    let asked = cp.enrollments();
    assert_eq!(asked.len(), 3, "the first start's attempt, then the second start's two");
    assert!(asked.iter().all(|request| *request == asked[0]), "one token, and one key, every time");
    assert_eq!(identity.node_id(), cp.node_of(&node.token).unwrap(), "the node the first attempt made");
    assert_eq!(public_key(&identity.key_path()), asked[0].1, "its key is the one every attempt asked with");
    assert!(!node.identity_dir().join("enroll-key.pem").exists(), "it became node.key");
    assert!(!node.token_file().exists(), "spent, and removed");
}

#[tokio::test]
async fn an_answer_the_node_cant_serve_with_leaves_it_unenrolled_to_ask_again() {
    let cp = ControlPlane::start().await;
    let node = Node::new(&cp);
    // A server certificate for another key: it loads like any other, and the API can't use it.
    cp.answer_next(Answer::WrongKey);
    node.enroll().await.expect_err("enrolled with a server certificate for another key");
    assert!(Identity::load(&node.config.state_dir).unwrap().is_none(), "not enrolled, so the next start asks again");
    // It does, with the same key and the token the first answer spent, and can serve.
    let identity = node.enroll().await.unwrap();
    blocklyd::api::tls::reloadable_server_config(&identity.server_tls().unwrap()).unwrap();
    let asked = cp.enrollments();
    assert_eq!((asked.len(), &asked[0]), (2, &asked[1]));
}

#[tokio::test]
async fn a_handshake_cut_short_is_tried_again_and_one_a_stranger_vouches_for_is_not() {
    let cp = ControlPlane::start().await;
    // An endpoint whose certificate another CA issued: refused at the handshake, and not again.
    let node = Node::new(&cp);
    std::fs::write(&node.fleet.ca, Ca::new("another CA").pem).unwrap();
    let refused = node.enroll().await.expect_err("enrolled with an endpoint it can't trust").to_string();
    assert!(refused.contains("TLS"), "{refused}");
    assert!(cp.enrollments().is_empty());
    // One that closes the first connection before saying anything, as a balancer in front of a
    // restarting control plane may: asked again, and enrolled.
    let mut node = Node::new(&cp);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    node.fleet.url = format!("https://{}", listener.local_addr().unwrap());
    let upstream = cp.url.trim_start_matches("https://").to_owned();
    tokio::spawn(async move {
        drop(listener.accept().await.unwrap());
        while let Ok((mut tcp, _)) = listener.accept().await {
            let mut to = tokio::net::TcpStream::connect(&upstream).await.unwrap();
            tokio::spawn(async move { tokio::io::copy_bidirectional(&mut tcp, &mut to).await });
        }
    });
    let identity = node.enroll().await.expect("enrolled once the endpoint answered");
    assert_eq!(identity.node_id(), cp.node_of(&node.token).unwrap());
}

#[tokio::test]
async fn a_node_with_an_identity_never_uses_an_enrollment_key() {
    let cp = ControlPlane::start().await;
    let node = Node::new(&cp);
    let identity = node.enroll().await.unwrap();
    let key = std::fs::read(identity.key_path()).unwrap();
    // What a crash between storing the identity and removing the enrollment key leaves.
    let leftover = node.identity_dir().join("enroll-key.pem");
    std::fs::write(&leftover, KeyPair::generate().unwrap().serialize_pem()).unwrap();

    let again = node.enroll().await.unwrap();
    assert_eq!(again.node_id(), identity.node_id());
    assert_eq!(std::fs::read(again.key_path()).unwrap(), key, "its own key, not the leftover");
    assert!(!leftover.exists(), "removed rather than kept around");
    assert_eq!(cp.enrollments().len(), 1, "never asked to enroll again");
}

#[tokio::test]
async fn asked_to_renew_a_node_makes_a_new_key_and_switches_to_it_whole() {
    let cp = ControlPlane::start().await;
    let node = Node::new(&cp);
    let enrolled = node.enroll().await.unwrap();
    let (key, client) = (public_key(&enrolled.key_path()), certificate(&enrolled.client_cert_path()));
    let replaced = [enrolled.key_path(), enrolled.server_cert_path(), enrolled.client_cert_path()];
    let name = format!("{}.nodes.test.fleet", enrolled.node_id());
    // Its answer writes the fleet CA another way, as a control plane upgraded since the node
    // enrolled might: the same CA all the same.
    cp.ask_to_renew(Renewal::Crlf);
    let serving = node.serve(enrolled, &cp).await;
    cp.until("a beat with a renewed certificate", |s| s.beats.last().is_some_and(|c| *c != client)).await;
    serving.stop.cancel();

    let renewals = cp.renewals();
    assert_eq!(renewals.len(), 1);
    let (asked_with, new_key) = &renewals[0];
    assert_eq!(*asked_with, client, "asked for over its current certificate");
    assert_ne!(*new_key, key, "for a new key");
    // node.json names the next generation, whose key is the one asked for, and the replaced
    // generation is gone.
    let renewed = Identity::load(&node.config.state_dir).unwrap().expect("an identity");
    assert_eq!(renewed.file.generation, 1);
    assert_eq!(serving.credentials.generation(), 1);
    assert_eq!(public_key(&renewed.key_path()), *new_key);
    assert_eq!(std::fs::metadata(renewed.key_path()).unwrap().permissions().mode() & 0o777, 0o600);
    for file in replaced {
        assert!(!file.exists(), "{} is still there", file.display());
    }
    // Heartbeats carry the new client certificate, and the API presents the new server one.
    assert_eq!(cp.beats().last(), Some(&certificate(&renewed.client_cert_path())));
    assert_eq!(serving.presents(&cp, &name).await, certificate(&renewed.server_cert_path()));
}

#[tokio::test]
async fn a_renewal_that_fails_leaves_the_node_on_its_current_identity() {
    let cp = ControlPlane::start().await;
    let node = Node::new(&cp);
    let enrolled = node.enroll().await.unwrap();
    let client = certificate(&enrolled.client_cert_path());
    let files = ["node.json", "node.key", "node.pem", "client.pem", "ca.pem"].map(|f| node.identity_dir().join(f));
    let on_disk = || files.iter().map(|f| std::fs::read(f).ok()).collect::<Vec<_>>();
    let before = on_disk();

    // Refused: the node beats on with its current certificate, and doesn't ask again at once,
    // however often the control plane asks it to renew.
    cp.ask_to_renew(Renewal::Refuse);
    let serving = node.serve(enrolled, &cp).await;
    cp.until("three beats", |s| s.beats.len() >= 3).await;
    serving.stop.cancel();
    assert_eq!(cp.renewals().len(), 1);
    assert!(cp.beats().iter().all(|c| *c == client), "every beat came with the current certificate");
    assert_eq!(on_disk(), before);

    // Answers the node can't use: another CA, or a server certificate for another key.
    for answer in [Renewal::OtherCa, Renewal::WrongKey] {
        cp.ask_to_renew(answer);
        serving.credentials.renew(&cp.url).await.expect_err("renewed with an answer it can't use");
        assert_eq!(serving.credentials.generation(), 0);
        assert_eq!(on_disk(), before, "{answer:?} changed the identity on disk");
    }
    // A restart finds the identity it had, and can serve with it.
    let identity = Identity::load(&node.config.state_dir).unwrap().expect("an identity");
    assert_eq!(identity.file.generation, 0);
    blocklyd::api::tls::reloadable_server_config(&identity.server_tls().unwrap()).unwrap();
    identity.client_tls().unwrap();
}

/// A token as `scripts/fleet.ts token` prints it, for this control plane's secret; with
/// `--node`, it names the node it re-enrolls.
fn pasted(cp: &ControlPlane, secret: &str, deployment: &str, ca_pem: &str, node: Option<&str>) -> String {
    use base64::Engine;
    use sha2::Digest;
    let json = serde_json::json!({
        "u": cp.url,
        "h": hex::encode(sha2::Sha256::digest(ca_pem.as_bytes())),
        "d": deployment,
        "s": secret,
        "n": node,
    });
    format!("bk1.{}", base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(json.to_string()))
}

#[tokio::test]
async fn a_pasted_token_sends_only_its_secret_once_it_names_this_deployment_and_ca() {
    let cp = ControlPlane::start().await;
    let node = Node::new(&cp);
    let write = |deployment: &str, ca_pem: &str| {
        std::fs::write(node.token_file(), pasted(&cp, &node.token, deployment, ca_pem, None)).unwrap();
    };
    write("production", cp.ca_pem());
    let refused = format!("{:#}", node.enroll().await.expect_err("enrolled into another deployment"));
    assert!(refused.contains("deployment production"), "{refused}");
    write("test", &Ca::new("another CA").pem);
    let refused = format!("{:#}", node.enroll().await.expect_err("enrolled under another CA"));
    assert!(refused.contains("different control planes"), "{refused}");
    assert!(cp.enrollments().is_empty(), "nothing is sent for a token this node can't use");

    write("test", cp.ca_pem());
    let identity = node.enroll().await.unwrap();
    assert_eq!(cp.enrollments()[0].0, node.token, "the secret alone, as a bare token is sent");
    assert_eq!(identity.node_id(), cp.node_of(&node.token).unwrap());
    assert!(!node.token_file().exists(), "spent, and removed");
}

/// A host's /proc and /sys under `root`, as far as `join` reads them: one address per interface,
/// on its own /16, and 8 GB of memory.
fn host(root: &Path, addresses: &[(&str, [u8; 4])]) {
    let (mut trie, mut route) = (String::from("Main:\n"), String::from("Iface\tDestination\tGateway\tFlags\n"));
    for (interface, ip) in addresses {
        trie += &format!("     |-- {}\n        /32 host LOCAL\n", std::net::Ipv4Addr::from(*ip));
        let network = u32::from_ne_bytes([ip[0], ip[1], 0, 0]);
        let mask = u32::from_ne_bytes([255, 255, 0, 0]);
        route += &format!("{interface}\t{network:08X}\t00000000\t0001\t0\t0\t0\t{mask:08X}\t0\t0\t0\n");
        std::fs::create_dir_all(root.join("sys/class/net").join(interface)).unwrap();
    }
    std::fs::create_dir_all(root.join("proc/net")).unwrap();
    std::fs::write(root.join("proc/net/fib_trie"), trie).unwrap();
    std::fs::write(root.join("proc/net/route"), route).unwrap();
    std::fs::write(root.join("proc/meminfo"), "MemTotal:        8388608 kB\n").unwrap();
}

async fn join(root: &Path, token: String, address: Option<std::net::IpAddr>) -> anyhow::Result<String> {
    let options = blocklyd::cli::join::Join {
        token,
        address,
        config: blocklyd::cli::join::CONFIG.into(),
        root: root.to_owned(),
        start: false,
        systemctl: "systemctl".into(),
    };
    blocklyd::cli::join::join(options).await
}

#[tokio::test]
async fn a_pasted_token_is_all_a_host_needs_to_join() {
    let cp = ControlPlane::start().await;
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    host(root, &[("enp7s0", [10, 0, 0, 5])]);
    let token = pasted(&cp, "secret", "production", cp.ca_pem(), None);
    let said = join(root, format!("{token}\n"), None).await.unwrap();

    let text = std::fs::read_to_string(root.join("etc/blocklyd/blocklyd.toml")).unwrap();
    let config = Config::parse(&text, Path::new("/etc/blocklyd/blocklyd.toml"), root).unwrap();
    assert_eq!(config.deployment_id, "production");
    assert_eq!(config.api.listen.to_string(), "10.0.0.5:7443", "worked out at each start, not written down");
    assert!(!text.contains("10.0.0.5"), "{text}");
    let fleet = config.fleet.unwrap();
    assert_eq!((fleet.url.as_str(), fleet.ca.to_str().unwrap()), (cp.url.as_str(), "/etc/blocklyd/fleet-ca.pem"));
    assert_eq!(std::fs::read_to_string(root.join("etc/blocklyd/fleet-ca.pem")).unwrap(), cp.ca_pem());
    let written = root.join("var/lib/blocklyd/enrollment-token");
    assert_eq!(std::fs::read_to_string(&written).unwrap(), token, "the whole token: enrollment checks it again");
    assert_eq!(std::fs::metadata(&written).unwrap().permissions().mode() & 0o777, 0o600);
    let unit = std::fs::read_to_string(root.join("etc/systemd/system/blocklyd.service")).unwrap();
    assert!(unit.contains("ExecStart=/usr/local/bin/blocklyd serve"), "{unit}");
    assert!(said.contains("api.listen = \"10.0.0.5:7443\"    (the host's private address, on enp7s0)"), "{said}");
    assert!(said.contains("systemctl enable --now blocklyd"), "{said}");

    // Run again before enrolling, over an operator's own setting: it is kept.
    std::fs::write(root.join("etc/blocklyd/blocklyd.toml"), format!("{text}heartbeat_seconds = 2\n")).unwrap();
    let said = join(root, token.clone(), None).await.unwrap();
    assert!(said.contains("Kept /etc/blocklyd/blocklyd.toml"), "{said}");
    assert!(
        std::fs::read_to_string(root.join("etc/blocklyd/blocklyd.toml")).unwrap().contains("heartbeat_seconds = 2")
    );

    // Enrolled, it changes nothing; a token for another deployment is refused.
    let key = KeyPair::generate().unwrap();
    let name = "n1.nodes.production.fleet";
    let file = IdentityFile {
        node_id: "n1".into(),
        deployment_id: "production".into(),
        control_plane: cp.url.clone(),
        allowed_clients: vec![support::CLIENT.into()],
        enrolled_at: "2026-10-06T00:00:00Z".into(),
        generation: 0,
    };
    let (server, client) = (cp.shared.ca.issue(&key, name, true), cp.shared.ca.issue(&key, name, false));
    Identity::save(&root.join("var/lib/blocklyd"), file, &key.serialize_pem(), &server, &client, cp.ca_pem()).unwrap();
    let said = join(root, token, None).await.unwrap();
    assert!(said.contains("already node n1 of deployment production: nothing changed"), "{said}");
    let refused = join(root, pasted(&cp, "secret", "staging", cp.ca_pem(), None), None).await.unwrap_err().to_string();
    assert!(refused.contains("already node n1 of deployment production, and the token is for staging"), "{refused}");

    // A token that re-enrolls another node is refused; one for this node restarts blocklyd.
    let refused = join(root, pasted(&cp, "again", "production", cp.ca_pem(), Some("n2")), None).await.unwrap_err();
    assert!(refused.to_string().contains("this host is node n1, and the token re-enrolls node n2"), "{refused}");
    let refused = join(root, pasted(&cp, "again", "production", cp.ca_pem(), Some("n1")), None).await.unwrap_err();
    assert!(refused.to_string().contains("without --no-start"), "{refused}");
}

#[tokio::test]
async fn nothing_is_written_until_the_ca_and_the_address_are_certain() {
    let cp = ControlPlane::start().await;
    let root = tempfile::tempdir().unwrap();
    let root = root.path();
    host(root, &[("enp7s0", [10, 0, 0, 5]), ("enp8s0", [10, 1, 0, 5])]);
    let refused = join(root, "c2VjcmV0".into(), None).await.unwrap_err().to_string();
    assert!(refused.contains("bare token") && refused.contains("bk1."), "{refused}");
    // Two private addresses: which one is the operator's to say.
    let token = pasted(&cp, "secret", "production", cp.ca_pem(), None);
    let refused = join(root, token.clone(), None).await.unwrap_err().to_string();
    assert!(refused.contains("10.0.0.5 (enp7s0), 10.1.0.5 (enp8s0)") && refused.contains("--address"), "{refused}");
    // A token whose CA isn't the one the endpoint serves.
    let other = pasted(&cp, "secret", "production", &Ca::new("another CA").pem, None);
    let refused = join(root, other, Some("10.1.0.5".parse().unwrap())).await.unwrap_err().to_string();
    assert!(refused.contains("not the one the token names"), "{refused}");
    assert!(!root.join("etc").exists() && !root.join("var").exists(), "nothing written");

    join(root, token, Some("10.1.0.5".parse().unwrap())).await.unwrap();
    let text = std::fs::read_to_string(root.join("etc/blocklyd/blocklyd.toml")).unwrap();
    let config = Config::parse(&text, Path::new("/etc/blocklyd/blocklyd.toml"), root).unwrap();
    assert_eq!(config.api.listen.to_string(), "10.1.0.5:7443");
    assert_eq!(config.network.edge_ips, vec!["10.1.0.5".parse::<std::net::IpAddr>().unwrap()]);
    assert!(config.inferred.iter().all(|i| i.key == "capacity.reserved_memory_mb"), "{:?}", config.inferred);
}
