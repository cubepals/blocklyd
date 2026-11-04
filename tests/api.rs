// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! The protocol over real TLS: who gets in, how many may knock at once, what malformed requests
//! get back, and a lifecycle driven only through HTTP.

mod support;

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use blocklyd::api::HandshakeLimits;
use hyper::StatusCode;
use hyper::header::{CONTENT_TYPE, ETAG, IF_MATCH, IF_NONE_MATCH};
use support::{CLIENT, client, fixture, pki, request, serve, serve_with, server_cert_as_client, spec_json, tls_over};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpSocket, TcpStream};

struct Env {
    /// Owns the temporary directory; dropped last.
    _f: support::Fixture,
    pki: support::Pki,
    server: support::Server,
    me: support::Identity,
}

async fn env() -> Env {
    let f = fixture("").await;
    let pki = pki(f.dir.path());
    let server = serve(f.manager.clone()).await;
    let me = client(&pki.ca, CLIENT);
    Env { _f: f, pki, server, me }
}

impl Env {
    async fn call(&self, method: &str, path: &str, body: Option<serde_json::Value>) -> support::Response {
        self.call_with(method, path, &[], body).await
    }
    async fn call_with(
        &self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: Option<serde_json::Value>,
    ) -> support::Response {
        request(
            self.server.addr,
            &self.pki.ca.pem,
            Some(&self.me),
            method,
            path,
            headers,
            body.map(|b| serde_json::to_vec(&b).unwrap()),
        )
        .await
        .expect("request")
    }
}

#[tokio::test]
async fn the_allowed_client_gets_in_and_every_answer_names_the_protocol() {
    let e = env().await;
    let r = e.call("GET", "/v1/health", None).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["status"], "ok", "reconciled, and the runtime answers");
    assert_eq!(r.headers.get("blocklyd-protocol").unwrap(), "1");
    assert!(r.headers.get("x-request-id").is_some());
    assert_eq!(r.headers.get("cache-control").unwrap(), "no-store");
}

#[tokio::test]
async fn no_client_certificate_no_connection() {
    let e = env().await;
    let r = request(e.server.addr, &e.pki.ca.pem, None, "GET", "/v1/health", &[], None).await;
    assert!(r.is_err(), "anonymous clients never reach HTTP: {:?}", r.map(|r| r.status));
}

#[tokio::test]
async fn a_certificate_from_another_ca_is_refused() {
    let e = env().await;
    let stranger_ca = blocklyd::certs::authority("someone else's CA").unwrap();
    let stranger = client(&stranger_ca, CLIENT);
    let r = request(e.server.addr, &e.pki.ca.pem, Some(&stranger), "GET", "/v1/health", &[], None).await;
    assert!(r.is_err(), "same name, wrong issuer");
}

#[tokio::test]
async fn a_server_certificate_cannot_be_used_as_a_client() {
    // A node's own certificate (serverAuth) from the same CA: a compromised host must not be
    // able to use its identity to call other nodes.
    let e = env().await;
    let host_identity = server_cert_as_client(&e.pki.ca);
    let r = request(e.server.addr, &e.pki.ca.pem, Some(&host_identity), "GET", "/v1/health", &[], None).await;
    assert!(r.is_err(), "extended key usage must be clientAuth");
}

#[tokio::test]
async fn a_valid_certificate_for_another_name_is_forbidden_and_its_connection_closed() {
    let e = env().await;
    let edge = client(&e.pki.ca, "edge.test.blockly.internal");
    let r = request(e.server.addr, &e.pki.ca.pem, Some(&edge), "GET", "/v1/workloads", &[], None).await.unwrap();
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(r.code(), "forbidden");
    // Its connection doesn't stay open for it, though it asked to keep it.
    let tls = tls_over(TcpStream::connect(e.server.addr).await.unwrap(), &e.pki.ca.pem, Some(&edge)).await.unwrap();
    let answer = until_closed(tls, KEEP_ALIVE, Duration::from_secs(2)).await.expect("the connection stayed open");
    assert!(answer.starts_with("HTTP/1.1 403 "), "{answer}");
    // The allowed client's does, as ever.
    let tls = tls_over(TcpStream::connect(e.server.addr).await.unwrap(), &e.pki.ca.pem, Some(&e.me)).await.unwrap();
    let answer = until_closed(tls, KEEP_ALIVE, Duration::from_millis(500)).await.expect_err("kept open");
    assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
}

/// A request for `/v1/health` that asks to keep its connection open.
const KEEP_ALIVE: &str = "GET /v1/health HTTP/1.1\r\nhost: localhost\r\n\r\n";

/// Sends `head` and reads what comes back until the server closes the connection, for up to
/// `within`: Err with what it read if the connection is still open then.
async fn until_closed(
    mut io: impl AsyncRead + AsyncWrite + Unpin,
    head: &str,
    within: Duration,
) -> Result<String, String> {
    io.write_all(head.as_bytes()).await.unwrap();
    let mut answer = Vec::new();
    let read = tokio::time::timeout(within, io.read_to_end(&mut answer)).await;
    let text = String::from_utf8_lossy(&answer).into_owned();
    if read.is_ok() { Ok(text) } else { Err(text) }
}

/// A TCP connection to `addr` from `source`, one of the loopback addresses.
async fn connect_from(source: &str, addr: SocketAddr) -> TcpStream {
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind(format!("{source}:0").parse().unwrap()).unwrap();
    socket.connect(addr).await.unwrap()
}

/// How many of `streams` the server closes within `within`, though none sent a byte.
async fn closed_within(streams: &mut [TcpStream], within: Duration) -> usize {
    let reads =
        streams.iter_mut().map(|s| async move { tokio::time::timeout(within, s.read(&mut [0u8; 1])).await.is_ok() });
    futures_util::future::join_all(reads).await.into_iter().filter(|closed| *closed).count()
}

#[tokio::test]
async fn handshakes_in_progress_are_capped_per_address_and_overall() {
    let f = fixture("").await;
    let pki = pki(f.dir.path());
    let server = serve_with(f.manager.clone(), HandshakeLimits { total: 6, per_peer: 4 }).await;
    let me = client(&pki.ca, CLIENT);
    // One address opens connections and says nothing: four wait in a handshake, the rest are
    // closed at once.
    let mut silent = Vec::new();
    for _ in 0..9 {
        silent.push(connect_from("127.0.0.2", server.addr).await);
    }
    assert_eq!(closed_within(&mut silent, Duration::from_secs(1)).await, 5);
    // The allowed client, from another address, still gets in.
    let r = request(server.addr, &pki.ca.pem, Some(&me), "GET", "/v1/health", &[], None).await.unwrap();
    assert_eq!(r.status, StatusCode::OK);
    // Six in all: a third address gets the two places left.
    let mut third = Vec::new();
    for _ in 0..3 {
        third.push(connect_from("127.0.0.3", server.addr).await);
    }
    assert_eq!(closed_within(&mut third, Duration::from_secs(1)).await, 1);
    let metrics = f.manager.metrics.render(&f.manager);
    assert!(metrics.contains(r#"blocklyd_auth_failures_total{reason="tls_handshakes_full"} 6"#), "{metrics}");
    // A handshake that ends, however it ends, gives its place back.
    drop((silent, third));
    let answer = support::eventually("a place for 127.0.0.2 again", async || {
        let Ok(tls) = tls_over(connect_from("127.0.0.2", server.addr).await, &pki.ca.pem, Some(&me)).await else {
            return (false, String::new());
        };
        let head = "GET /v1/health HTTP/1.1\r\nhost: localhost\r\nconnection: close\r\n\r\n";
        let answer = until_closed(tls, head, Duration::from_secs(2)).await.unwrap_or_else(|open| open);
        (answer.starts_with("HTTP/1.1 200 "), answer)
    })
    .await;
    assert!(answer.contains("\"status\":\"ok\""), "{answer}");
}

/// What tracing writes, kept to be read.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn refused_handshakes_never_flood_the_log() {
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let e = env().await;
    // Each log line caches whether anyone listens, worked out by the first thread to reach it.
    // Another test refusing a handshake on its own thread, with no subscriber, can reach it while
    // this one is being set, and leave it cached as unheard: worked out again now, it is heard.
    tracing::callsite::rebuild_interest_cache();
    for _ in 0..100 {
        let mut garbage = TcpStream::connect(e.server.addr).await.unwrap();
        garbage.write_all(b"GET / HTTP/1.1\r\nhost: localhost\r\n\r\n").await.unwrap();
        let closed = tokio::time::timeout(Duration::from_secs(5), garbage.read_to_end(&mut Vec::new())).await;
        assert!(closed.is_ok(), "the server closes a connection that isn't TLS");
    }
    assert_eq!(e.call("GET", "/v1/health", None).await.status, StatusCode::OK);
    let text = String::from_utf8(captured.0.lock().unwrap().clone()).unwrap();
    let warnings: Vec<&str> = text.lines().filter(|l| l.contains("WARN") && l.contains("TLS handshake")).collect();
    assert!(warnings.len() <= 1, "{warnings:#?}");
    let debug = text.lines().filter(|l| l.contains("DEBUG") && l.contains("TLS handshake refused")).count();
    assert_eq!(debug, 100, "each one is still logged, at debug");
    let metrics = e._f.manager.metrics.render(&e._f.manager);
    assert!(metrics.contains(r#"blocklyd_auth_failures_total{reason="tls_handshake"} 100"#), "and counted");
}

#[tokio::test]
async fn malformed_requests_get_the_protocols_errors() {
    let e = env().await;
    let path = "/v1/workloads/w1";
    let raw = request(e.server.addr, &e.pki.ca.pem, Some(&e.me), "PUT", path, &[], Some(b"{not json".to_vec()))
        .await
        .unwrap();
    assert_eq!((raw.status, raw.code().as_str()), (StatusCode::BAD_REQUEST, "invalid_request"));

    let mut unknown = spec_json();
    unknown["privileged"] = true.into();
    let r = e.call("PUT", path, Some(unknown)).await;
    assert_eq!(
        (r.status, r.code().as_str()),
        (StatusCode::UNPROCESSABLE_ENTITY, "invalid_request"),
        "unknown fields are refused"
    );

    let mut invalid = spec_json();
    invalid["image"] = "docker.io/attacker/miner:latest".into();
    invalid["resources"]["memoryMb"] = 1.into();
    let r = e.call("PUT", path, Some(invalid)).await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(r.json()["error"]["details"]["fields"].as_array().unwrap().len(), 2);

    for bad in ["/v1/workloads/..%2F..%2Fetc", "/v1/workloads/UPPER", "/v1/workloads/a_b", "/v1/workloads/-x"] {
        let r = e.call("GET", bad, None).await;
        assert_eq!((r.status, r.code().as_str()), (StatusCode::BAD_REQUEST, "invalid_workload_id"), "{bad}");
    }
    let r = e.call("GET", "/v2/workloads", None).await;
    assert_eq!((r.status, r.code().as_str()), (StatusCode::NOT_FOUND, "no_route"));

    let huge = serde_json::json!({ "command": ["sh", "-c", "x".repeat(3 * 1024 * 1024)] });
    let r = e.call("POST", "/v1/workloads/w1/exec", Some(huge)).await;
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "bodies are bounded");

    let r =
        e.call_with("PUT", path, &[(IF_MATCH.as_str(), "a"), (IF_NONE_MATCH.as_str(), "*")], Some(spec_json())).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn methods_nobody_uses_are_counted_under_one_label() {
    // Any token is a method to HTTP: one label value each would be a series kept for good.
    let e = env().await;
    for method in ["FOO", "BAR"] {
        assert_eq!(e.call(method, "/v1/health", None).await.status, StatusCode::METHOD_NOT_ALLOWED);
    }
    let metrics = e._f.manager.metrics.render(&e._f.manager);
    let series: Vec<_> = metrics.lines().filter(|l| l.starts_with("blocklyd_http_requests_total{")).collect();
    assert_eq!(series, [r#"blocklyd_http_requests_total{method="other",route="/v1/health",status="405"} 2"#]);
}

#[tokio::test]
async fn the_ops_listener_stops_at_once_even_with_half_a_request_open() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    let f = fixture("").await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cancel = tokio_util::sync::CancellationToken::new();
    let serving = tokio::spawn(blocklyd::api::serve_ops(listener, f.manager.clone(), cancel.clone()));
    // A whole request is answered as ever.
    let mut whole = TcpStream::connect(addr).await.unwrap();
    whole.write_all(b"GET /healthz HTTP/1.1\r\nhost: ops\r\nconnection: close\r\n\r\n").await.unwrap();
    let mut answer = String::new();
    whole.read_to_string(&mut answer).await.unwrap();
    assert!(answer.starts_with("HTTP/1.1 200 ") && answer.ends_with("\r\n\r\nok\n"), "{answer}");
    // Half of one, from a client that went quiet: a graceful shutdown would wait on it forever.
    let mut half = TcpStream::connect(addr).await.unwrap();
    half.write_all(b"GET /metrics HTTP/1.1\r\n").await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    cancel.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(2), serving).await;
    stopped.expect("the ops listener waited on half a request").unwrap();
    assert!(TcpStream::connect(addr).await.is_err(), "nothing new is taken");
}

#[tokio::test]
async fn a_whole_lifecycle_over_http() {
    let e = env().await;
    let path = "/v1/workloads/w1";
    let created = e.call("PUT", path, Some(spec_json())).await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", String::from_utf8_lossy(&created.body));
    let etag = created.headers.get(ETAG).unwrap().to_str().unwrap().to_owned();
    assert_eq!(etag, format!("\"{}\"", created.json()["workload"]["specDigest"].as_str().unwrap()));
    let body = created.json();
    assert!(!String::from_utf8_lossy(&created.body).contains("first-secret"), "secrets never come back");
    assert_eq!(body["workload"]["secretNames"][0], "RCON_PASSWORD");
    assert_eq!(
        body["workload"]["ports"][0]["endpoints"]["edge"][0],
        format!("127.0.0.1:{}", body["workload"]["ports"][0]["hostPort"])
    );

    assert_eq!(
        e.call("PUT", path, Some(spec_json())).await.status,
        StatusCode::OK,
        "a duplicate create is an unchanged 200"
    );
    assert_eq!(
        e.call_with("PUT", path, &[(IF_NONE_MATCH.as_str(), "*")], Some(spec_json())).await.status,
        StatusCode::PRECONDITION_FAILED
    );

    let started = e.call("POST", &format!("{path}/start"), None).await;
    assert_eq!((started.status, started.json()["changed"].as_bool()), (StatusCode::OK, Some(true)));
    let again = e.call("POST", &format!("{path}/start"), None).await;
    assert_eq!(again.json()["changed"].as_bool(), Some(false));

    let exec = e
        .call_with(
            "POST",
            &format!("{path}/exec"),
            &[("idempotency-key", "op-1:access-read")],
            Some(serde_json::json!({ "command": ["cat", "/data/whitelist.json"] })),
        )
        .await;
    assert_eq!(exec.status, StatusCode::OK);
    assert_eq!(exec.json()["stdout"], "cat /data/whitelist.json");

    let logs = e.call("GET", &format!("{path}/logs?tail=5"), None).await;
    assert_eq!(logs.headers.get(CONTENT_TYPE).unwrap(), "application/x-ndjson");
    let lines: Vec<serde_json::Value> =
        String::from_utf8_lossy(&logs.body).lines().map(|l| serde_json::from_str(l).unwrap()).collect();
    assert_eq!(lines.last().unwrap()["event"], "end");

    let stats = e.call("GET", &format!("{path}/stats"), None).await;
    assert_eq!(stats.json()["state"], "running");

    let list = e.call("GET", "/v1/workloads?changedSince=2000-01-01T00:00:00Z", None).await;
    assert_eq!(list.json()["workloads"].as_array().unwrap().len(), 1);
    let future = e.call("GET", "/v1/workloads?changedSince=2999-01-01T00:00:00Z", None).await;
    assert_eq!(future.json()["workloads"].as_array().unwrap().len(), 0);

    let stopped = e.call("POST", &format!("{path}/stop"), Some(serde_json::json!({ "timeoutSeconds": 5 }))).await;
    assert_eq!(stopped.json()["workload"]["state"], "stopped");
    let bad_stop = e.call("POST", &format!("{path}/stop"), Some(serde_json::json!({ "timeoutSeconds": 0 }))).await;
    assert_eq!(bad_stop.status, StatusCode::UNPROCESSABLE_ENTITY);

    let unconfirmed = e.call("DELETE", &format!("{path}?data=delete"), None).await;
    assert_eq!(
        (unconfirmed.status, unconfirmed.code().as_str()),
        (StatusCode::PRECONDITION_REQUIRED, "confirmation_required")
    );
    let deleted =
        e.call_with("DELETE", &format!("{path}?data=delete"), &[("x-blockly-confirm-delete-data", "w1")], None).await;
    assert_eq!(deleted.json()["data"], "trashed");
    assert_eq!(e.call("GET", path, None).await.status, StatusCode::NOT_FOUND);

    let host = e.call("GET", "/v1/node", None).await;
    assert_eq!(host.json()["nodeId"], "test-node");
    e.server.cancel.cancel();
    e.server.task.await.unwrap();
}
