//! The protocol over real TLS: who gets in, what malformed requests get back, and a lifecycle
//! driven only through HTTP.

mod support;

use support::{CLIENT, client, fixture, pki, request, serve, server_cert_as_client, spec_json};

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
    assert_eq!(r.status, 200);
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
async fn a_valid_certificate_for_another_name_is_forbidden() {
    let e = env().await;
    let edge = client(&e.pki.ca, "edge.test.blockly.internal");
    let r = request(e.server.addr, &e.pki.ca.pem, Some(&edge), "GET", "/v1/workloads", &[], None).await.unwrap();
    assert_eq!(r.status, 403);
    assert_eq!(r.code(), "forbidden");
}

#[tokio::test]
async fn malformed_requests_get_the_protocols_errors() {
    let e = env().await;
    let path = "/v1/workloads/w1";
    let raw = request(e.server.addr, &e.pki.ca.pem, Some(&e.me), "PUT", path, &[], Some(b"{not json".to_vec()))
        .await
        .unwrap();
    assert_eq!((raw.status, raw.code().as_str()), (400, "invalid_request"));

    let mut unknown = spec_json();
    unknown["privileged"] = true.into();
    let r = e.call("PUT", path, Some(unknown)).await;
    assert_eq!((r.status, r.code().as_str()), (422, "invalid_request"), "unknown fields are refused");

    let mut invalid = spec_json();
    invalid["image"] = "docker.io/attacker/miner:latest".into();
    invalid["resources"]["memoryMb"] = 1.into();
    let r = e.call("PUT", path, Some(invalid)).await;
    assert_eq!(r.status, 422);
    assert_eq!(r.json()["error"]["details"]["fields"].as_array().unwrap().len(), 2);

    for bad in ["/v1/workloads/..%2F..%2Fetc", "/v1/workloads/UPPER", "/v1/workloads/a_b", "/v1/workloads/-x"] {
        let r = e.call("GET", bad, None).await;
        assert_eq!((r.status, r.code().as_str()), (400, "invalid_workload_id"), "{bad}");
    }
    let r = e.call("GET", "/v2/workloads", None).await;
    assert_eq!((r.status, r.code().as_str()), (404, "no_route"));

    let huge = serde_json::json!({ "command": ["sh", "-c", "x".repeat(3 * 1024 * 1024)] });
    let r = e.call("POST", "/v1/workloads/w1/exec", Some(huge)).await;
    assert_eq!(r.status, 413, "bodies are bounded");

    let r = e.call_with("PUT", path, &[("if-match", "a"), ("if-none-match", "*")], Some(spec_json())).await;
    assert_eq!(r.status, 400);
}

#[tokio::test]
async fn methods_nobody_uses_are_counted_under_one_label() {
    // Any token is a method to HTTP: one label value each would be a series kept for good.
    let e = env().await;
    for method in ["FOO", "BAR"] {
        assert_eq!(e.call(method, "/v1/health", None).await.status, 405);
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
    assert_eq!(created.status, 201, "{}", String::from_utf8_lossy(&created.body));
    let etag = created.headers.get("etag").unwrap().to_str().unwrap().to_owned();
    assert_eq!(etag, format!("\"{}\"", created.json()["workload"]["specDigest"].as_str().unwrap()));
    let body = created.json();
    assert!(!String::from_utf8_lossy(&created.body).contains("first-secret"), "secrets never come back");
    assert_eq!(body["workload"]["secretNames"][0], "RCON_PASSWORD");
    assert_eq!(
        body["workload"]["ports"][0]["endpoints"]["edge"][0],
        format!("127.0.0.1:{}", body["workload"]["ports"][0]["hostPort"])
    );

    assert_eq!(e.call("PUT", path, Some(spec_json())).await.status, 200, "a duplicate create is an unchanged 200");
    assert_eq!(e.call_with("PUT", path, &[("if-none-match", "*")], Some(spec_json())).await.status, 412);

    let started = e.call("POST", &format!("{path}/start"), None).await;
    assert_eq!((started.status, started.json()["changed"].as_bool()), (200, Some(true)));
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
    assert_eq!(exec.status, 200);
    assert_eq!(exec.json()["stdout"], "cat /data/whitelist.json");

    let logs = e.call("GET", &format!("{path}/logs?tail=5"), None).await;
    assert_eq!(logs.headers.get("content-type").unwrap(), "application/x-ndjson");
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
    assert_eq!(bad_stop.status, 422);

    let unconfirmed = e.call("DELETE", &format!("{path}?data=delete"), None).await;
    assert_eq!((unconfirmed.status, unconfirmed.code().as_str()), (428, "confirmation_required"));
    let deleted =
        e.call_with("DELETE", &format!("{path}?data=delete"), &[("x-blockly-confirm-delete-data", "w1")], None).await;
    assert_eq!(deleted.json()["data"], "trashed");
    assert_eq!(e.call("GET", path, None).await.status, 404);

    let host = e.call("GET", "/v1/node", None).await;
    assert_eq!(host.json()["nodeId"], "test-node");
    e.server.cancel.cancel();
    e.server.task.await.unwrap();
}
