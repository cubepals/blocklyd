//! The protocol over HTTP/1.1 + JSON, under `/v1`, behind mutual TLS; and a plain ops listener
//! for `/healthz`, `/readyz` and `/metrics`.
//!
//! HTTP/JSON rather than gRPC: Blockly's control plane is TypeScript with no protobuf toolchain,
//! its other internal protocol (the edge's) is versioned HTTP/JSON, and nothing here needs a
//! bidirectional stream: exec is request/response (the console is RCON), logs are one-way. JSON is
//! also what an operator can read with curl at 3 a.m.

// Handlers answer early with a ready-made `Response`, axum's own idiom for rejections.
#![allow(clippy::result_large_err)]

pub mod error;
pub mod tls;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{FromRequest, FromRequestParts, MatchedPath, Path, Query, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header, request::Parts};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use futures_util::StreamExt;
use hyper::body::Incoming;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;
use tracing::{Instrument, info, info_span, warn};

use self::error::error_response;
use self::tls::ClientIdentity;
use crate::ids::{SnapshotId, WorkloadId};
use crate::manager::{Manager, NodeError, Precondition};
use crate::protocol::{
    CONFIRM_DELETE_HEADER, DataDisposition, EPOCH_HEADER, ExecRequest, ExportRequest, FenceRequest, LogRecord,
    PROTOCOL_HEADER, PROTOCOL_VERSION, RestoreRequest, SnapshotRequest, StopRequest, UploadRequest, WorkloadSpec,
};
use crate::runtime::parse_time;

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<Manager>,
}

/// The workload id in the path, validated before any handler sees it.
pub struct WorkloadPath(pub WorkloadId);

impl<S: Send + Sync> FromRequestParts<S> for WorkloadPath {
    type Rejection = Response;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path(raw) = Path::<String>::from_request_parts(parts, state)
            .await
            .map_err(|e| error_response(StatusCode::BAD_REQUEST, "invalid_request", e.body_text(), None))?;
        WorkloadId::parse(&raw)
            .map(WorkloadPath)
            .map_err(|e| error_response(StatusCode::BAD_REQUEST, "invalid_workload_id", e.to_string(), None))
    }
}

/// A workload and one of its snapshots, from the path.
pub struct SnapshotPath(pub WorkloadId, pub SnapshotId);

impl<S: Send + Sync> FromRequestParts<S> for SnapshotPath {
    type Rejection = Response;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Path((workload, snapshot)) = Path::<(String, String)>::from_request_parts(parts, state)
            .await
            .map_err(|e| error_response(StatusCode::BAD_REQUEST, "invalid_request", e.body_text(), None))?;
        let workload = WorkloadId::parse(&workload)
            .map_err(|e| error_response(StatusCode::BAD_REQUEST, "invalid_workload_id", e.to_string(), None))?;
        let snapshot = SnapshotId::parse(&snapshot)
            .map_err(|e| error_response(StatusCode::BAD_REQUEST, "invalid_snapshot_id", e.to_string(), None))?;
        Ok(SnapshotPath(workload, snapshot))
    }
}

/// JSON bodies, with the protocol's own error shape when they don't parse.
pub struct ApiJson<T>(pub T);

impl<S: Send + Sync, T: serde::de::DeserializeOwned> FromRequest<S> for ApiJson<T> {
    type Rejection = Response;
    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        Json::<T>::from_request(req, state).await.map(|Json(v)| ApiJson(v)).map_err(|e| {
            let status = e.status();
            let code = if status == StatusCode::PAYLOAD_TOO_LARGE { "payload_too_large" } else { "invalid_request" };
            error_response(status, code, e.body_text(), None)
        })
    }
}

/// Query strings, likewise.
pub struct ApiQuery<T>(pub T);

impl<S: Send + Sync, T: serde::de::DeserializeOwned> FromRequestParts<S> for ApiQuery<T> {
    type Rejection = Response;
    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        Query::<T>::from_request_parts(parts, state)
            .await
            .map(|Query(v)| ApiQuery(v))
            .map_err(|e| error_response(StatusCode::BAD_REQUEST, "invalid_request", e.body_text(), None))
    }
}

/// Runs an operation to completion even if the client goes away mid-request: a stop that lost
/// its caller still finishes, and the caller's retry finds it done.
async fn detached<T: Send + 'static>(
    fut: impl std::future::Future<Output = Result<T, NodeError>> + Send + 'static,
) -> Result<T, NodeError> {
    tokio::spawn(fut.in_current_span()).await.map_err(|e| NodeError::Internal(format!("operation panicked: {e}")))?
}

fn etag(digest: &str) -> HeaderValue {
    HeaderValue::from_str(&format!("\"{digest}\"")).unwrap_or_else(|_| HeaderValue::from_static("\"\""))
}

fn precondition(headers: &HeaderMap) -> Result<Precondition, Response> {
    let text = |name: header::HeaderName| headers.get(name).map(|v| v.to_str().unwrap_or("").trim().to_owned());
    match (text(header::IF_MATCH), text(header::IF_NONE_MATCH)) {
        (Some(_), Some(_)) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "send If-Match or If-None-Match, not both",
            None,
        )),
        (Some(m), None) if m == "*" => Ok(Precondition::IfMatch("*".into())),
        (Some(m), None) => Ok(Precondition::IfMatch(m.trim_matches('"').to_owned())),
        (None, Some(n)) if n == "*" => Ok(Precondition::IfNoneMatch),
        (None, Some(_)) => {
            Err(error_response(StatusCode::BAD_REQUEST, "invalid_request", "If-None-Match takes only *", None))
        }
        (None, None) => Ok(Precondition::None),
    }
}

/// The placement epoch a mutating request acts for, if it names one.
fn epoch(headers: &HeaderMap) -> Result<Option<u64>, Response> {
    match headers.get(EPOCH_HEADER).map(|v| v.to_str().map(str::trim)) {
        None => Ok(None),
        Some(Ok(raw)) => raw.parse::<u64>().map(Some).map_err(|_| {
            error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                format!("{EPOCH_HEADER} is a non-negative integer"),
                None,
            )
        }),
        Some(Err(_)) => Err(error_response(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            format!("{EPOCH_HEADER} is a non-negative integer"),
            None,
        )),
    }
}

async fn health(State(app): State<AppState>) -> Response {
    Json(app.manager.health().await).into_response()
}

async fn node(State(app): State<AppState>) -> Response {
    Json(app.manager.node_status().await).into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ListQuery {
    changed_since: Option<String>,
}

async fn list(State(app): State<AppState>, ApiQuery(q): ApiQuery<ListQuery>) -> Response {
    let since = match q.changed_since.as_deref() {
        None => None,
        Some(raw) => match parse_time(Some(raw)) {
            Some(t) => Some(t),
            None => {
                return error_response(StatusCode::BAD_REQUEST, "invalid_request", "changedSince is RFC 3339", None);
            }
        },
    };
    let body = crate::protocol::ListResponse {
        workloads: app.manager.list(since),
        observed_at: crate::runtime::format_time(crate::manager::now()),
    };
    Json(body).into_response()
}

async fn put_workload(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
    ApiJson(spec): ApiJson<WorkloadSpec>,
) -> Result<Response, Response> {
    let pre = precondition(&headers)?;
    let epoch = epoch(&headers)?;
    let manager = app.manager.clone();
    let result = detached(async move { manager.ensure(id, spec, pre, epoch).await })
        .await
        .map_err(IntoResponse::into_response)?;
    let status =
        if result.outcome == crate::protocol::EnsureOutcome::Created { StatusCode::CREATED } else { StatusCode::OK };
    let tag = etag(&result.workload.spec_digest);
    Ok((status, [(header::ETAG, tag)], Json(result)).into_response())
}

async fn get_workload(State(app): State<AppState>, WorkloadPath(id): WorkloadPath) -> Result<Response, NodeError> {
    let view = app.manager.view(&id)?;
    let tag = etag(&view.spec_digest);
    Ok(([(header::ETAG, tag)], Json(view)).into_response())
}

async fn start(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
) -> Result<Response, Response> {
    let epoch = epoch(&headers)?;
    let manager = app.manager.clone();
    let result = detached(async move { manager.start(id, epoch).await }).await.map_err(IntoResponse::into_response)?;
    Ok(Json(result).into_response())
}

async fn stop(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
    body: bytes::Bytes,
) -> Result<Response, Response> {
    let epoch = epoch(&headers)?;
    // The body is optional: an empty one stops with the spec's grace.
    let request: StopRequest = if body.iter().all(u8::is_ascii_whitespace) {
        StopRequest::default()
    } else {
        serde_json::from_slice(&body)
            .map_err(|e| error_response(StatusCode::UNPROCESSABLE_ENTITY, "invalid_request", e.to_string(), None))?
    };
    let manager = app.manager.clone();
    let result = detached(async move { manager.stop(id, request.timeout_seconds, epoch).await })
        .await
        .map_err(IntoResponse::into_response)?;
    Ok(Json(result).into_response())
}

async fn kill(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
) -> Result<Response, Response> {
    let epoch = epoch(&headers)?;
    let manager = app.manager.clone();
    let result = detached(async move { manager.kill(id, epoch).await }).await.map_err(IntoResponse::into_response)?;
    Ok(Json(result).into_response())
}

async fn export(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
    ApiJson(request): ApiJson<ExportRequest>,
) -> Result<Response, Response> {
    let epoch = epoch(&headers)?;
    let manager = app.manager.clone();
    let result =
        detached(async move { manager.export(id, request, epoch).await }).await.map_err(IntoResponse::into_response)?;
    Ok(Json(result).into_response())
}

async fn restore(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
    ApiJson(request): ApiJson<RestoreRequest>,
) -> Result<Response, Response> {
    let epoch = epoch(&headers)?;
    let manager = app.manager.clone();
    let result = detached(async move { manager.restore(id, request, epoch).await })
        .await
        .map_err(IntoResponse::into_response)?;
    Ok(Json(result).into_response())
}

async fn take_snapshot(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
    ApiJson(request): ApiJson<SnapshotRequest>,
) -> Result<Response, Response> {
    let epoch = epoch(&headers)?;
    let manager = app.manager.clone();
    let result = detached(async move { manager.snapshot(id, request, epoch).await })
        .await
        .map_err(IntoResponse::into_response)?;
    let status = if result.created { StatusCode::CREATED } else { StatusCode::OK };
    Ok((status, Json(result)).into_response())
}

async fn list_snapshots(State(app): State<AppState>, WorkloadPath(id): WorkloadPath) -> Result<Response, NodeError> {
    Ok(Json(app.manager.snapshots(&id)?).into_response())
}

async fn delete_snapshot(
    State(app): State<AppState>,
    SnapshotPath(id, snapshot): SnapshotPath,
) -> Result<Response, NodeError> {
    let manager = app.manager.clone();
    Ok(Json(detached(async move { manager.delete_snapshot(id, snapshot).await }).await?).into_response())
}

async fn upload_snapshot(
    State(app): State<AppState>,
    SnapshotPath(id, snapshot): SnapshotPath,
    ApiJson(request): ApiJson<UploadRequest>,
) -> Result<Response, NodeError> {
    let manager = app.manager.clone();
    Ok(Json(detached(async move { manager.upload_snapshot(id, snapshot, request).await }).await?).into_response())
}

async fn fence(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    ApiJson(request): ApiJson<FenceRequest>,
) -> Result<Response, NodeError> {
    let manager = app.manager.clone();
    Ok(Json(detached(async move { manager.fence(id, request.current_epoch).await }).await?).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeleteQuery {
    data: Option<DataDisposition>,
}

async fn delete(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
    ApiQuery(q): ApiQuery<DeleteQuery>,
) -> Result<Response, Response> {
    // Data is kept unless deleting it is asked for explicitly, and confirmed.
    let data = q.data.unwrap_or(DataDisposition::Keep);
    let confirm = headers.get(CONFIRM_DELETE_HEADER).and_then(|v| v.to_str().ok()).map(str::to_owned);
    let epoch = epoch(&headers)?;
    let manager = app.manager.clone();
    let result = detached(async move { manager.delete(id, data, confirm, epoch).await })
        .await
        .map_err(IntoResponse::into_response)?;
    Ok(Json(result).into_response())
}

async fn exec(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    headers: HeaderMap,
    ApiJson(request): ApiJson<ExecRequest>,
) -> Result<Response, Response> {
    let key = match headers.get("idempotency-key").map(|v| v.to_str()) {
        None => None,
        Some(Ok(k)) if !k.is_empty() && k.len() <= 128 && k.bytes().all(|b| b.is_ascii_graphic()) => Some(k.to_owned()),
        Some(_) => {
            return Err(error_response(
                StatusCode::BAD_REQUEST,
                "invalid_request",
                "Idempotency-Key is 1-128 visible ASCII characters",
                None,
            ));
        }
    };
    let epoch = epoch(&headers)?;
    let manager = app.manager.clone();
    let result = detached(async move { manager.exec(id, request, key, epoch).await })
        .await
        .map_err(IntoResponse::into_response)?;
    Ok(Json(result).into_response())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogsQuery {
    tail: Option<u32>,
    since: Option<i64>,
    follow: Option<bool>,
}

async fn logs(
    State(app): State<AppState>,
    WorkloadPath(id): WorkloadPath,
    ApiQuery(q): ApiQuery<LogsQuery>,
) -> Result<Response, NodeError> {
    let stream = app.manager.logs(&id, q.tail, q.since, q.follow.unwrap_or(false))?;
    let body = Body::from_stream(stream.map(|record: LogRecord| {
        let mut line = serde_json::to_vec(&record).expect("log records serialize");
        line.push(b'\n');
        Ok::<_, std::convert::Infallible>(bytes::Bytes::from(line))
    }));
    Ok(([(header::CONTENT_TYPE, "application/x-ndjson")], body).into_response())
}

async fn stats(State(app): State<AppState>, WorkloadPath(id): WorkloadPath) -> Result<Response, NodeError> {
    Ok(Json(app.manager.stats(&id).await?).into_response())
}

async fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "no_route", "no such route in protocol v1", None)
}

/// Every request: a request id, authorization by client identity, a log line, a metric, and the
/// protocol version on the response.
async fn envelope(State(app): State<AppState>, request: Request, next: Next) -> Response {
    let started = Instant::now();
    let method = request.method().clone();
    let route =
        request.extensions().get::<MatchedPath>().map_or_else(|| "unmatched".to_owned(), |p| p.as_str().to_owned());
    let request_id = request
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| {
            !v.is_empty() && v.len() <= 64 && v.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        })
        .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned);
    let identity = request.extensions().get::<ClientIdentity>().cloned().unwrap_or_default();
    let peer = request.extensions().get::<PeerAddr>().map_or_else(|| "-".to_owned(), |p| p.0.ip().to_string());
    let span = info_span!(
        "request",
        %request_id,
        %method,
        %route,
        %peer,
        client = identity.allowed.as_deref().unwrap_or("-")
    );
    let mut response = match &identity.allowed {
        Some(_) => next.run(request).instrument(span.clone()).await,
        None => {
            app.manager.metrics.auth_failure("identity_not_allowed");
            let _enter = span.enter();
            warn!(names = ?identity.names, "refused: client identity is not allowed");
            error_response(StatusCode::FORBIDDEN, "forbidden", "this client identity may not use this node", None)
        }
    };
    let status = response.status().as_u16();
    app.manager.metrics.http(&method, &route, status);
    let headers = response.headers_mut();
    headers.insert(PROTOCOL_HEADER, HeaderValue::from(PROTOCOL_VERSION));
    if let Ok(v) = HeaderValue::from_str(&request_id) {
        headers.insert("x-request-id", v);
    }
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    let _enter = span.enter();
    info!(status, latency_ms = started.elapsed().as_millis() as u64, "request");
    response
}

pub fn router(state: AppState, max_body: usize) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/node", get(node))
        .route("/v1/workloads", get(list))
        .route("/v1/workloads/{id}", put(put_workload).get(get_workload).delete(delete))
        .route("/v1/workloads/{id}/start", post(start))
        .route("/v1/workloads/{id}/stop", post(stop))
        .route("/v1/workloads/{id}/kill", post(kill))
        .route("/v1/workloads/{id}/fence", post(fence))
        .route("/v1/workloads/{id}/export", post(export))
        .route("/v1/workloads/{id}/restore", post(restore))
        .route("/v1/workloads/{id}/snapshots", post(take_snapshot).get(list_snapshots))
        .route("/v1/workloads/{id}/snapshots/{snapshot}", axum::routing::delete(delete_snapshot))
        .route("/v1/workloads/{id}/snapshots/{snapshot}/upload", post(upload_snapshot))
        .route("/v1/workloads/{id}/exec", post(exec))
        .route("/v1/workloads/{id}/logs", get(logs))
        .route("/v1/workloads/{id}/stats", get(stats))
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(state.clone(), envelope))
        .layer(axum::extract::DefaultBodyLimit::max(max_body))
        .with_state(state)
}

/// Serves the API over mutual TLS until `cancel`, then drains connections for up to `grace`.
pub async fn serve_tls(
    listener: TcpListener,
    tls: Arc<rustls::ServerConfig>,
    allowed: Vec<String>,
    app: Router,
    manager: Arc<Manager>,
    cancel: CancellationToken,
    grace: Duration,
) {
    let acceptor = TlsAcceptor::from(tls);
    let graceful = GracefulShutdown::new();
    let allowed = Arc::new(allowed);
    loop {
        let (tcp, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(pair) => pair,
                Err(e) => {
                    warn!(error = %e, "accept failed");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                }
            },
            () = cancel.cancelled() => break,
        };
        let acceptor = acceptor.clone();
        let app = app.clone();
        let allowed = allowed.clone();
        let manager = manager.clone();
        let watcher = graceful.watcher();
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(Duration::from_secs(10), acceptor.accept(tcp)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(e)) => {
                    manager.metrics.auth_failure("tls_handshake");
                    warn!(%peer, error = %e, "TLS handshake refused");
                    return;
                }
                Err(_) => {
                    manager.metrics.auth_failure("tls_handshake_timeout");
                    return;
                }
            };
            let identity = tls::identify(stream.get_ref().1.peer_certificates(), &allowed);
            let service = tower::service_fn(move |mut request: axum::http::Request<Incoming>| {
                request.extensions_mut().insert(identity.clone());
                request.extensions_mut().insert(PeerAddr(peer));
                app.clone().oneshot(request.map(Body::new))
            });
            let mut builder = hyper::server::conn::http1::Builder::new();
            // A slow client can't hold a connection open by trickling headers.
            builder.timer(TokioTimer::new()).header_read_timeout(Duration::from_secs(10));
            let connection = builder.serve_connection(TokioIo::new(stream), TowerToHyperService::new(service));
            if let Err(e) = watcher.watch(connection).await {
                tracing::debug!(%peer, error = %e, "connection ended");
            }
        });
    }
    drop(listener);
    tokio::select! {
        () = graceful.shutdown() => info!("all API connections closed"),
        () = tokio::time::sleep(grace) => warn!(seconds = grace.as_secs(), "shutdown grace ran out with requests in flight"),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PeerAddr(pub SocketAddr);

/// Plain HTTP on the ops listener: liveness, readiness and metrics. No workload control.
pub fn ops_router(manager: Arc<Manager>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok\n" }))
        .route(
            "/readyz",
            get(|State(m): State<Arc<Manager>>| async move {
                if m.docker_up() && m.reconciled() {
                    (StatusCode::OK, "ready\n")
                } else {
                    (StatusCode::SERVICE_UNAVAILABLE, "not ready\n")
                }
            }),
        )
        .route(
            "/metrics",
            get(|State(m): State<Arc<Manager>>| async move {
                (
                    [(header::CONTENT_TYPE, "application/openmetrics-text; version=1.0.0; charset=utf-8")],
                    m.metrics.render(&m),
                )
            }),
        )
        .with_state(manager)
}

/// Serves the ops endpoints until `cancel`, then stops at once. Each answers from memory, so there
/// is nothing to drain, and a graceful shutdown would wait forever on a client that sent half a
/// request: it would keep blocklyd from exiting, an upgrade's restart included. Connections still
/// open end with the runtime.
pub async fn serve_ops(listener: TcpListener, manager: Arc<Manager>, cancel: CancellationToken) {
    let serving = axum::serve(listener, ops_router(manager)).into_future();
    tokio::select! {
        _ = serving => {}
        () = cancel.cancelled() => {}
    }
}
