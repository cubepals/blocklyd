//! Errors as the protocol states them: an HTTP status and a stable machine-readable code. Codes
//! are the contract; messages are for people and may change.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::manager::NodeError;
use crate::protocol::{ErrorBody, ErrorDetail};

pub fn error_response(
    status: StatusCode,
    code: &str,
    message: impl Into<String>,
    details: Option<serde_json::Value>,
) -> Response {
    let body = ErrorBody { error: ErrorDetail { code: code.into(), message: message.into(), details } };
    (status, Json(body)).into_response()
}

impl IntoResponse for NodeError {
    fn into_response(self) -> Response {
        let message = self.to_string();
        let (status, code, details) = match &self {
            NodeError::Invalid(errors) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "invalid_request", Some(serde_json::json!({ "fields": errors })))
            }
            NodeError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found", None),
            NodeError::SnapshotNotFound(_) => (StatusCode::NOT_FOUND, "snapshot_not_found", None),
            NodeError::PreconditionFailed { current } => (
                StatusCode::PRECONDITION_FAILED,
                "precondition_failed",
                Some(serde_json::json!({ "currentSpecDigest": current })),
            ),
            NodeError::Conflict { code, .. } => (StatusCode::CONFLICT, *code, None),
            NodeError::InsufficientCapacity(_) => (StatusCode::CONFLICT, "insufficient_capacity", None),
            NodeError::InsufficientDisk(_) => (StatusCode::INSUFFICIENT_STORAGE, "insufficient_disk", None),
            NodeError::NoFreePorts(_) => (StatusCode::CONFLICT, "no_free_ports", None),
            NodeError::ConfirmationRequired => (StatusCode::PRECONDITION_REQUIRED, "confirmation_required", None),
            NodeError::IdempotencyMismatch => (StatusCode::UNPROCESSABLE_ENTITY, "idempotency_key_reused", None),
            NodeError::RuntimeUnavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "runtime_unavailable", None),
            NodeError::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, "timeout", None),
            NodeError::Runtime(_) => (StatusCode::BAD_GATEWAY, "runtime_error", None),
            NodeError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal", None),
            NodeError::StaleEpoch { asked, current } => (
                StatusCode::CONFLICT,
                "stale_epoch",
                Some(serde_json::json!({ "askedEpoch": asked, "currentEpoch": current })),
            ),
            NodeError::EpochAhead { asked, current } => (
                StatusCode::CONFLICT,
                "epoch_ahead",
                Some(serde_json::json!({ "askedEpoch": asked, "currentEpoch": current })),
            ),
            NodeError::Superseded { by } => {
                (StatusCode::CONFLICT, "superseded", Some(serde_json::json!({ "supersededBy": by })))
            }
            NodeError::InvalidArchive(_) => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_archive", None),
            NodeError::ChecksumMismatch { expected, actual } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "checksum_mismatch",
                Some(serde_json::json!({ "expected": expected, "actual": actual })),
            ),
            NodeError::Transfer(_) => (StatusCode::BAD_GATEWAY, "transfer_failed", None),
            NodeError::ArchiveTooLarge { size_bytes, limit_bytes } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "archive_too_large",
                Some(serde_json::json!({ "sizeBytes": size_bytes, "limitBytes": limit_bytes })),
            ),
            NodeError::EpochRequired { current } => (
                StatusCode::PRECONDITION_REQUIRED,
                "epoch_required",
                Some(serde_json::json!({ "currentEpoch": current })),
            ),
        };
        error_response(status, code, message, details)
    }
}
