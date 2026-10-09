//! Errors as the protocol states them: an HTTP status and a stable machine-readable code. Codes
//! are the contract; messages are for people and may change.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::manager::NodeError;
use crate::protocol::{ErrorBody, ErrorDetail};

pub(crate) fn error_response(
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
            Self::Invalid(errors) => {
                (StatusCode::UNPROCESSABLE_ENTITY, "invalid_request", Some(serde_json::json!({ "fields": errors })))
            }
            Self::NotFound(_) => (StatusCode::NOT_FOUND, "not_found", None),
            Self::SnapshotNotFound(_) => (StatusCode::NOT_FOUND, "snapshot_not_found", None),
            Self::PreconditionFailed { current } => (
                StatusCode::PRECONDITION_FAILED,
                "precondition_failed",
                Some(serde_json::json!({ "currentSpecDigest": current })),
            ),
            Self::Conflict { code, .. } => (StatusCode::CONFLICT, code.as_str(), None),
            Self::InsufficientCapacity(_) => (StatusCode::CONFLICT, "insufficient_capacity", None),
            Self::InsufficientDisk(_) => (StatusCode::INSUFFICIENT_STORAGE, "insufficient_disk", None),
            Self::NoFreePorts(_) => (StatusCode::CONFLICT, "no_free_ports", None),
            Self::ConfirmationRequired => (StatusCode::PRECONDITION_REQUIRED, "confirmation_required", None),
            Self::IdempotencyMismatch => (StatusCode::UNPROCESSABLE_ENTITY, "idempotency_key_reused", None),
            Self::RuntimeUnavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "runtime_unavailable", None),
            Self::Timeout(_) => (StatusCode::GATEWAY_TIMEOUT, "timeout", None),
            Self::Runtime(_) => (StatusCode::BAD_GATEWAY, "runtime_error", None),
            Self::Internal(_) | Self::Io { .. } | Self::Store(_) => {
                (StatusCode::INTERNAL_SERVER_ERROR, "internal", None)
            }
            Self::StaleEpoch { asked, current } => (
                StatusCode::CONFLICT,
                "stale_epoch",
                Some(serde_json::json!({ "askedEpoch": asked, "currentEpoch": current })),
            ),
            Self::EpochAhead { asked, current } => (
                StatusCode::CONFLICT,
                "epoch_ahead",
                Some(serde_json::json!({ "askedEpoch": asked, "currentEpoch": current })),
            ),
            Self::Superseded { by } => {
                (StatusCode::CONFLICT, "superseded", Some(serde_json::json!({ "supersededBy": by })))
            }
            Self::InvalidArchive(_) => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_archive", None),
            Self::ChecksumMismatch { expected, actual } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "checksum_mismatch",
                Some(serde_json::json!({ "expected": expected, "actual": actual })),
            ),
            Self::Transfer(_) | Self::TransferBroke { .. } => (StatusCode::BAD_GATEWAY, "transfer_failed", None),
            Self::ArchiveTooLarge { size_bytes, limit_bytes } => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "archive_too_large",
                Some(serde_json::json!({ "sizeBytes": size_bytes, "limitBytes": limit_bytes })),
            ),
            Self::EpochRequired { current } => (
                StatusCode::PRECONDITION_REQUIRED,
                "epoch_required",
                Some(serde_json::json!({ "currentEpoch": current })),
            ),
        };
        error_response(status, code, message, details)
    }
}
