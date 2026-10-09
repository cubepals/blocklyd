//! Running one command inside a workload: the request, its bounds, the fingerprint an
//! idempotency key is checked against, and the answer. Reading the workload's log is not here:
//! `LogRecord` is in `workload.rs`.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::FieldError;
use crate::ids::WorkloadId;

#[derive(Clone, Debug, Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExecRequest {
    /// argv, never a shell string: blocklyd adds no shell. It runs inside the workload, as
    /// the workload's user, with its limits; never on the host.
    pub command: Vec<String>,
    #[serde(default = "thirty")]
    pub timeout_seconds: u32,
}

fn thirty() -> u32 {
    30
}

pub(crate) const MAX_EXEC_ARGS: usize = 256;
/// Linux refuses a single argument over 128 KiB (MAX_ARG_STRLEN).
pub(crate) const MAX_EXEC_ARG: usize = 128 * 1024;
pub(crate) const MAX_EXEC_TOTAL: usize = 1024 * 1024;
pub(crate) const MAX_EXEC_TIMEOUT: u32 = 600;

impl ExecRequest {
    pub(crate) fn validate(&self) -> Result<(), Vec<FieldError>> {
        let mut errors = Vec::new();
        if self.command.is_empty() || self.command.len() > MAX_EXEC_ARGS {
            errors.push(FieldError { field: "command".into(), problem: format!("1 to {MAX_EXEC_ARGS} arguments") });
        }
        if self.command.first().is_some_and(|c| c.is_empty()) {
            errors.push(FieldError { field: "command[0]".into(), problem: "names a program".into() });
        }
        if self.command.iter().any(|a| a.len() > MAX_EXEC_ARG || a.contains('\0')) {
            errors.push(FieldError {
                field: "command".into(),
                problem: format!("arguments are at most {MAX_EXEC_ARG} bytes, without NUL"),
            });
        }
        if self.command.iter().map(String::len).sum::<usize>() > MAX_EXEC_TOTAL {
            errors.push(FieldError {
                field: "command".into(),
                problem: format!("at most {MAX_EXEC_TOTAL} bytes in all"),
            });
        }
        if !(1..=MAX_EXEC_TIMEOUT).contains(&self.timeout_seconds) {
            errors.push(FieldError {
                field: "timeoutSeconds".into(),
                problem: format!("must be between 1 and {MAX_EXEC_TIMEOUT}"),
            });
        }
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }

    /// What makes two requests "the same" under one idempotency key.
    pub(crate) fn fingerprint(&self, id: &WorkloadId) -> String {
        let mut hash = Sha256::new();
        hash.update(id.as_str());
        for arg in &self.command {
            hash.update((arg.len() as u64).to_be_bytes());
            hash.update(arg);
        }
        hash.update(self.timeout_seconds.to_be_bytes());
        hex::encode(hash.finalize())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ExecResponse {
    /// None when it timed out and was killed before reporting one.
    pub exit_code: Option<i64>,
    pub stdout: String,
    pub(crate) stderr: String,
    pub(crate) stdout_truncated: bool,
    pub(crate) stderr_truncated: bool,
    pub timed_out: bool,
    /// blocklyd killed the process (by its host pid) after the timeout. Docker alone can't:
    /// its API has no way to stop an exec (moby/moby#35703).
    pub killed: bool,
    pub(crate) duration_ms: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_requests_are_bounded() {
        let ok: ExecRequest = serde_json::from_value(serde_json::json!({ "command": ["du", "-sk", "/data"] })).unwrap();
        assert!(ok.validate().is_ok());
        assert_eq!(ok.timeout_seconds, 30);
        let bad: ExecRequest =
            serde_json::from_value(serde_json::json!({ "command": [], "timeoutSeconds": 0 })).unwrap();
        assert_eq!(bad.validate().unwrap_err().len(), 2);
        let nul: ExecRequest = serde_json::from_value(serde_json::json!({ "command": ["sh", "a\u{0}b"] })).unwrap();
        assert!(nul.validate().is_err());
        assert!(
            serde_json::from_value::<ExecRequest>(serde_json::json!({ "command": ["x"], "privileged": true })).is_err()
        );
    }

    #[test]
    fn fingerprints_separate_arguments() {
        let id = WorkloadId::parse("w").unwrap();
        let a = ExecRequest { command: vec!["ab".into(), "c".into()], timeout_seconds: 5 };
        let b = ExecRequest { command: vec!["a".into(), "bc".into()], timeout_seconds: 5 };
        assert_ne!(a.fingerprint(&id), b.fingerprint(&id));
    }
}
