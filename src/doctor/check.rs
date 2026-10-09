//! One verdict about the host: it passes, warns or fails, and short of a pass it says what to do.

use serde::Serialize;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Status {
    Pass,
    Warn,
    Fail,
}

/// One verdict about the host.
#[derive(Clone, Debug, Serialize)]
pub struct Check {
    /// Stable, for scripts: `docker.live_restore`, `state_dir.ownable`.
    pub(crate) name: &'static str,
    pub(crate) status: Status,
    pub(crate) detail: String,
    /// What to do, when it isn't a pass.
    pub(crate) fix: Option<String>,
    /// A failure here stops blocklyd from starting, as it did before doctor existed.
    #[serde(skip)]
    pub(crate) gates_start: bool,
}

impl Check {
    pub(crate) fn pass(name: &'static str, detail: impl Into<String>) -> Self {
        Self { name, status: Status::Pass, detail: detail.into(), fix: None, gates_start: false }
    }

    pub(crate) fn warn(name: &'static str, detail: impl Into<String>, fix: impl Into<String>) -> Self {
        Self { name, status: Status::Warn, detail: detail.into(), fix: Some(fix.into()), gates_start: false }
    }

    pub(crate) fn fail(name: &'static str, detail: impl Into<String>, fix: impl Into<String>) -> Self {
        Self { name, status: Status::Fail, detail: detail.into(), fix: Some(fix.into()), gates_start: false }
    }

    /// Marks a check whose failure keeps blocklyd from starting.
    pub(crate) fn gating(self) -> Self {
        Self { gates_start: true, ..self }
    }
}
