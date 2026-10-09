//! The state a workload is in, from its record and what the runtime last showed of its container:
//! what every view, heartbeat and restart decision goes by.

use crate::protocol::WorkloadState;
use crate::runtime::{ContainerInfo, ContainerStatus, parse_time};
use crate::store::{Phase, WorkloadRecord};

/// Whether a stop blocklyd was asked for is under way: a container still running then reads as
/// stopping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stopping {
    Underway,
    NotAsked,
}

/// Whether what blocklyd last saw of the runtime is current (`Manager::trustworthy`), or history
/// after an outage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Sight {
    Current,
    Stale,
}

pub(crate) fn derive_state(
    record: &WorkloadRecord,
    info: Option<&ContainerInfo>,
    stopping: Stopping,
    sight: Sight,
) -> WorkloadState {
    let state = derive_power_state(record, info, stopping, sight);
    let at_rest = matches!(
        state,
        WorkloadState::Created | WorkloadState::Stopped | WorkloadState::Crashed | WorkloadState::Missing
    );
    if record.superseded_by.is_some() && at_rest { WorkloadState::Fenced } else { state }
}

pub(super) fn derive_power_state(
    record: &WorkloadRecord,
    info: Option<&ContainerInfo>,
    stopping: Stopping,
    sight: Sight,
) -> WorkloadState {
    match record.phase {
        Phase::Retained => WorkloadState::Retained,
        Phase::Creating if info.is_none() => WorkloadState::Creating,
        // What blocklyd last saw is history, not the state: a daemon stop may have killed it.
        _ if sight == Sight::Stale => WorkloadState::Unknown,
        _ => match info {
            None => WorkloadState::Missing,
            Some(i) => match i.status {
                ContainerStatus::Created => WorkloadState::Created,
                ContainerStatus::Running | ContainerStatus::Paused => match stopping {
                    Stopping::Underway => WorkloadState::Stopping,
                    Stopping::NotAsked => WorkloadState::Running,
                },
                ContainerStatus::Restarting => WorkloadState::Restarting,
                ContainerStatus::Removing => WorkloadState::Stopping,
                ContainerStatus::Exited | ContainerStatus::Dead => {
                    let requested = parse_time(record.stop_requested_at.as_deref());
                    // A little slack: the runtime's clock and ours are the same host's, but its
                    // timestamps are finer and a stop can be recorded a hair after the exit.
                    let after_request =
                        requested.is_some_and(|r| i.finished_at.is_none_or(|f| f >= r - time::Duration::seconds(2)));
                    if after_request || (clean_exit(i.exit_code) && !i.oom_killed) {
                        WorkloadState::Stopped
                    } else {
                        WorkloadState::Crashed
                    }
                }
                ContainerStatus::Unknown => WorkloadState::Unknown,
            },
        },
    }
}

/// Requested stops end with these without being failures: SIGTERM handled late (143), SIGINT
/// (130), or a clean exit.
pub(super) fn clean_exit(code: i64) -> bool {
    matches!(code, 0 | 130 | 143)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    fn record(phase: Phase, stop_requested_at: Option<&str>) -> WorkloadRecord {
        let mut r = crate::store::tests::record("w");
        r.phase = phase;
        r.stop_requested_at = stop_requested_at.map(str::to_owned);
        r
    }

    fn exited(code: i64, oom: bool, finished: &str) -> ContainerInfo {
        ContainerInfo {
            id: "c".into(),
            name: "n".into(),
            labels: BTreeMap::new(),
            env: BTreeMap::new(),
            status: ContainerStatus::Exited,
            exit_code: code,
            oom_killed: oom,
            started_at: None,
            finished_at: parse_time(Some(finished)),
            restart_count: 0,
            health: None,
        }
    }

    #[test]
    fn exits_are_stops_or_crashes_by_what_was_asked() {
        let at = "2026-09-28T10:00:00Z";
        let later = "2026-09-28T10:00:05Z";
        let r = record(Phase::Active, None);
        assert_eq!(
            derive_state(&r, Some(&exited(0, false, later)), Stopping::NotAsked, Sight::Current),
            WorkloadState::Stopped
        );
        assert_eq!(
            derive_state(&r, Some(&exited(1, false, later)), Stopping::NotAsked, Sight::Current),
            WorkloadState::Crashed
        );
        assert_eq!(
            derive_state(&r, Some(&exited(137, true, later)), Stopping::NotAsked, Sight::Current),
            WorkloadState::Crashed
        );
        let asked = record(Phase::Active, Some(at));
        assert_eq!(
            derive_state(&asked, Some(&exited(137, false, later)), Stopping::NotAsked, Sight::Current),
            WorkloadState::Stopped
        );
        // A crash before the stop request is still a crash.
        let long_before = "2026-09-28T09:00:00Z";
        assert_eq!(
            derive_state(&asked, Some(&exited(1, false, long_before)), Stopping::NotAsked, Sight::Current),
            WorkloadState::Crashed
        );
    }

    #[test]
    fn missing_vs_unknown_depends_on_whether_the_runtime_answered() {
        let r = record(Phase::Active, None);
        assert_eq!(derive_state(&r, None, Stopping::NotAsked, Sight::Current), WorkloadState::Missing);
        assert_eq!(derive_state(&r, None, Stopping::NotAsked, Sight::Stale), WorkloadState::Unknown);
        assert_eq!(
            derive_state(&record(Phase::Retained, None), None, Stopping::NotAsked, Sight::Current),
            WorkloadState::Retained
        );
        assert_eq!(
            derive_state(&record(Phase::Creating, None), None, Stopping::NotAsked, Sight::Current),
            WorkloadState::Creating
        );
    }
}
