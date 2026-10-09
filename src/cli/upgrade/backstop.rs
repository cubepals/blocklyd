//! The trial's backstop: a blocklyd that starts on trial and hasn't ended the trial DEADLINE
//! after it started ends itself, with status 1, and the unit's `ExecStopPost` (`blocklyd.prev
//! upgrade --exited`) puts the binary before it back, as after any start that failed.
//!
//! `Upgrader::watch_trial` counts TRIAL_SECONDS only from the end of startup, and stops blocklyd by
//! cancelling its tasks: a startup that hangs never starts that clock, and a task that never ends
//! keeps the process up after it. So this is a plain thread, which a stalled runtime can't hold
//! up, waiting on nothing but the clock and a flag the Upgrader sets when a heartbeat ends the
//! trial.
//!
//! Not here: putting a binary back. Only the binary before it does that after this exit, so
//! nothing here races `watch_trial` or the heartbeat that ends the trial.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tracing::{error, warn};

use super::{Layout, TRIAL_SECONDS, on_trial};

/// How long after the process started a trial must have ended: startup, then the trial itself.
pub const DEADLINE: Duration = Duration::from_secs(2 * TRIAL_SECONDS);

/// The backstop of one start: armed only when it is on trial.
#[derive(Debug, Default)]
pub struct Backstop {
    ended: Arc<AtomicBool>,
    armed: bool,
}

impl Backstop {
    /// Armed when `state_dir` names a trial for this binary, counting from `started`, when the
    /// process started.
    pub fn arm(state_dir: &Path, started: Instant) -> Self {
        let version = crate::fleet::daemon_version();
        let mut backstop = Backstop::default();
        if on_trial(&Layout::new(state_dir, Path::new("/")), &version).is_none() {
            return backstop;
        }
        let ended = backstop.ended.clone();
        let watch = move || {
            loop {
                match due(ended.load(Ordering::SeqCst), started.elapsed()) {
                    Due::Never => return,
                    Due::In(wait) => std::thread::sleep(wait),
                    Due::Now => {
                        error!(
                            version,
                            "still on trial {} s after starting: stopping, for the blocklyd before it to be put back",
                            DEADLINE.as_secs()
                        );
                        std::process::exit(1);
                    }
                }
            }
        };
        match std::thread::Builder::new().name("trial-backstop".into()).spawn(watch) {
            Ok(_) => backstop.armed = true,
            Err(e) => warn!(error = %e, "couldn't arm the trial's backstop; the trial still ends on its own"),
        }
        backstop
    }

    /// Set once a heartbeat ended the trial (`Upgrader::accepted`): blocklyd then runs on.
    pub fn ended(&self) -> Arc<AtomicBool> {
        self.ended.clone()
    }
}

#[derive(Debug, PartialEq)]
enum Due {
    /// The trial ended: nothing to do, ever.
    Never,
    /// Look again then.
    In(Duration),
    /// Stop the process.
    Now,
}

/// What the backstop does, `elapsed` since the process started.
fn due(ended: bool, elapsed: Duration) -> Due {
    if ended {
        Due::Never
    } else if elapsed < DEADLINE {
        Due::In(DEADLINE - elapsed)
    } else {
        Due::Now
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::upgrade::{Trial, swap};

    #[test]
    fn a_trial_not_ended_by_the_deadline_stops_the_process_and_one_ended_never_does() {
        assert_eq!(DEADLINE, Duration::from_secs(240), "startup, then the 120 s trial");
        assert_eq!(due(false, Duration::ZERO), Due::In(DEADLINE));
        assert_eq!(due(false, Duration::from_secs(100)), Due::In(Duration::from_secs(140)));
        assert_eq!(due(false, DEADLINE), Due::Now);
        assert_eq!(due(false, DEADLINE * 3), Due::Now);
        for elapsed in [Duration::ZERO, DEADLINE, DEADLINE * 3] {
            assert_eq!(due(true, elapsed), Due::Never);
        }
    }

    #[test]
    fn only_a_start_on_trial_is_armed() {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("var/lib/blocklyd");
        let layout = Layout::new(&state, root.path());
        assert!(!Backstop::arm(&state, Instant::now()).armed, "no trial");
        let trial = |to: String| Trial { from: "0.2.0".into(), to, sha256: "ab".repeat(32) };
        for (path, version) in [(&layout.bin, "0.2.0"), (&layout.next(), "99.0.0")] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, version).unwrap();
        }
        swap(&layout, &trial("99.0.0".into())).unwrap();
        assert!(!Backstop::arm(&state, Instant::now()).armed, "a trial for another version");
        std::fs::write(layout.next(), "").unwrap();
        swap(&layout, &trial(crate::fleet::daemon_version())).unwrap();
        let backstop = Backstop::arm(&state, Instant::now());
        // Ended at once: the thread finds it so when it wakes, and returns.
        backstop.ended().store(true, Ordering::SeqCst);
        assert!(backstop.armed, "a trial for this binary");
    }
}
