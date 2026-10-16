// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! The priority a thread doing a transfer works at: the lowest CPU priority (nice 19) and the idle
//! I/O class, so a server booting on the node gets the CPU and the disk first and a snapshot,
//! export or restore beside it takes what is left.
//!
//! [`Idle`] lowers the thread it is entered on and puts both back when it drops. The threads are
//! tokio's blocking pool, shared with every other piece of blocking work, so the work done there
//! never hands a thread back still lowered.
//!
//! Lowering is always allowed; raising the nice value back is not. It takes `CAP_SYS_NICE`, or an
//! `RLIMIT_NICE` that reaches the thread's old value (systemd's `LimitNICE=`). Where it can't come
//! back, transfers keep their CPU priority and take only the idle I/O class, which any thread may
//! leave again. Whether it can is tried once, on a thread of its own that ends after it.
//!
//! Both calls name the calling thread by `who = 0`: on Linux the nice value and the I/O priority
//! belong to each thread, not to the process.

use std::marker::PhantomData;
use std::sync::OnceLock;

/// The lowest priority `setpriority(2)` gives a thread: nice values run from -20 to 19.
const IDLE_NICE: i32 = 19;

// `ioprio_set(2)`'s numbers, from the kernel's include/uapi/linux/ioprio.h. libc exports the
// syscalls but none of these.
/// `which`: one thread, named by its id (0 for the caller).
const IOPRIO_WHO_PROCESS: libc::c_int = 1;
/// Where the class sits in an I/O priority value; the bits below are its level.
const IOPRIO_CLASS_SHIFT: u32 = 13;
/// The class whose I/O is served only when nothing else asks for the disk.
const IOPRIO_CLASS_IDLE: i32 = 3;

/// Held while a thread does transfer work: the thread runs at nice 19 in the idle I/O class until
/// this drops, and then at what it had before. It belongs to the thread it was entered on.
#[must_use = "the thread goes back to its priority when this drops"]
pub struct Idle {
    /// The nice value to put back, when it was lowered.
    nice: Option<i32>,
    /// The I/O priority to put back, when it was changed.
    io: Option<i32>,
    _thread: PhantomData<*const ()>,
}

impl Idle {
    /// Lowers the calling thread. What can't be lowered stays as it was; the work goes on either
    /// way.
    pub fn enter() -> Self {
        let nice = match rustix::process::getpriority_process(None) {
            Ok(was) if was < IDLE_NICE && nice_comes_back() => {
                rustix::process::setpriority_process(None, IDLE_NICE).ok().map(|()| was)
            }
            _ => None,
        };
        let io = io_priority().ok().filter(|_| set_io_priority(IOPRIO_CLASS_IDLE << IOPRIO_CLASS_SHIFT).is_ok());
        Self { nice, io, _thread: PhantomData }
    }
}

impl Drop for Idle {
    fn drop(&mut self) {
        if let Some(was) = self.io
            && let Err(e) = set_io_priority(was)
        {
            tracing::warn!(error = %e, "a transfer thread kept the idle I/O class");
        }
        if let Some(was) = self.nice
            && let Err(e) = rustix::process::setpriority_process(None, was)
        {
            tracing::warn!(error = %e, "a transfer thread kept nice {IDLE_NICE}");
        }
    }
}

/// Whether a thread lowered to nice 19 may go back: tried once, on a thread that ends after it.
fn nice_comes_back() -> bool {
    static BACK: OnceLock<bool> = OnceLock::new();
    *BACK.get_or_init(|| {
        let tried = std::thread::spawn(|| {
            let was = rustix::process::getpriority_process(None)?;
            rustix::process::setpriority_process(None, IDLE_NICE)?;
            rustix::process::setpriority_process(None, was)
        })
        .join();
        let back = matches!(tried, Ok(Ok(())));
        if !back {
            tracing::info!(
                "transfers keep their CPU priority and take only the idle I/O class: lowering a thread's \
                 nice value for good needs CAP_SYS_NICE or LimitNICE=+0"
            );
        }
        back
    })
}

/// The calling thread's I/O priority, as `ioprio_get(2)` gives it.
#[expect(unsafe_code, reason = "ioprio_get, which neither rustix nor libc wraps")]
fn io_priority() -> std::io::Result<i32> {
    // SAFETY: ioprio_get takes two integers and reads no memory.
    let got = unsafe { libc::syscall(libc::SYS_ioprio_get, IOPRIO_WHO_PROCESS, 0) };
    if got < 0 { Err(std::io::Error::last_os_error()) } else { Ok(got as i32) }
}

#[expect(unsafe_code, reason = "ioprio_set, which neither rustix nor libc wraps")]
fn set_io_priority(priority: i32) -> std::io::Result<()> {
    // SAFETY: ioprio_set takes three integers and reads no memory.
    let set = unsafe { libc::syscall(libc::SYS_ioprio_set, IOPRIO_WHO_PROCESS, 0, priority) };
    if set < 0 { Err(std::io::Error::last_os_error()) } else { Ok(()) }
}

/// The calling thread's nice value and I/O class, for tests that watch a transfer's thread.
#[cfg(test)]
fn now() -> (i32, i32) {
    let nice = rustix::process::getpriority_process(None).unwrap();
    (nice, io_priority().unwrap() >> IOPRIO_CLASS_SHIFT)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::thread::ThreadId;

    use super::*;

    type Seen = (ThreadId, (i32, i32));

    fn seen() -> Seen {
        (std::thread::current().id(), now())
    }

    #[test]
    fn a_transfer_works_at_idle_priority_and_hands_its_thread_back_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        fs::create_dir_all(data.join("world/region")).unwrap();
        fs::write(data.join("server.properties"), "level-name=world\n").unwrap();
        fs::write(data.join("world/region/r.0.0.mca"), vec![7u8; 70_000]).unwrap();
        let me = (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw());

        // One blocking thread, so every piece of work below runs on the same one, as work after a
        // transfer would.
        let runtime = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).build().unwrap();
        let on_the_pool =
            |work: Box<dyn FnOnce() -> Vec<Seen> + Send>| runtime.block_on(runtime.spawn_blocking(work)).unwrap();
        let (path, out) = (dir.path().to_owned(), data.clone());
        let mut during = Vec::new();
        let before = on_the_pool(Box::new(|| vec![seen()]))[0];
        during.push((
            "packing",
            on_the_pool(Box::new(move || {
                let mut seen_at = Vec::new();
                crate::tarball::pack(&out, &path.join("a.tar.gz"), &[], &mut |_| {
                    seen_at.push(seen());
                    Ok(())
                })
                .unwrap();
                seen_at
            })),
        ));
        let after_pack = on_the_pool(Box::new(|| vec![seen()]))[0];
        let (path, from) = (dir.path().to_owned(), data.clone());
        during.push((
            "copying",
            on_the_pool(Box::new(move || {
                let mut seen_at = Vec::new();
                crate::tree::copy_tree(&from, &path.join("copy"), None, &[], &mut |_| {
                    seen_at.push(seen());
                    Ok(())
                })
                .unwrap();
                seen_at
            })),
        ));
        let after_copy = on_the_pool(Box::new(|| vec![seen()]))[0];
        let path = dir.path().to_owned();
        during.push((
            "unpacking",
            on_the_pool(Box::new(move || {
                let mut seen_at = Vec::new();
                crate::tarball::unpack(&path.join("a.tar.gz"), &path.join("restored"), me, &mut |_| {
                    seen_at.push(seen());
                    Ok(())
                })
                .unwrap();
                seen_at
            })),
        ));
        let after_unpack = on_the_pool(Box::new(|| vec![seen()]))[0];

        let (thread, (nice, _)) = before;
        let lowered = if nice < IDLE_NICE && nice_comes_back() { IDLE_NICE } else { nice };
        for (what, seen_at) in during {
            assert!(!seen_at.is_empty(), "{what} reported no progress");
            for at in seen_at {
                assert_eq!(at, (thread, (lowered, IOPRIO_CLASS_IDLE)), "while {what}");
            }
        }
        for (what, after) in [("packing", after_pack), ("copying", after_copy), ("unpacking", after_unpack)] {
            assert_eq!(after, before, "after {what}");
        }
    }

    #[test]
    fn idle_nests_and_the_outer_one_puts_the_thread_back() {
        let before = now();
        let outer = Idle::enter();
        let inner = Idle::enter();
        drop(inner);
        assert_eq!(now().1, IOPRIO_CLASS_IDLE, "the outer one still holds");
        drop(outer);
        assert_eq!(now(), before);
    }
}
