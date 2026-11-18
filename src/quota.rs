// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Each workload's data held to the size its spec promised (`storage.sizeGb`), by the
//! filesystem's own project quotas: XFS mounted with `prjquota`, or ext4 made with `project` and
//! mounted so. A server writing past its size gets `EDQUOT` from the kernel, the node's disk never
//! fills, and its use is read from the quota in O(1) instead of walked.
//!
//! - Every data directory is put in a project of its own, derived from the workload's key
//!   (`project_of`), never from a file of the host's, with `FS_XFLAG_PROJINHERIT` so all it
//!   creates is counted. A directory that holds files already (an older node, a restore swapped
//!   in) has them all put in it, walking as `tree.rs` does: relative to open directories, links
//!   never followed, only directories and regular files touched.
//! - The project's hard limit is the spec's size. `statfs` inside the directory then reports that
//!   limit as the disk's size, so what reads the room left from inside the workload (the
//!   control plane's disk growth) sees the quota.
//! - A filesystem without project quotas is left as it was: blocklyd walks it as before
//!   (`Quotas::open` gives None). Nothing here ever refuses a start: a quota that can't be set is
//!   reported, and the workload runs.
//!
//! Not here: when the size grows (the control plane's), or which filesystem a host has (`doctor`).

use std::fs::File;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd};
use std::path::Path;

use linux_raw_sys::general::{FS_XFLAG_PROJINHERIT, fsxattr};
use linux_raw_sys::ioctl::{FS_IOC_FSGETXATTR, FS_IOC_FSSETXATTR};
use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags};
use rustix::io::Errno;

use crate::ids::WorkloadId;
use crate::tree::MAX_DEPTH;

/// `PRJQUOTA` from linux/quota.h, which neither libc nor linux-raw-sys exports.
const PRJQUOTA: libc::c_int = 2;

/// The unit of a quota's block limit (`QIF_DQBLKSIZE`, linux/quota.h), whatever the block size.
const QUOTA_BLOCK_BYTES: u64 = 1024;

/// Project ids below this are left to the host's own (`/etc/projid`).
const FIRST_PROJECT: u32 = 1 << 20;

/// Above this, 16-bit project ids (old XFS) can't hold them; XFS made since 2013 and ext4 take 32.
const LAST_PROJECT: u32 = i32::MAX as u32;

const GIB: u64 = 1 << 30;

/// The limit a project is held to: the spec's size, never below what the data already holds.
/// `current` is the limit the filesystem keeps for it now (0: none yet). Data found larger than its
/// size the first time it is held keeps a gibibyte of room above it, rounded to whole gibibytes,
/// and that limit stays until the size grows past it: nothing that ran before is broken by the
/// limit arriving. No size, no limit.
pub(crate) fn limit_for(size_gb: u32, used: u64, current: u64) -> Option<u64> {
    let size = u64::from(size_gb) * GIB;
    if size == 0 {
        return None;
    }
    if current != 0 {
        return Some(size.max(current));
    }
    Some(if used > size { (used + GIB).next_multiple_of(GIB) } else { size })
}

/// The project a workload's data is first offered: from its key, so the same workload is offered the
/// same one anywhere. FNV-1a, folded into the range. Two keys can share one, so the manager takes
/// the next free one where it is taken here (`next_project`), and keeps what it took.
pub(crate) fn project_of(id: &WorkloadId) -> u32 {
    let hash = id.as_str().bytes().fold(0x811c_9dc5_u32, |h, b| (h ^ u32::from(b)).wrapping_mul(0x0100_0193));
    FIRST_PROJECT + hash % (LAST_PROJECT - FIRST_PROJECT)
}

/// The first project from what `id` is offered that isn't `taken`.
pub(crate) fn first_free(id: &WorkloadId, taken: &std::collections::HashSet<u32>) -> u32 {
    let mut project = project_of(id);
    while taken.contains(&project) {
        project = next_project(project);
    }
    project
}

/// The project after `project`, within the range.
fn next_project(project: u32) -> u32 {
    if project + 1 >= LAST_PROJECT { FIRST_PROJECT } else { project + 1 }
}

/// What a workload's record of its project (`assign`'s `done`) says: the project, whether the
/// walk that puts its data in it finished or is only reserved (`pending <project>`).
pub(crate) fn recorded(text: &str) -> Option<u32> {
    text.trim().trim_start_matches(PENDING).trim().parse().ok()
}

/// How a project taken but not yet walked is recorded.
pub(crate) const PENDING: &str = "pending";

/// The project quotas of the filesystem holding the workloads.
pub(crate) struct Quotas {
    fs: File,
}

impl Quotas {
    /// None when `dir`'s filesystem keeps no project quotas, or blocklyd may not manage them.
    pub(crate) fn open(dir: &Path) -> Option<Self> {
        let quotas = Self { fs: File::open(dir).ok()? };
        quotas.get(FIRST_PROJECT).ok().map(|_| quotas)
    }

    /// A project's quota; an empty one for a project the filesystem keeps no record of yet, which
    /// it answers with ENOENT while quotas are on (ESRCH means they are off).
    fn get(&self, project: u32) -> io::Result<libc::dqblk> {
        let mut quota = no_quota();
        match self.quotactl(libc::Q_GETQUOTA, project, &mut quota) {
            Err(e) if e.raw_os_error() == Some(libc::ENOENT) => Ok(no_quota()),
            done => done.map(|()| quota),
        }
    }

    /// The bytes `project` holds now.
    pub(crate) fn used(&self, project: u32) -> io::Result<u64> {
        Ok(self.get(project)?.dqb_curspace)
    }

    /// Holds `project` to `bytes`, rounded up to the quota's unit.
    pub(crate) fn limit(&self, project: u32, bytes: u64) -> io::Result<()> {
        let mut quota = no_quota();
        quota.dqb_bhardlimit = bytes.div_ceil(QUOTA_BLOCK_BYTES);
        quota.dqb_valid = libc::QIF_BLIMITS;
        self.quotactl(libc::Q_SETQUOTA, project, &mut quota)
    }

    /// The limit `project` is held to now, in bytes; 0 for none.
    pub(crate) fn limit_of(&self, project: u32) -> io::Result<u64> {
        Ok(self.get(project)?.dqb_bhardlimit * QUOTA_BLOCK_BYTES)
    }

    /// quotactl_fd(2): the filesystem named by an open directory, so no block device is needed
    /// (blocklyd's unit sees none).
    #[expect(unsafe_code, reason = "quotactl_fd, which neither rustix nor libc wraps")]
    fn quotactl(&self, cmd: libc::c_int, project: u32, quota: &mut libc::dqblk) -> io::Result<()> {
        let id = libc::c_int::try_from(project).map_err(io::Error::other)?;
        // SAFETY: the descriptor is open for the call, and `quota` is a valid dqblk it may fill.
        let done = unsafe {
            libc::syscall(
                libc::SYS_quotactl_fd,
                self.fs.as_raw_fd(),
                libc::QCMD(cmd, PRJQUOTA),
                id,
                std::ptr::from_mut(quota),
            )
        };
        if done == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
    }
}

/// Puts `dir` and everything in it in `project`, new entries included (`FS_XFLAG_PROJINHERIT`).
/// `done` (beside `dir`, out of the workload's reach) records a walk that finished: until it names
/// `project`, a walk cut short is taken up again. Does nothing where both say it is done: what is
/// created since inherits. Blocking.
pub(crate) fn assign(dir: &Path, project: u32, done: &Path) -> io::Result<()> {
    if assigned(dir, project, done)? {
        return Ok(());
    }
    let root = rustix::fs::open(dir, dir_flags(), Mode::empty())?;
    let attrs = get_attrs(root.as_fd())?;
    // The root first, so what is created during the walk inherits; the rest after.
    set_attrs(root.as_fd(), attrs, project, true)?;
    walk_setting(root, project, true, 0)?;
    std::fs::write(done, project.to_string())
}

/// Whether `dir` is wholly in `project`: its walk finished (`done` names it) and its root still
/// carries it, so data swapped in since is walked again. Blocking.
pub(crate) fn assigned(dir: &Path, project: u32, done: &Path) -> io::Result<bool> {
    let root = rustix::fs::open(dir, dir_flags(), Mode::empty())?;
    let attrs = get_attrs(root.as_fd())?;
    let finished = std::fs::read_to_string(done).ok().is_some_and(|d| d.trim() == project.to_string());
    Ok(finished && attrs.fsx_projid == project && attrs.fsx_xflags & FS_XFLAG_PROJINHERIT != 0)
}

/// Takes `dir` and everything in it out of any project (back to 0, inheriting nothing): what a
/// workload no longer holds, such as data a restore replaced, kept in the trash for a while, stops
/// counting against its limit. The root last, so a walk cut short is taken up again. Blocking.
pub(crate) fn release(dir: &Path) -> io::Result<()> {
    let root = rustix::fs::open(dir, dir_flags(), Mode::empty())?;
    let attrs = get_attrs(root.as_fd())?;
    if attrs.fsx_projid == 0 && attrs.fsx_xflags & FS_XFLAG_PROJINHERIT == 0 {
        return Ok(());
    }
    let again = rustix::fs::open(dir, dir_flags(), Mode::empty())?;
    walk_setting(again, 0, false, 0)?;
    set_attrs(root.as_fd(), attrs, 0, false)
}

/// Sets `project` on everything under `dir`; directories inherit it when `inherit` says so.
fn walk_setting(dir: OwnedFd, project: u32, inherit: bool, depth: usize) -> io::Result<()> {
    if depth >= MAX_DEPTH {
        return Err(io::Error::other(format!("a tree nests deeper than {MAX_DEPTH} directories")));
    }
    let mut entries = Dir::new(dir)?;
    while let Some(entry) = entries.read() {
        let entry = entry?;
        let name = entry.file_name();
        if name == c"." || name == c".." {
            continue;
        }
        let parent = entries.fd()?;
        let kind = match rustix::fs::statat(parent, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => FileType::from_raw_mode(stat.st_mode),
            Err(e) if vanished(e) => continue,
            Err(e) => return Err(e.into()),
        };
        let flags = match kind {
            FileType::Directory => dir_flags(),
            // Non-blocking, so a file under a lease is skipped rather than waited for.
            FileType::RegularFile => OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            // Links, FIFOs, sockets and devices hold no blocks worth counting, and are never opened.
            _ => continue,
        };
        let opened = match rustix::fs::openat(parent, name, flags, Mode::empty()) {
            Ok(fd) => fd,
            Err(e) if vanished(e) || e == Errno::AGAIN => continue,
            Err(e) => return Err(e.into()),
        };
        // As opened, not as looked at: an entry swapped since is taken for what it is now.
        let now = FileType::from_raw_mode(rustix::fs::fstat(&opened)?.st_mode);
        if now != kind {
            continue;
        }
        let attrs = get_attrs(opened.as_fd())?;
        set_attrs(opened.as_fd(), attrs, project, inherit && kind == FileType::Directory)?;
        if kind == FileType::Directory {
            walk_setting(opened, project, inherit, depth + 1)?;
        }
    }
    Ok(())
}

fn dir_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

/// Gone, or replaced by something that can't be opened this way, between listing and opening.
fn vanished(e: Errno) -> bool {
    matches!(e, Errno::NOENT | Errno::LOOP | Errno::NOTDIR | Errno::NXIO)
}

/// A quota with nothing in it, for quotactl to fill or to read the fields `dqb_valid` names from.
fn no_quota() -> libc::dqblk {
    libc::dqblk {
        dqb_bhardlimit: 0,
        dqb_bsoftlimit: 0,
        dqb_curspace: 0,
        dqb_ihardlimit: 0,
        dqb_isoftlimit: 0,
        dqb_curinodes: 0,
        dqb_btime: 0,
        dqb_itime: 0,
        dqb_valid: 0,
    }
}

fn get_attrs(fd: BorrowedFd<'_>) -> io::Result<fsxattr> {
    let mut attrs =
        fsxattr { fsx_xflags: 0, fsx_extsize: 0, fsx_nextents: 0, fsx_projid: 0, fsx_cowextsize: 0, fsx_pad: [0; 8] };
    fsxattr_ioctl(fd, FS_IOC_FSGETXATTR, &mut attrs)?;
    Ok(attrs)
}

fn set_attrs(fd: BorrowedFd<'_>, mut attrs: fsxattr, project: u32, inherit: bool) -> io::Result<()> {
    attrs.fsx_projid = project;
    if inherit {
        attrs.fsx_xflags |= FS_XFLAG_PROJINHERIT;
    } else {
        attrs.fsx_xflags &= !FS_XFLAG_PROJINHERIT;
    }
    fsxattr_ioctl(fd, FS_IOC_FSSETXATTR, &mut attrs)
}

/// FS_IOC_FSGETXATTR fills `attrs`, FS_IOC_FSSETXATTR reads it.
#[expect(unsafe_code, reason = "the fsxattr ioctls, which rustix doesn't wrap")]
fn fsxattr_ioctl(fd: BorrowedFd<'_>, request: u32, attrs: &mut fsxattr) -> io::Result<()> {
    // SAFETY: the descriptor is open, both requests take a pointer to one fsxattr, and `attrs` is one.
    let done = unsafe { libc::ioctl(fd.as_raw_fd(), libc::Ioctl::from(request), std::ptr::from_mut(attrs)) };
    if done == 0 { Ok(()) } else { Err(io::Error::last_os_error()) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_workload_has_one_project_of_its_own_above_the_hosts() {
        let a = WorkloadId::parse("a1b2c3d4-0000-4000-8000-000000000001").unwrap();
        let b = WorkloadId::parse("a1b2c3d4-0000-4000-8000-000000000002").unwrap();
        assert_eq!(project_of(&a), project_of(&a));
        assert_ne!(project_of(&a), project_of(&b));
        for id in [&a, &b] {
            assert!((FIRST_PROJECT..LAST_PROJECT).contains(&project_of(id)));
        }
    }

    #[test]
    fn the_limit_is_the_size_and_never_below_what_the_data_holds() {
        // A new workload: its size.
        assert_eq!(limit_for(5, 0, 0), Some(5 * GIB));
        // Data within its size when the limit first comes: its size, however full.
        assert_eq!(limit_for(5, 5 * GIB, 0), Some(5 * GIB));
        // Larger than its size: kept, with a gibibyte of room, in whole gibibytes.
        assert_eq!(limit_for(5, 7 * GIB + 1, 0), Some(9 * GIB));
        // Once set, that limit stays while the size is smaller, and follows the size past it.
        assert_eq!(limit_for(5, 9 * GIB, 9 * GIB), Some(9 * GIB));
        assert_eq!(limit_for(12, 9 * GIB, 9 * GIB), Some(12 * GIB));
        // A size that grows raises the limit.
        assert_eq!(limit_for(8, GIB, 5 * GIB), Some(8 * GIB));
    }

    #[test]
    fn no_size_no_limit() {
        assert_eq!(limit_for(0, 3 * GIB, 0), None);
        // And the protocol never lets a spec come without one.
        let spec = serde_json::json!({ "image": "alpine:3.22", "resources": { "memoryMb": 256 },
            "storage": { "mountPath": "/data" } });
        assert!(serde_json::from_value::<crate::protocol::WorkloadSpec>(spec).is_err());
    }

    #[test]
    fn two_workloads_whose_keys_share_a_project_get_two() {
        let a = WorkloadId::parse("a1b2c3d4-0000-4000-8000-0000000145af").unwrap();
        let b = WorkloadId::parse("a1b2c3d4-0000-4000-8000-00000002a720").unwrap();
        assert_eq!(project_of(&a), project_of(&b), "the pair found in review");
        let taken = std::collections::HashSet::from([first_free(&a, &Default::default())]);
        assert_ne!(first_free(&b, &taken), project_of(&a));
    }

    #[test]
    fn a_taken_project_is_passed_over_within_the_range() {
        assert_eq!(next_project(FIRST_PROJECT), FIRST_PROJECT + 1);
        assert_eq!(next_project(LAST_PROJECT - 1), FIRST_PROJECT);
        assert_eq!(recorded("1234567"), Some(1_234_567));
        assert_eq!(recorded("pending 1234567\n"), Some(1_234_567));
        assert_eq!(recorded("garbage"), None);
    }

    #[test]
    fn a_filesystem_without_project_quotas_is_left_to_the_walk() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Quotas::open(dir.path()).is_none());
    }
}
