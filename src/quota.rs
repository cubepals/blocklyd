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

/// The project a workload's data is counted in: from its key alone, so every node and every
/// restart agrees, and no file of the host's can drift from it. FNV-1a, folded into the range.
pub(crate) fn project_of(id: &WorkloadId) -> u32 {
    let hash = id.as_str().bytes().fold(0x811c_9dc5_u32, |h, b| (h ^ u32::from(b)).wrapping_mul(0x0100_0193));
    FIRST_PROJECT + hash % (LAST_PROJECT - FIRST_PROJECT)
}

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

    fn get(&self, project: u32) -> io::Result<libc::dqblk> {
        let mut quota = no_quota();
        self.quotactl(libc::Q_GETQUOTA, project, &mut quota)?;
        Ok(quota)
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
/// Does nothing where `dir` is in it already: what is created in it since inherits. Blocking.
pub(crate) fn assign(dir: &Path, project: u32) -> io::Result<()> {
    let root = rustix::fs::open(dir, dir_flags(), Mode::empty())?;
    let attrs = get_attrs(root.as_fd())?;
    if attrs.fsx_projid == project && attrs.fsx_xflags & FS_XFLAG_PROJINHERIT != 0 {
        return Ok(());
    }
    // The root first, so what is created during the walk inherits; the rest after.
    set_attrs(root.as_fd(), attrs, project, true)?;
    assign_under(root, project, 0)
}

fn assign_under(dir: OwnedFd, project: u32, depth: usize) -> io::Result<()> {
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
        set_attrs(opened.as_fd(), attrs, project, kind == FileType::Directory)?;
        if kind == FileType::Directory {
            assign_under(opened, project, depth + 1)?;
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
    fn a_filesystem_without_project_quotas_is_left_to_the_walk() {
        let dir = tempfile::tempdir().unwrap();
        assert!(Quotas::open(dir.path()).is_none());
    }
}
