//! Walking a directory a workload can write to, without being led anywhere else.
//!
//! A server's data directory belongs to whatever runs in its container: plugins and mods can
//! create, rename and replace anything in it, while blocklyd reads it as root. A walk by path
//! follows a directory that was swapped for a symlink between looking and opening, and would read
//! the host's files (this node's key, say) into a backup its owner can download. So every step
//! here is taken relative to a directory already open, with `O_NOFOLLOW`:
//!
//! - a symlink is reported as a symlink and never followed;
//! - a directory or file replaced while the walk looks at it is skipped;
//! - a FIFO, socket or device is never opened;
//! - a file is read through the descriptor that was opened and checked.
//!
//! `copy_tree` uses it for local snapshots, sharing blocks with the source (`FICLONE`) where the
//! filesystem can, and `transfer::pack` for archives. `disk_usage` measures a tree by the same
//! rules, opening only its directories.

use std::ffi::{CString, OsStr};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use rustix::fs::{AtFlags, Dir, FileType, Mode, OFlags, Timespec, Timestamps};
use rustix::io::Errno;

pub use crate::protocol::Method;

/// Directories deeper than this are refused rather than walked: no world nests like that, and
/// the walk holds a descriptor per level.
pub const MAX_DEPTH: usize = 128;
/// Entries a walk visits before it gives up: worlds hold thousands.
pub const MAX_ENTRIES: u64 = 2_000_000;

/// What the walk saw of an entry, from the descriptor it opened (or, for a link, the link itself).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Meta {
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub mtime: i64,
    pub mtime_nsec: i64,
}

impl From<&rustix::fs::Stat> for Meta {
    #[allow(clippy::unnecessary_cast)] // The field types differ between architectures.
    fn from(s: &rustix::fs::Stat) -> Self {
        Self {
            mode: s.st_mode as u32,
            uid: s.st_uid as u32,
            gid: s.st_gid as u32,
            size: s.st_size.max(0) as u64,
            mtime: s.st_mtime as i64,
            mtime_nsec: s.st_mtime_nsec as i64,
        }
    }
}

pub enum Kind<'a> {
    Dir,
    /// Opened read-only, at offset 0.
    File(&'a mut File),
    /// The link's target, as stored. Never resolved.
    Link(&'a Path),
}

pub struct Entry<'a> {
    /// Relative to the root; empty for the root itself, which comes first.
    pub path: &'a Path,
    pub meta: Meta,
    pub kind: Kind<'a>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Walked {
    pub dirs: u64,
    pub files: u64,
    pub links: u64,
    /// FIFOs, sockets, devices, and entries that changed while the walk looked at them.
    pub skipped: u64,
    /// The regular files' sizes when they were opened.
    pub bytes: u64,
}

impl Walked {
    pub fn entries(&self) -> u64 {
        self.dirs + self.files + self.links
    }
}

/// Visits `root` and everything under it, parents before children, names in byte order. Names in
/// `exclude` are left out at the top level only. Blocking.
pub fn walk(root: &Path, exclude: &[String], visit: &mut dyn FnMut(Entry<'_>) -> io::Result<()>) -> io::Result<Walked> {
    walk_at_most(root, exclude, MAX_ENTRIES, visit)
}

/// `walk`, giving up past `max_entries`: `MAX_ENTRIES`, but for tests.
fn walk_at_most(
    root: &Path,
    exclude: &[String],
    max_entries: u64,
    visit: &mut dyn FnMut(Entry<'_>) -> io::Result<()>,
) -> io::Result<Walked> {
    let fd = rustix::fs::open(root, dir_flags(), Mode::empty())?;
    let meta = Meta::from(&rustix::fs::fstat(&fd)?);
    let mut walked = Walked { dirs: 1, ..Walked::default() };
    visit(Entry { path: Path::new(""), meta, kind: Kind::Dir })?;
    let mut path = PathBuf::new();
    descend(&fd, &mut path, 0, exclude, max_entries, visit, &mut walked)?;
    Ok(walked)
}

fn dir_flags() -> OFlags {
    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC
}

/// The entry changed between being listed and being opened: it is skipped, not an error.
fn vanished(e: Errno) -> bool {
    matches!(e, Errno::NOENT | Errno::LOOP | Errno::NOTDIR | Errno::NXIO)
}

fn descend(
    dir: &OwnedFd,
    path: &mut PathBuf,
    depth: usize,
    exclude: &[String],
    max_entries: u64,
    visit: &mut dyn FnMut(Entry<'_>) -> io::Result<()>,
    walked: &mut Walked,
) -> io::Result<()> {
    if depth >= MAX_DEPTH {
        return Err(io::Error::other(format!("{} nests deeper than {MAX_DEPTH} directories", path.display())));
    }
    let mut names: Vec<CString> = Vec::new();
    for entry in Dir::read_from(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if name == c"." || name == c".." {
            continue;
        }
        if depth == 0 && exclude.iter().any(|e| e.as_bytes() == name.to_bytes()) {
            continue;
        }
        names.push(name.to_owned());
        // Counted as the names are read, so a directory of millions can't fill memory before the
        // limit is looked at: each name read is one more entry, or skip, once visited.
        if walked.entries() + walked.skipped + names.len() as u64 > max_entries {
            return Err(io::Error::other(format!("more than {max_entries} entries")));
        }
    }
    names.sort();
    for name in names {
        if walked.entries() + walked.skipped >= max_entries {
            return Err(io::Error::other(format!("more than {max_entries} entries")));
        }
        let stat = match rustix::fs::statat(dir, name.as_c_str(), AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(e) if vanished(e) => {
                walked.skipped += 1;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        path.push(OsStr::from_bytes(name.to_bytes()));
        let result = match FileType::from_raw_mode(stat.st_mode) {
            FileType::Directory => match rustix::fs::openat(dir, name.as_c_str(), dir_flags(), Mode::empty()) {
                Ok(child) => {
                    let meta = Meta::from(&rustix::fs::fstat(&child)?);
                    walked.dirs += 1;
                    visit(Entry { path, meta, kind: Kind::Dir })
                        .and_then(|()| descend(&child, path, depth + 1, exclude, max_entries, visit, walked))
                }
                Err(e) if vanished(e) => {
                    walked.skipped += 1;
                    Ok(())
                }
                Err(e) => Err(e.into()),
            },
            FileType::RegularFile => {
                // NONBLOCK: a FIFO swapped in after the stat would otherwise hang the open.
                let flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::NONBLOCK | OFlags::NOCTTY;
                match rustix::fs::openat(dir, name.as_c_str(), flags, Mode::empty()) {
                    Ok(fd) => {
                        let stat = rustix::fs::fstat(&fd)?;
                        if FileType::from_raw_mode(stat.st_mode) == FileType::RegularFile {
                            let meta = Meta::from(&stat);
                            walked.files += 1;
                            walked.bytes += meta.size;
                            let mut file = File::from(fd);
                            visit(Entry { path, meta, kind: Kind::File(&mut file) })
                        } else {
                            walked.skipped += 1;
                            Ok(())
                        }
                    }
                    Err(e) if vanished(e) => {
                        walked.skipped += 1;
                        Ok(())
                    }
                    Err(e) => Err(e.into()),
                }
            }
            FileType::Symlink => match rustix::fs::readlinkat(dir, name.as_c_str(), Vec::new()) {
                Ok(target) => {
                    walked.links += 1;
                    let target = PathBuf::from(OsStr::from_bytes(target.as_bytes()));
                    visit(Entry { path, meta: Meta::from(&stat), kind: Kind::Link(&target) })
                }
                Err(e) if vanished(e) || e == Errno::INVAL => {
                    walked.skipped += 1;
                    Ok(())
                }
                Err(e) => Err(e.into()),
            },
            _ => {
                walked.skipped += 1;
                Ok(())
            }
        };
        path.pop();
        result?;
    }
    Ok(())
}

/// Bytes a tree occupies on disk (allocated blocks, not apparent sizes). Each directory is opened
/// relative to its parent with `O_NOFOLLOW`, and anything else is looked at where it is, never
/// opened or followed. Entries on another filesystem (a mount inside the tree) are neither counted
/// nor entered. Blocking.
pub fn disk_usage(root: &Path) -> io::Result<u64> {
    let fd = rustix::fs::open(root, dir_flags(), Mode::empty())?;
    let (mut total, device) = allocated(&rustix::fs::fstat(&fd)?);
    count(fd, device, 0, &mut total)?;
    Ok(total)
}

/// Bytes allocated to an entry, and the filesystem it is on.
#[allow(clippy::unnecessary_cast)] // The field types differ between architectures.
fn allocated(stat: &rustix::fs::Stat) -> (u64, u64) {
    (stat.st_blocks.max(0) as u64 * 512, stat.st_dev as u64)
}

fn count(dir: OwnedFd, device: u64, depth: usize, total: &mut u64) -> io::Result<()> {
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
        let dir = entries.fd()?;
        let stat = match rustix::fs::statat(dir, name, AtFlags::SYMLINK_NOFOLLOW) {
            Ok(stat) => stat,
            Err(e) if vanished(e) => continue,
            Err(e) => return Err(e.into()),
        };
        let (bytes, on) = allocated(&stat);
        if on != device {
            continue;
        }
        if FileType::from_raw_mode(stat.st_mode) != FileType::Directory {
            *total += bytes;
            continue;
        }
        // Counted and entered as opened: a directory swapped for a link since the look above
        // doesn't open, and is skipped.
        match rustix::fs::openat(dir, name, dir_flags(), Mode::empty()) {
            Ok(child) => {
                let (bytes, on) = allocated(&rustix::fs::fstat(&child)?);
                if on == device {
                    *total += bytes;
                    count(child, device, depth + 1, total)?;
                }
            }
            Err(e) if vanished(e) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Copied {
    pub walked: Walked,
    pub method: Method,
}

/// The filesystem can't share blocks between these two files; copying is the only way.
fn no_reflink(e: Errno) -> bool {
    matches!(e, Errno::OPNOTSUPP | Errno::XDEV | Errno::NOTTY | Errno::NOSYS | Errno::PERM)
}

/// A long write its caller's `room` stopped: the disk is under the floor the caller keeps. It comes
/// back inside the `io::Error` that `copy_tree` and `transfer::pack` return, so the caller can tell
/// it from the disk's own failures (`NoRoom::of`), as `transfer::UnpackError::NoRoom` is told apart.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct NoRoom(pub String);

impl NoRoom {
    /// Tells `room` the bytes written so far; its refusal stops the write.
    pub fn ask(room: &mut dyn FnMut(u64) -> Result<(), String>, written: u64) -> io::Result<()> {
        room(written).map_err(|message| io::Error::other(NoRoom(message)))
    }

    /// What `room` said, if `e` is its refusal.
    pub fn of(e: &io::Error) -> Option<&str> {
        e.get_ref()?.downcast_ref::<NoRoom>().map(|refused| refused.0.as_str())
    }
}

/// Copies `src` to `dst`, which must not exist yet. Files share blocks with the source where the
/// filesystem can (XFS with reflink, btrfs), and are copied where it can't (ext4). Symlinks are
/// copied as symlinks; set-id bits and others' write are dropped, as an unpack drops them.
/// Everything is given to `owner`, or keeps the source's owner without one. Blocking.
///
/// After each file `room` is told the bytes copied so far, shared or not, and an `Err` from it
/// stops the copy (`NoRoom`): the caller keeps the disk above its floor this way. What was copied
/// until then stays for the caller to remove.
///
/// `dst` must lie where the workload can't reach (a snapshot, a restore's staging directory):
/// it is written by path.
pub fn copy_tree(
    src: &Path,
    dst: &Path,
    owner: Option<(u32, u32)>,
    exclude: &[String],
    room: &mut dyn FnMut(u64) -> Result<(), String>,
) -> io::Result<Copied> {
    let (mut reflink, mut cloned, mut copied, mut bytes) = (true, false, false, 0u64);
    let walked = walk(src, exclude, &mut |entry| {
        let target = dst.join(entry.path);
        let (uid, gid) = owner.unwrap_or((entry.meta.uid, entry.meta.gid));
        match entry.kind {
            Kind::Dir => {
                let mode = if entry.path.as_os_str().is_empty() { 0o750 } else { 0o755 };
                fs::DirBuilder::new().mode(mode).create(&target)?;
                std::os::unix::fs::lchown(&target, Some(uid), Some(gid))
            }
            Kind::File(file) => {
                let out = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
                    .mode(0o600)
                    .open(&target)?;
                let mut done = false;
                if reflink {
                    match rustix::fs::ioctl_ficlone(&out, &*file) {
                        Ok(()) => {
                            done = true;
                            cloned = true;
                        }
                        Err(e) if no_reflink(e) => reflink = false,
                        // Some files can't be shared where others can (EINVAL); copy just this one.
                        Err(_) => {}
                    }
                }
                if !done {
                    let mut out = &out;
                    io::copy(file, &mut out)?;
                    copied = true;
                }
                std::os::unix::fs::fchown(&out, Some(uid), Some(gid))?;
                out.set_permissions(fs::Permissions::from_mode(entry.meta.mode & 0o755))?;
                let at = Timespec { tv_sec: entry.meta.mtime, tv_nsec: entry.meta.mtime_nsec as _ };
                rustix::fs::futimens(&out, &Timestamps { last_access: at, last_modification: at })?;
                bytes += entry.meta.size;
                NoRoom::ask(room, bytes)
            }
            Kind::Link(to) => {
                std::os::unix::fs::symlink(to, &target)?;
                std::os::unix::fs::lchown(&target, Some(uid), Some(gid))
            }
        }
    })?;
    let method = if cloned && !copied { Method::Reflink } else { Method::Copy };
    Ok(Copied { walked, method })
}

/// Whether files in `dir` can share blocks: a probe of two small files. Blocking.
pub fn reflink_supported(dir: &Path) -> bool {
    let probe = dir.join(format!(".reflink-probe-{}", uuid::Uuid::new_v4()));
    let (a, b) = (probe.with_extension("a"), probe.with_extension("b"));
    let result = (|| -> io::Result<bool> {
        fs::write(&a, vec![0x5a; 64 * 1024])?;
        let src = File::open(&a)?;
        let dst = OpenOptions::new().write(true).create_new(true).mode(0o600).open(&b)?;
        Ok(rustix::fs::ioctl_ficlone(&dst, &src).is_ok())
    })();
    let _ = fs::remove_file(&a);
    let _ = fs::remove_file(&b);
    result.unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use super::*;

    fn tree() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        fs::create_dir_all(root.join("world/region")).unwrap();
        fs::write(root.join("server.properties"), "level-name=world\n").unwrap();
        fs::write(root.join("world/level.dat"), b"level").unwrap();
        fs::write(root.join("world/region/r.0.0.mca"), vec![7u8; 70_000]).unwrap();
        fs::create_dir_all(root.join("libraries/x")).unwrap();
        fs::write(root.join("libraries/x/lib.jar"), b"jar").unwrap();
        dir
    }

    fn me() -> (u32, u32) {
        (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw())
    }

    #[test]
    fn a_walk_visits_parents_first_in_order_and_leaves_out_what_is_excluded() {
        let dir = tree();
        let mut seen = Vec::new();
        let walked = walk(&dir.path().join("data"), &["libraries".into()], &mut |e| {
            seen.push(e.path.to_string_lossy().into_owned());
            Ok(())
        })
        .unwrap();
        assert_eq!(
            seen,
            ["", "server.properties", "world", "world/level.dat", "world/region", "world/region/r.0.0.mca"]
        );
        assert_eq!((walked.dirs, walked.files, walked.links), (3, 3, 0));
        assert_eq!(walked.bytes, 17 + 5 + 70_000);
    }

    #[test]
    fn a_directory_of_more_names_than_a_walk_takes_is_refused_before_any_is_visited() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        fs::create_dir_all(root.join("world")).unwrap();
        for n in 0..8 {
            fs::write(root.join(format!("world/r.{n}.mca")), b"").unwrap();
        }
        // The root, world/ and eight files: exactly the limit, which a walk still takes.
        let walked = walk_at_most(&root, &[], 10, &mut |_| Ok(())).unwrap();
        assert_eq!(walked.entries(), 10);
        fs::write(root.join("world/r.8.mca"), b"").unwrap();
        let mut visited = Vec::new();
        let refused = walk_at_most(&root, &[], 10, &mut |e| {
            visited.push(e.path.to_string_lossy().into_owned());
            Ok(())
        })
        .unwrap_err();
        assert_eq!(refused.to_string(), "more than 10 entries");
        assert_eq!(visited, ["", "world"], "refused as world/'s names were read, before any of them");
    }

    #[test]
    fn exclusion_is_by_top_level_name_only() {
        let dir = tree();
        let root = dir.path().join("data");
        fs::create_dir_all(root.join("world/libraries")).unwrap();
        let mut seen = Vec::new();
        walk(&root, &["libraries".into()], &mut |e| {
            seen.push(e.path.to_string_lossy().into_owned());
            Ok(())
        })
        .unwrap();
        assert!(seen.contains(&"world/libraries".to_owned()), "a nested name of the same spelling stays");
        assert!(!seen.iter().any(|p| p.starts_with("libraries")));
    }

    #[test]
    fn links_are_reported_never_followed_and_fifos_are_never_opened() {
        let dir = tree();
        let root = dir.path().join("data");
        let outside = dir.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret"), b"host secret").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("world/escape")).unwrap();
        std::os::unix::fs::symlink(outside.join("secret"), root.join("secret-link")).unwrap();
        rustix::fs::mknodat(
            rustix::fs::CWD,
            root.join("pipe").as_path(),
            FileType::Fifo,
            Mode::from_raw_mode(0o600),
            0,
        )
        .unwrap();
        let mut read = Vec::new();
        let walked = walk(&root, &[], &mut |e| {
            if let Kind::File(f) = e.kind {
                let mut s = Vec::new();
                f.read_to_end(&mut s).unwrap();
                read.push(s);
            }
            Ok(())
        })
        .unwrap();
        assert_eq!(walked.links, 2);
        assert_eq!(walked.skipped, 1, "the FIFO");
        assert!(!read.iter().any(|r| r == b"host secret"), "nothing outside the root was read");
    }

    #[test]
    fn a_directory_swapped_for_a_link_mid_walk_is_never_followed() {
        // What a plugin can do while the server runs: swap a directory it owns for a link to the
        // host, over and over, hoping a walk looks before the swap and opens after it.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let outside = dir.path().join("outside");
        fs::create_dir_all(root.join("world")).unwrap();
        fs::write(root.join("world/level.dat"), b"level").unwrap();
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("level.dat"), b"host secret").unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapper = {
            let (root, outside, stop) = (root.clone(), outside.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = fs::rename(root.join("world"), root.join("world.real"));
                    let _ = std::os::unix::fs::symlink(&outside, root.join("world"));
                    let _ = fs::remove_file(root.join("world"));
                    let _ = fs::rename(root.join("world.real"), root.join("world"));
                }
            })
        };
        for _ in 0..300 {
            let _ = walk(&root, &[], &mut |e| {
                if let Kind::File(f) = e.kind {
                    let mut body = Vec::new();
                    f.read_to_end(&mut body)?;
                    assert_ne!(body, b"host secret", "read through a swapped-in link");
                }
                Ok(())
            });
        }
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        swapper.join().unwrap();
    }

    /// The disk usage as it was measured before, by path: what an honest tree still comes to.
    fn usage_by_path(root: &Path) -> u64 {
        use std::os::unix::fs::MetadataExt;
        let mut total = fs::symlink_metadata(root).unwrap().blocks() * 512;
        let mut stack = vec![root.to_owned()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir).unwrap().map(Result::unwrap) {
                let meta = entry.metadata().unwrap();
                total += meta.blocks() * 512;
                if meta.is_dir() {
                    stack.push(entry.path());
                }
            }
        }
        total
    }

    #[test]
    fn disk_usage_counts_an_honest_tree_as_it_always_did() {
        let dir = tree();
        let root = dir.path().join("data");
        fs::write(root.join("world/empty"), b"").unwrap();
        fs::hard_link(root.join("world/level.dat"), root.join("level.dat.bak")).unwrap();
        std::os::unix::fs::symlink("world/level.dat", root.join("link")).unwrap();
        let fifo = root.join("pipe");
        rustix::fs::mknodat(rustix::fs::CWD, fifo.as_path(), FileType::Fifo, Mode::from_raw_mode(0o600), 0).unwrap();
        let used = disk_usage(&root).unwrap();
        assert!(used > 0);
        assert_eq!(used, usage_by_path(&root));
    }

    #[test]
    fn disk_usage_never_counts_or_enters_a_directory_swapped_for_a_link() {
        // The swap above, while the data is measured. A measure that opens directories by path
        // resolves `world` again for every directory beneath it, and follows the link whenever
        // the swap has it in place: what is behind it would count as the workload's.
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let outside = dir.path().join("outside");
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("host.img"), vec![1u8; 8 << 20]).unwrap();
        for n in 0..64 {
            fs::create_dir_all(root.join(format!("world/DIM{n}"))).unwrap();
            // The same names behind the link, each holding the one 8 MiB file.
            fs::create_dir_all(outside.join(format!("DIM{n}"))).unwrap();
            fs::hard_link(outside.join("host.img"), outside.join(format!("DIM{n}/host.img"))).unwrap();
        }
        let honest = disk_usage(&root).unwrap();
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let swapper = {
            let (root, outside, stop) = (root.clone(), outside.clone(), stop.clone());
            std::thread::spawn(move || {
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let _ = fs::rename(root.join("world"), root.join("world.real"));
                    let _ = std::os::unix::fs::symlink(&outside, root.join("world"));
                    let _ = fs::remove_file(root.join("world"));
                    let _ = fs::rename(root.join("world.real"), root.join("world"));
                }
            })
        };
        let most = (0..300).filter_map(|_| disk_usage(&root).ok()).max().unwrap_or(0);
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        swapper.join().unwrap();
        // Honest, give or take what the swap renames: never the 8 MiB behind the link, which
        // counts only once the measure enters it.
        assert!(most < honest + (1 << 20), "measured {most} bytes of a {honest}-byte tree");
    }

    #[test]
    fn a_copy_holds_the_same_bytes_and_tamed_modes() {
        let dir = tree();
        let root = dir.path().join("data");
        fs::set_permissions(root.join("server.properties"), fs::Permissions::from_mode(0o4777)).unwrap();
        std::os::unix::fs::symlink("world/level.dat", root.join("link")).unwrap();
        let dst = dir.path().join("copy");
        let copied = copy_tree(&root, &dst, Some(me()), &["libraries".into()], &mut |_| Ok(())).unwrap();
        assert_eq!(fs::read(dst.join("world/region/r.0.0.mca")).unwrap(), vec![7u8; 70_000]);
        assert_eq!(fs::read_link(dst.join("link")).unwrap(), Path::new("world/level.dat"));
        assert!(!dst.join("libraries").exists());
        let mode = fs::metadata(dst.join("server.properties")).unwrap().permissions().mode() & 0o7777;
        assert_eq!(mode, 0o755);
        let mtime = |p: &Path| fs::metadata(p).unwrap().modified().unwrap();
        assert_eq!(mtime(&dst.join("world/level.dat")), mtime(&root.join("world/level.dat")));
        assert_eq!(copied.walked.files, 3);
        // Whichever the filesystem allows; both give the same bytes.
        assert!(matches!(copied.method, Method::Reflink | Method::Copy));
    }

    #[test]
    fn a_copy_stops_when_the_disk_has_no_room() {
        let dir = tree();
        let mut told = Vec::new();
        let refused = copy_tree(&dir.path().join("data"), &dir.path().join("copy"), Some(me()), &[], &mut |copied| {
            told.push(copied);
            if copied > 20 { Err("the disk is under its floor".into()) } else { Ok(()) }
        })
        .unwrap_err();
        assert_eq!(NoRoom::of(&refused), Some("the disk is under its floor"), "{refused}");
        // Files in byte order: lib.jar, server.properties, then level.dat, which is one too many.
        assert_eq!(told, [3, 3 + 17, 3 + 17 + 5]);
        assert!(!dir.path().join("copy/world/region").exists(), "nothing after the refusal");
        assert_eq!(NoRoom::of(&io::Error::other("a disk's own failure")), None);
    }
}
