//! World archives: a gzip tarball of a workload's data directory. It is the format Blockly's
//! backups, downloads and uploads already use: paths relative to the data root, with
//! `server.properties` at the top. So an archive blocklyd writes is one Blockly can already read,
//! and the other way round.
//!
//! Export writes the archive to a spool file first, hashing as it goes: a presigned PUT needs its
//! length up front, and the sha256 is what a later restore checks against. It reads the data
//! through `tree::walk`, so a workload rearranging its files meanwhile can't make it read
//! anything else. Compression is gzip's fastest level: region files are compressed already, so
//! a slower level costs CPU for little.
//!
//! Restore is the dangerous direction. The archive may be a player's upload, and blocklyd unpacks
//! it as root. So it keeps only regular files and directories. It refuses the whole archive if
//! any path is absolute or climbs out with `..`. It never follows a link. It strips setuid,
//! setgid and sticky bits and group/other write, and gives every file to the workload's user. It
//! unpacks beside the data and swaps by rename, so a bad archive never costs the current world.

use std::cell::Cell;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use flate2::Compression;
use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use sha2::{Digest, Sha256};

use crate::tree::{Kind, NoRoom};

/// Unpacked bytes an archive may expand to. A world is a few GB; this stops a gzip bomb.
pub const MAX_UNPACKED_BYTES: u64 = 256 * 1024 * 1024 * 1024;
/// Entries an archive may hold. Worlds hold thousands; this bounds the work a bad one causes.
pub const MAX_ENTRIES: u64 = crate::tree::MAX_ENTRIES;
/// What a restore lets tar read of a member besides the data it unpacks: its long name, long link
/// and PAX records, or the data of an entry it skips. Go's archive/tar and libarchive stop there
/// too; no ordinary writer comes near it.
const MAX_METADATA_BYTES: u64 = 1024 * 1024;
/// The longest one download's body may take, however steadily it comes: the control plane gives
/// up on a transfer after 3 hours (its TRANSFER_MS), and nobody waits for it after that.
pub const DOWNLOAD_LIMIT: Duration = Duration::from_secs(3 * 3600);

static DOWNLOAD_IDLE_MS: AtomicU64 = AtomicU64::new(120_000);

/// How long a transfer may go without receiving anything before it counts as stalled: a store
/// that hung mid-body, or a path that drops packets without a reset, never ends a body by itself.
/// Whatever arrives starts it over, so a slow but live download of any size never trips it.
pub fn download_idle() -> Duration {
    Duration::from_millis(DOWNLOAD_IDLE_MS.load(Ordering::Relaxed))
}

/// Shortens `download_idle` for the whole process, so a test sees a stall end without waiting two
/// minutes. Nothing else calls it.
#[doc(hidden)]
pub fn set_download_idle(idle: Duration) {
    DOWNLOAD_IDLE_MS.store(idle.as_millis() as u64, Ordering::Relaxed);
}

/// The parts an archive of `size` bytes fills, `part_size` each but the last: one at least, so
/// an empty archive is still an object.
pub fn parts_needed(size: u64, part_size: u64) -> u64 {
    size.div_ceil(part_size.max(1)).max(1)
}

/// Counts and hashes what passes through.
pub struct Hashing<W> {
    inner: W,
    hasher: Sha256,
    bytes: u64,
}

impl<W> Hashing<W> {
    pub fn new(inner: W) -> Self {
        Self { inner, hasher: Sha256::new(), bytes: 0 }
    }
    pub fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
        self.bytes += data.len() as u64;
    }
    pub fn bytes(&self) -> u64 {
        self.bytes
    }
    pub fn finish(self) -> (W, String, u64) {
        (self.inner, hex::encode(self.hasher.finalize()), self.bytes)
    }
}

impl<W: Write> Write for Hashing<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.update(&buf[..n]);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packed {
    pub size_bytes: u64,
    pub sha256: String,
    /// Directories, files and links in the archive.
    pub entries: u64,
}

/// Writes `root` as a .tar.gz to `out`, leaving out the top-level names in `exclude`. Paths are
/// `./`-relative, as `tar -C <root> .` writes them. After each file `room` is told the bytes read
/// so far, more than the archive grew by, and an `Err` from it stops the pack (`tree::NoRoom`).
/// Blocking.
pub fn pack(
    root: &Path,
    out: &Path,
    exclude: &[String],
    room: &mut dyn FnMut(u64) -> Result<(), String>,
) -> io::Result<Packed> {
    let file = OpenOptions::new().write(true).create_new(true).mode(0o600).open(out)?;
    let gz = GzEncoder::new(Hashing::new(io::BufWriter::with_capacity(256 * 1024, file)), Compression::fast());
    let mut builder = tar::Builder::new(gz);
    let mut read = 0;
    let walked = crate::tree::walk(root, exclude, &mut |entry| {
        let name = Path::new(".").join(entry.path);
        let mut header = tar::Header::new_gnu();
        header.set_mode(entry.meta.mode & 0o7777);
        header.set_uid(u64::from(entry.meta.uid));
        header.set_gid(u64::from(entry.meta.gid));
        header.set_mtime(entry.meta.mtime.max(0) as u64);
        match entry.kind {
            Kind::Dir => {
                header.set_entry_type(tar::EntryType::Directory);
                header.set_size(0);
                builder.append_data(&mut header, name, io::empty())
            }
            Kind::File(file) => {
                header.set_entry_type(tar::EntryType::Regular);
                header.set_size(entry.meta.size);
                builder.append_data(&mut header, name, exactly(file, entry.meta.size))?;
                read += entry.meta.size;
                NoRoom::ask(room, read)
            }
            Kind::Link(target) => {
                header.set_entry_type(tar::EntryType::Symlink);
                header.set_size(0);
                builder.append_link(&mut header, name, target)
            }
        }
    })?;
    let gz = builder.into_inner()?;
    let hashing = gz.finish()?;
    let (writer, sha256, size_bytes) = hashing.finish();
    let file = writer.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()?;
    Ok(Packed { size_bytes, sha256, entries: walked.entries() })
}

/// Exactly `size` bytes of a file: cut there if it grew since it was opened, padded with zeros if
/// it shrank. An archive states each file's size before its bytes, and any other count breaks it.
fn exactly(file: &mut File, size: u64) -> impl Read {
    file.take(size).chain(io::repeat(0)).take(size)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unpacked {
    pub entries: u64,
    /// Links, devices and the like: never unpacked.
    pub skipped: u64,
    pub bytes: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum UnpackError {
    #[error("the archive is unsafe: {0}")]
    Unsafe(String),
    #[error("the archive is too large: {0}")]
    TooLarge(String),
    #[error("the archive is damaged or not a gzip tarball: {0}")]
    Damaged(String),
    /// The caller said the disk can't take more.
    #[error("{0}")]
    NoRoom(String),
    #[error("{0}")]
    Io(#[from] io::Error),
}

/// A path as it should land under the destination: relative, with no `..`, and no `.`. None for
/// the archive's root entry.
fn clean(raw: &Path) -> Result<Option<PathBuf>, UnpackError> {
    let mut out = PathBuf::new();
    for component in raw.components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::CurDir => {}
            Component::ParentDir => return Err(UnpackError::Unsafe(format!("{} climbs out", raw.display()))),
            Component::RootDir | Component::Prefix(_) => {
                return Err(UnpackError::Unsafe(format!("{} is absolute", raw.display())));
            }
        }
    }
    Ok((!out.as_os_str().is_empty()).then_some(out))
}

/// Unpacks a .tar.gz into `dest`, which must not exist yet, and gives everything to `owner`.
/// After each write `room` is told the bytes written so far, and an `Err` from it stops the
/// unpack: the caller keeps the disk above its floor this way. Blocking.
pub fn unpack(
    archive: &Path,
    dest: &Path,
    owner: (u32, u32),
    room: &mut dyn FnMut(u64) -> Result<(), String>,
) -> Result<Unpacked, UnpackError> {
    fs::DirBuilder::new().mode(0o750).create(dest)?;
    // Every gzip member, as gzip -d reads them: an archive written in several (BGZF, or files
    // concatenated) is one tarball, which a reader of the first member alone would cut short.
    let gzip = MultiGzDecoder::new(BufReader::new(File::open(archive)?));
    let budget = Rc::new(Budget::default());
    budget.allow(MAX_METADATA_BYTES);
    let mut reader = tar::Archive::new(Budgeted { inner: gzip, budget: budget.clone() });
    let mut done = Unpacked { entries: 0, skipped: 0, bytes: 0 };
    let (mut written, mut buffer) = (0u64, vec![0u8; 256 * 1024]);
    let damaged = |e: io::Error| UnpackError::Damaged(e.to_string());
    for entry in reader.entries().map_err(damaged)? {
        let mut entry = entry.map_err(damaged)?;
        // What tar may read before it hands over the next member: this one's data if it is one
        // unpacked below (bounded by MAX_UNPACKED_BYTES there), and the next one's headers.
        let unpacked = matches!(
            entry.header().entry_type(),
            tar::EntryType::Regular | tar::EntryType::Continuous | tar::EntryType::GNUSparse
        );
        let data = if unpacked { entry.size().saturating_add(511) & !511 } else { 0 };
        budget.allow(data.saturating_add(MAX_METADATA_BYTES));
        done.entries += 1;
        if done.entries > MAX_ENTRIES {
            return Err(UnpackError::TooLarge(format!("more than {MAX_ENTRIES} entries")));
        }
        let raw = entry.path().map_err(damaged)?.into_owned();
        let Some(relative) = clean(&raw)? else { continue };
        let target = dest.join(&relative);
        let mode = entry.header().mode().unwrap_or(0o644);
        match entry.header().entry_type() {
            tar::EntryType::Directory => {
                fs::DirBuilder::new().recursive(true).mode(0o755).create(&target)?;
            }
            tar::EntryType::Regular | tar::EntryType::Continuous | tar::EntryType::GNUSparse => {
                // Saturating: a PAX record can state any size, u64::MAX among them.
                done.bytes = done.bytes.saturating_add(entry.size());
                if done.bytes > MAX_UNPACKED_BYTES {
                    return Err(UnpackError::TooLarge(format!("more than {MAX_UNPACKED_BYTES} bytes unpacked")));
                }
                if let Some(parent) = target.parent() {
                    fs::DirBuilder::new().recursive(true).mode(0o755).create(parent)?;
                }
                // Only directories this function made lie above it, and it makes no links, so the
                // path can't lead outside. O_NOFOLLOW guards the last step anyway.
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits() as i32)
                    .mode(0o644)
                    .open(&target)?;
                // Not io::copy: a read that fails is a damaged archive, but a write that fails is
                // this disk's trouble, and `room` is asked as the bytes land.
                loop {
                    let n = match entry.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => n,
                        Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                        Err(e) => return Err(damaged(e)),
                    };
                    file.write_all(&buffer[..n])?;
                    written += n as u64;
                    room(written).map_err(UnpackError::NoRoom)?;
                }
                // Keep the execute bits (scripts), drop setuid/setgid/sticky and others' write.
                fs::set_permissions(&target, fs::Permissions::from_mode(mode & 0o755))?;
            }
            _ => done.skipped += 1,
        }
    }
    chown_tree(dest, owner)?;
    Ok(done)
}

/// How far into a restore's archive tar may read, and how far it has. tar reads a member's long
/// name, long link and PAX records whole into memory before it hands the member over (tar 0.4.46,
/// `EntryFields::read_all`), and skips the data of one unpack doesn't take by reading it: without a
/// bound, an archive whose metadata decompresses to gigabytes makes root blocklyd hold or read them
/// all. tar-rs PR #481 (<https://github.com/alexcrichton/tar-rs/pull/481>) adds
/// `Archive::set_max_metadata_size`: once tar ships it, that takes the headers' part (at
/// `MAX_METADATA_BYTES`), and this keeps bounding the data skipped.
#[derive(Default)]
struct Budget {
    read: Cell<u64>,
    limit: Cell<u64>,
}

impl Budget {
    /// Lets tar read `more` bytes past where it is now.
    fn allow(&self, more: u64) {
        self.limit.set(self.read.get().saturating_add(more));
    }
}

/// The decompressed archive as tar reads it, held to its `Budget`. A read past it fails, and the
/// archive is damaged; it never looks like the archive's end, which tar would take for a whole one.
struct Budgeted<R> {
    inner: R,
    budget: Rc<Budget>,
}

impl<R: Read> Read for Budgeted<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let left = self.budget.limit.get().saturating_sub(self.budget.read.get());
        if left == 0 && !buf.is_empty() {
            return Err(io::Error::other(format!(
                "a member's headers, or the data of an entry that isn't unpacked, run past {MAX_METADATA_BYTES} bytes"
            )));
        }
        let most = buf.len().min(usize::try_from(left).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..most])?;
        self.budget.read.set(self.budget.read.get() + n as u64);
        Ok(n)
    }
}

fn chown_tree(dir: &Path, owner: (u32, u32)) -> io::Result<()> {
    std::os::unix::fs::lchown(dir, Some(owner.0), Some(owner.1))?;
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if entry.file_type()?.is_dir() {
            chown_tree(&path, owner)?;
        } else {
            std::os::unix::fs::lchown(&path, Some(owner.0), Some(owner.1))?;
        }
    }
    Ok(())
}

/// Bytes a directory holds, for disk admission before an export. Blocking.
pub fn tree_bytes(dir: &Path) -> u64 {
    crate::store::disk_usage(dir).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive_of(entries: &[(&str, tar::EntryType, &[u8])]) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.tar.gz");
        let gz = GzEncoder::new(File::create(&path).unwrap(), Compression::fast());
        let mut builder = tar::Builder::new(gz);
        for (name, kind, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(*kind);
            header.set_size(data.len() as u64);
            header.set_mode(0o4777);
            if *kind == tar::EntryType::Symlink {
                header.set_link_name("/etc/passwd").unwrap();
            }
            // Written raw, so names a well-behaved writer would refuse can be tested.
            let bytes = name.as_bytes();
            header.as_old_mut().name[..bytes.len()].copy_from_slice(bytes);
            header.set_cksum();
            builder.append(&header, *data).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap();
        (dir, path)
    }

    fn me() -> (u32, u32) {
        (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw())
    }

    #[test]
    fn a_world_round_trips_with_its_hash() {
        let src = tempfile::tempdir().unwrap();
        fs::write(src.path().join("server.properties"), "level-name=world\n").unwrap();
        fs::create_dir_all(src.path().join("world/region")).unwrap();
        fs::write(src.path().join("world/level.dat"), b"level").unwrap();
        fs::write(src.path().join("world/region/r.0.0.mca"), vec![7u8; 100_000]).unwrap();
        let out = tempfile::tempdir().unwrap();
        let archive = out.path().join("world.tar.gz");
        let packed = pack(src.path(), &archive, &[], &mut |_| Ok(())).unwrap();
        assert_eq!(packed.size_bytes, fs::metadata(&archive).unwrap().len());
        assert_eq!(packed.sha256, hex::encode(Sha256::digest(fs::read(&archive).unwrap())));

        let dest = out.path().join("restored");
        let done = unpack(&archive, &dest, me(), &mut |_| Ok(())).unwrap();
        assert_eq!(fs::read(dest.join("world/level.dat")).unwrap(), b"level");
        assert_eq!(fs::read(dest.join("world/region/r.0.0.mca")).unwrap().len(), 100_000);
        assert_eq!(fs::read_to_string(dest.join("server.properties")).unwrap(), "level-name=world\n");
        assert!(done.bytes >= 100_000);
    }

    #[test]
    fn a_file_that_changes_while_read_keeps_its_stated_size() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("latest.log");
        fs::write(&path, b"0123456789").unwrap();
        let mut file = File::open(&path).unwrap();
        let mut grown = Vec::new();
        exactly(&mut file, 4).read_to_end(&mut grown).unwrap();
        assert_eq!(grown, b"0123", "cut where the header said");
        let mut file = File::open(&path).unwrap();
        let mut shrunk = Vec::new();
        exactly(&mut file, 14).read_to_end(&mut shrunk).unwrap();
        assert_eq!(shrunk, b"0123456789\0\0\0\0", "padded to it");
    }

    #[test]
    fn a_pack_stops_when_the_disk_has_no_room() {
        let src = tempfile::tempdir().unwrap();
        fs::write(src.path().join("level.dat"), b"level").unwrap();
        fs::write(src.path().join("r.0.0.mca"), vec![7u8; 100_000]).unwrap();
        fs::write(src.path().join("server.properties"), b"level-name=world").unwrap();
        let out = tempfile::tempdir().unwrap();
        let mut told = Vec::new();
        let refused = pack(src.path(), &out.path().join("w.tar.gz"), &[], &mut |read| {
            told.push(read);
            if read > 50_000 { Err("the disk is under its floor".into()) } else { Ok(()) }
        })
        .unwrap_err();
        assert_eq!(NoRoom::of(&refused), Some("the disk is under its floor"), "{refused}");
        assert_eq!(told, [5, 100_005], "told after each file, and stopped at the refusal");
    }

    #[test]
    fn climbing_out_refuses_the_whole_archive() {
        for name in ["../escape", "a/../../escape", "/etc/cron.d/x"] {
            let (dir, archive) =
                archive_of(&[("ok.txt", tar::EntryType::Regular, b"fine"), (name, tar::EntryType::Regular, b"x")]);
            let err = unpack(&archive, &dir.path().join("out"), me(), &mut |_| Ok(())).unwrap_err();
            assert!(matches!(err, UnpackError::Unsafe(_)), "{name}: {err}");
            assert!(!dir.path().join("escape").exists());
        }
    }

    #[test]
    fn links_and_devices_are_skipped_and_modes_are_tamed() {
        let (dir, archive) = archive_of(&[
            ("link", tar::EntryType::Symlink, b""),
            ("script.sh", tar::EntryType::Regular, b"#!/bin/sh\n"),
        ]);
        let dest = dir.path().join("out");
        let done = unpack(&archive, &dest, me(), &mut |_| Ok(())).unwrap();
        assert_eq!(done.skipped, 1);
        assert!(fs::symlink_metadata(dest.join("link")).is_err(), "no link was made");
        let mode = fs::metadata(dest.join("script.sh")).unwrap().permissions().mode() & 0o7777;
        assert_eq!(mode, 0o755, "setuid and others' write are gone, execute kept");
    }

    #[test]
    fn an_archive_in_several_gzip_members_unpacks_whole() {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data) in [("level.dat", &b"level"[..]), ("world/r.0.0.mca", &[7u8; 3000][..])] {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            builder.append_data(&mut header, name, data).unwrap();
        }
        let tar = builder.into_inner().unwrap();
        // Split in the first file's padding, and exactly where the second file's header starts.
        for at in [700, 1024] {
            let dir = tempfile::tempdir().unwrap();
            let mut members = Vec::new();
            for part in [&tar[..at], &tar[at..]] {
                let mut gz = GzEncoder::new(Vec::new(), Compression::fast());
                gz.write_all(part).unwrap();
                members.extend(gz.finish().unwrap());
            }
            let archive = dir.path().join("a.tar.gz");
            fs::write(&archive, members).unwrap();
            let dest = dir.path().join("out");
            let done = unpack(&archive, &dest, me(), &mut |_| Ok(())).unwrap();
            assert_eq!(done.entries, 2, "split at {at}");
            assert_eq!(fs::read(dest.join("level.dat")).unwrap(), b"level", "split at {at}");
            assert_eq!(fs::read(dest.join("world/r.0.0.mca")).unwrap(), [7u8; 3000], "split at {at}");
        }
    }

    /// One PAX record: its length, which counts itself, then `key=value` and a newline.
    fn pax_record(key: &str, value: &str) -> Vec<u8> {
        let body = format!(" {key}={value}\n");
        let mut length = body.len() + 1;
        while length.to_string().len() + body.len() > length {
            length += 1;
        }
        format!("{length}{body}").into_bytes()
    }

    #[test]
    fn metadata_larger_than_a_mebibyte_is_damaged_before_it_is_held() {
        let long = vec![b'a'; 2 << 20];
        let pax = pax_record("path", &"a".repeat((1 << 20) + 1));
        let link = vec![0u8; (1 << 20) + 1024];
        let cases: [(&str, tar::EntryType, &[u8]); 3] = [
            ("././@LongLink", tar::EntryType::GNULongName, &long),
            ("././@PaxHeader", tar::EntryType::XHeader, &pax),
            // Data on an entry unpack skips is read through all the same, to get past it.
            ("link", tar::EntryType::Symlink, &link),
        ];
        for (name, kind, data) in cases {
            let (dir, archive) = archive_of(&[(name, kind, data), ("level.dat", tar::EntryType::Regular, b"x")]);
            let err = unpack(&archive, &dir.path().join("out"), me(), &mut |_| Ok(())).unwrap_err();
            assert!(matches!(&err, UnpackError::Damaged(m) if m.contains("run past 1048576 bytes")), "{kind:?}: {err}");
        }
    }

    #[test]
    fn a_pax_size_near_the_largest_number_is_too_large_not_an_overflow() {
        // As large as tar takes: past its end, the next member's place would overflow.
        let huge = pax_record("size", &(u64::MAX - (1 << 20)).to_string());
        let (dir, archive) = archive_of(&[
            ("level.dat", tar::EntryType::Regular, &[1u8; 5000]),
            ("././@PaxHeader", tar::EntryType::XHeader, &huge),
            ("r.0.0.mca", tar::EntryType::Regular, b""),
        ]);
        let err = unpack(&archive, &dir.path().join("out"), me(), &mut |_| Ok(())).unwrap_err();
        assert!(matches!(err, UnpackError::TooLarge(_)), "{err}");
    }

    #[test]
    fn long_names_and_pax_records_of_ordinary_size_still_unpack() {
        let src = tempfile::tempdir().unwrap();
        // Over tar's 100-byte name field, so it goes as a GNU long name.
        let deep = format!("world/{}/{}", "d".repeat(150), "f".repeat(140));
        fs::create_dir_all(src.path().join(&deep).parent().unwrap()).unwrap();
        fs::write(src.path().join(&deep), b"far down").unwrap();
        let out = tempfile::tempdir().unwrap();
        let archive = out.path().join("world.tar.gz");
        pack(src.path(), &archive, &[], &mut |_| Ok(())).unwrap();
        let dest = out.path().join("restored");
        unpack(&archive, &dest, me(), &mut |_| Ok(())).unwrap();
        assert_eq!(fs::read(dest.join(&deep)).unwrap(), b"far down");

        let pax = pax_record("path", "from/pax.dat");
        let (dir, archive) = archive_of(&[
            ("././@PaxHeader", tar::EntryType::XHeader, &pax),
            ("ignored", tar::EntryType::Regular, b"pax"),
        ]);
        unpack(&archive, &dir.path().join("out"), me(), &mut |_| Ok(())).unwrap();
        assert_eq!(fs::read(dir.path().join("out/from/pax.dat")).unwrap(), b"pax");
    }

    #[test]
    fn garbage_is_damaged_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.tar.gz");
        fs::write(&path, b"this is not gzip").unwrap();
        assert!(matches!(unpack(&path, &dir.path().join("out"), me(), &mut |_| Ok(())), Err(UnpackError::Damaged(_))));
    }

    #[test]
    fn an_unpack_stops_when_the_disk_has_no_room() {
        let big = vec![7u8; 600_000];
        let (dir, archive) = archive_of(&[
            ("world/r.0.0.mca", tar::EntryType::Regular, &big),
            ("level.dat", tar::EntryType::Regular, b"x"),
        ]);
        let mut told = Vec::new();
        let err = unpack(&archive, &dir.path().join("out"), me(), &mut |written| {
            told.push(written);
            if written > 300_000 { Err("the disk is under its floor".into()) } else { Ok(()) }
        })
        .unwrap_err();
        assert!(matches!(&err, UnpackError::NoRoom(m) if m == "the disk is under its floor"), "{err}");
        assert!(told.len() > 1 && told.windows(2).all(|w| w[0] < w[1]), "told as the bytes land: {told:?}");
        assert!(!dir.path().join("out/level.dat").exists(), "nothing after the refusal");
    }
}
