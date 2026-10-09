//! Writing so that a crash leaves the old or the new, never part of either.
//!
//! - `write_atomic`: the one way a file is replaced whole, for the records (`store.rs`), the
//!   node's identity and upgrade files, and doctor's daemon.json.
//! - `FilesystemSync`: a tree written file by file (a restore's unpack, a snapshot's copy) is on
//!   disk, with one `syncfs`, before anything calls it complete.
//!
//! Not for deciding what is complete: the restore's marker and a snapshot's `snapshot.json` are
//! `store.rs`'s, and they are written only after this has made the tree under them durable. Nor for
//! the spools, which nothing reads after a crash.

use std::fs::{self, File};
use std::io::{self, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// Which step of `write_atomic` failed, and on which path: each caller says it in its own words.
#[derive(Debug, thiserror::Error)]
#[error("{what} {path}: {source}")]
pub struct WriteError {
    /// "writing", "syncing" or "replacing".
    pub what: &'static str,
    pub path: PathBuf,
    pub source: io::Error,
}

/// How the name of `write_atomic`'s temp file for a file called `name` starts: `.<name>.`, with six
/// random letters and digits after it. A crash between making it and the rename leaves it behind.
pub fn temp_prefix(name: &str) -> String {
    format!(".{name}.")
}

/// Replaces `path` whole with `contents`, at `mode`: a crash leaves the old file or the new one,
/// never half of either, and the new one is on disk once this returns. It is written beside `path`
/// under a name of its own, made exclusively (two writes of one file at once never share one), given
/// its mode outright so that no umask narrows it, synced, renamed over `path`, and the directory
/// synced. On a failure the temp file goes and `path` is as it was.
pub fn write_atomic(path: &Path, contents: &[u8], mode: u32) -> Result<(), WriteError> {
    let dir = path.parent().filter(|dir| !dir.as_os_str().is_empty()).unwrap_or(Path::new("."));
    let failed = |what: &'static str, path: &Path| {
        let path = path.to_owned();
        move |source| WriteError { what, path, source }
    };
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let mut temp =
        tempfile::Builder::new().prefix(&temp_prefix(&name)).tempfile_in(dir).map_err(failed("writing", path))?;
    let staged = temp.path().to_owned();
    temp.write_all(contents).map_err(failed("writing", &staged))?;
    temp.as_file().set_permissions(fs::Permissions::from_mode(mode)).map_err(failed("writing", &staged))?;
    temp.as_file().sync_all().map_err(failed("syncing", &staged))?;
    temp.persist(path).map_err(|e| failed("replacing", path)(e.error))?;
    File::open(dir).and_then(|d| d.sync_all()).map_err(failed("syncing", dir))
}

/// The filesystem a tree is being written to, synced whole once it is written: one `syncfs(2)`
/// instead of an fsync per file, as PostgreSQL's `recovery_init_sync_method=syncfs` does. It
/// flushes whatever else is waiting on that filesystem too (the running servers' saves), so on a
/// busy disk it takes a while, once per tree.
///
/// Opened before the writing starts: Linux (5.8 and later) reports to `syncfs` a write-back error
/// that happened after the descriptor was opened, so one opened after the copy would miss an error
/// during it.
pub struct FilesystemSync(File);

impl FilesystemSync {
    /// Starts on the filesystem `dir` is on: a directory that exists, where the tree will go.
    pub fn begin(dir: &Path) -> io::Result<Self> {
        File::open(dir).map(Self)
    }

    /// Everything written to the filesystem since `begin` is on disk once this returns `Ok`;
    /// otherwise the error that kept it from it (EIO, or ENOSPC on a full disk).
    pub fn finish(self) -> io::Result<()> {
        rustix::fs::syncfs(&self.0).map_err(io::Error::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> =
            fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).collect();
        names.sort();
        names
    }

    #[test]
    fn a_file_is_replaced_whole_at_its_mode_and_leaves_nothing_beside_it() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("node.pem");
        write_atomic(&path, b"a certificate longer than the next", 0o600).unwrap();
        write_atomic(&path, b"shorter", 0o644).unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"shorter", "the old contents are gone, not overwritten in place");
        assert_eq!(fs::metadata(&path).unwrap().permissions().mode() & 0o777, 0o644);
        assert_eq!(names_in(dir.path()), ["node.pem"]);
    }

    #[test]
    fn a_failed_write_leaves_the_old_file_and_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        // A directory where the file goes: the rename fails, for root too.
        let path = dir.path().join("workload.json");
        fs::create_dir_all(path.join("in-the-way")).unwrap();
        let failed = write_atomic(&path, b"{}", 0o600).unwrap_err();
        assert_eq!((failed.what, failed.path.as_path()), ("replacing", path.as_path()), "{failed}");
        assert_eq!(names_in(dir.path()), ["workload.json"]);
        assert!(path.join("in-the-way").is_dir(), "what was there is as it was");
        // A directory that doesn't exist: nothing is made.
        let failed = write_atomic(&dir.path().join("missing/workload.json"), b"{}", 0o600).unwrap_err();
        assert_eq!(failed.what, "writing", "{failed}");
        assert_eq!(names_in(dir.path()), ["workload.json"]);
    }

    #[test]
    fn a_tree_is_synced_on_the_filesystem_it_went_to() {
        let dir = tempfile::tempdir().unwrap();
        let sync = FilesystemSync::begin(dir.path()).unwrap();
        std::fs::create_dir(dir.path().join("data")).unwrap();
        std::fs::write(dir.path().join("data/level.dat"), b"level").unwrap();
        sync.finish().unwrap();
        let missing = FilesystemSync::begin(&dir.path().join("missing")).err().unwrap();
        assert_eq!(missing.kind(), io::ErrorKind::NotFound);
    }
}
