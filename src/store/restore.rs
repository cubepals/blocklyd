//! A restore's new data, put together in `data.restoring/` beside the data, takes the data's place
//! in one swap, and what it replaced goes to the trash. A crash anywhere leaves the old data or the
//! new, and `Store::recover_restore` settles what it left when blocklyd next starts.

use std::fs::{self, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use super::{Store, StoreError, io_err};
use crate::ids::WorkloadId;

impl Store {
    /// Where a restore unpacks: beside the data, so replacing it is one rename.
    pub fn restoring_dir(&self, id: &WorkloadId) -> PathBuf {
        self.workload_dir(id).join("data.restoring")
    }

    /// Puts a restore's new data, complete in `data.restoring/` and on disk (`durable.rs`), in place
    /// of `data/`, and moves what it replaces into the trash, where it stays for the retention (a
    /// restore can be undone by hand until then). Returns where that went.
    ///
    /// The swap is one `renameat2(RENAME_EXCHANGE)`: a crash leaves the old data in place or the
    /// new, never neither. A filesystem that can't exchange gets two renames instead, and if a
    /// crash falls between them, `recover_restore` finishes the second.
    pub fn swap_in_restored(&self, id: &WorkloadId, now_unix: i64) -> Result<Option<PathBuf>, StoreError> {
        self.swap_with(id, now_unix, exchange)
    }

    fn swap_with(
        &self,
        id: &WorkloadId,
        now_unix: i64,
        exchange: fn(&Path, &Path) -> rustix::io::Result<()>,
    ) -> Result<Option<PathBuf>, StoreError> {
        let (data, restoring) = (self.data_dir(id), self.restoring_dir(id));
        mark_complete(&restoring)?;
        let previous = match exchange(&restoring, &data) {
            // What it replaced is now where the new data was.
            Ok(()) => Some(self.trash_replaced(id, &restoring, now_unix)?),
            // Nothing to replace.
            Err(rustix::io::Errno::NOENT) if fs::symlink_metadata(&data).is_err() => {
                fs::rename(&restoring, &data).map_err(io_err("restoring", &data))?;
                None
            }
            Err(rustix::io::Errno::INVAL | rustix::io::Errno::NOSYS | rustix::io::Errno::OPNOTSUPP) => {
                let previous = self.trash_replaced(id, &data, now_unix)?;
                fs::rename(&restoring, &data).map_err(io_err("restoring", &data))?;
                Some(previous)
            }
            Err(e) => return Err(StoreError::Io { what: "swapping in", path: data, source: e.into() }),
        };
        // A marker left behind is removed when blocklyd next starts.
        let _ = fs::remove_file(data.join(RESTORE_COMPLETE));
        let dir = self.workload_dir(id);
        File::open(&dir).and_then(|d| d.sync_all()).map_err(io_err("syncing", &dir))?;
        Ok(previous)
    }

    /// Settles what a restore left on disk when it didn't get to finish: called when blocklyd
    /// starts, and before each restore, so never while one is under way.
    pub fn recover_restore(&self, id: &WorkloadId, now_unix: i64) -> Result<Recovery, StoreError> {
        let (data, restoring) = (self.data_dir(id), self.restoring_dir(id));
        let leftover = fs::symlink_metadata(&restoring).is_ok();
        if is_marked(&data) {
            // The swap happened: what is at data.restoring/, if anything, is the data it replaced.
            let previous = if leftover { Some(self.trash_replaced(id, &restoring, now_unix)?) } else { None };
            let _ = fs::remove_file(data.join(RESTORE_COMPLETE));
            return Ok(Recovery::Finished { previous });
        }
        if !leftover {
            return Ok(Recovery::None);
        }
        if fs::symlink_metadata(&data).is_err() && is_marked(&restoring) {
            // Only the second of the fallback's two renames was left: the old data is in the
            // trash already, and the new data is complete.
            fs::rename(&restoring, &data).map_err(io_err("restoring", &data))?;
            let _ = fs::remove_file(data.join(RESTORE_COMPLETE));
            let dir = self.workload_dir(id);
            File::open(&dir).and_then(|d| d.sync_all()).map_err(io_err("syncing", &dir))?;
            return Ok(Recovery::Finished { previous: None });
        }
        // An unpack or copy that didn't finish, or finished and was never swapped in: nothing it
        // made has replaced the workload's data.
        fs::remove_dir_all(&restoring).map_err(io_err("removing", &restoring))?;
        Ok(Recovery::Discarded)
    }

    /// Moves data a restore replaced into the trash, under a name of its own: two restores in one
    /// second each keep what they replaced.
    fn trash_replaced(&self, id: &WorkloadId, from: &Path, now_unix: i64) -> Result<PathBuf, StoreError> {
        let trash = self.trash_dir();
        fs::DirBuilder::new().recursive(true).mode(0o700).create(&trash).map_err(io_err("creating", &trash))?;
        let mut to = trash.join(format!("{id}-replaced.{now_unix}"));
        for n in 1.. {
            if fs::symlink_metadata(&to).is_err() {
                break;
            }
            to = trash.join(format!("{id}-replaced-{n}.{now_unix}"));
        }
        fs::rename(from, &to).map_err(io_err("trashing", from))?;
        Ok(to)
    }
}

/// Left at the top of a restore's new data, `data.restoring/`, once it is complete. It moves with
/// the directory when the two are swapped, so after a crash it says which side of the swap
/// blocklyd stopped on (`Store::recover_restore`); it is removed once the new data is in place.
/// The name is blocklyd's: a world that holds it at its top loses that entry.
pub const RESTORE_COMPLETE: &str = ".blocklyd-restore-complete";

/// What `Store::recover_restore` found and did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Recovery {
    /// No restore was left unfinished.
    None,
    /// The restored data is in place now; the data it replaced went to the trash, at `previous`
    /// if that was left to do.
    Finished { previous: Option<PathBuf> },
    /// The new data never replaced anything, and is gone; the workload's data is as it was.
    Discarded,
}

fn exchange(a: &Path, b: &Path) -> rustix::io::Result<()> {
    rustix::fs::renameat_with(rustix::fs::CWD, a, rustix::fs::CWD, b, rustix::fs::RenameFlags::EXCHANGE)
}

/// Marks a restore's new data complete, durably, before it is swapped in. Whatever a world brought
/// under the marker's name goes first; the marker is made new, never through a link.
fn mark_complete(restoring: &Path) -> Result<(), StoreError> {
    let marker = restoring.join(RESTORE_COMPLETE);
    let _ = fs::remove_file(&marker);
    let _ = fs::remove_dir_all(&marker);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&marker)
        .and_then(|f| f.sync_all())
        .map_err(io_err("marking", &marker))?;
    File::open(restoring).and_then(|d| d.sync_all()).map_err(io_err("syncing", restoring))
}

/// Whether `dir` holds the marker this daemon made. A workload can make a file of that name in its
/// own data, but not one owned by the daemon.
fn is_marked(dir: &Path) -> bool {
    use std::os::unix::fs::MetadataExt;
    fs::symlink_metadata(dir.join(RESTORE_COMPLETE))
        .is_ok_and(|m| m.is_file() && m.uid() == rustix::process::geteuid().as_raw())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_restore_is_swapped_in_whole_by_either_route() {
        type Exchange = fn(&Path, &Path) -> rustix::io::Result<()>;
        let refuse: Exchange = |_, _| Err(rustix::io::Errno::INVAL);
        for (route, exchange) in [("exchange", super::exchange as Exchange), ("two renames", refuse)] {
            let tmp = tempfile::tempdir().unwrap();
            let store = Store::open(tmp.path()).unwrap();
            let id = WorkloadId::parse("w").unwrap();
            fs::create_dir_all(store.data_dir(&id)).unwrap();
            fs::write(store.data_dir(&id).join("level.dat"), b"old").unwrap();
            fs::create_dir_all(store.restoring_dir(&id)).unwrap();
            fs::write(store.restoring_dir(&id).join("level.dat"), b"new").unwrap();
            let previous = store.swap_with(&id, 1_000, exchange).unwrap().expect("the old data was kept");
            assert_eq!(fs::read(store.data_dir(&id).join("level.dat")).unwrap(), b"new", "{route}");
            assert_eq!(fs::read(previous.join("level.dat")).unwrap(), b"old", "{route}");
            assert!(!store.restoring_dir(&id).exists(), "{route}");
            assert!(!store.data_dir(&id).join(RESTORE_COMPLETE).exists(), "{route}: the marker is gone");
            assert_eq!(store.recover_restore(&id, 1_000).unwrap(), Recovery::None, "{route}: nothing left over");
            // A second restore in the same second keeps what it replaced too.
            fs::create_dir_all(store.restoring_dir(&id)).unwrap();
            let again = store.swap_with(&id, 1_000, exchange).unwrap().unwrap();
            assert_ne!(again, previous, "{route}");
            assert_eq!(fs::read(again.join("level.dat")).unwrap(), b"new", "{route}");
        }
    }
}
