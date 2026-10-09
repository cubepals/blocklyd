//! What a crash leaves in the state directory, cleared when blocklyd starts.
//!
//! An export's or a download's spool file, the copy of a snapshot that never got its
//! `snapshot.json`, and the temp file of a record write (`durable::write_atomic`) are removed by
//! what made them, however it ends. A crash, a kill or a power cut stops that, and they stay:
//! nothing reads them again, and they count against the disk's floor. This removes them, only
//! when blocklyd starts, under the state directory's lock and before anything is served, so none
//! of them can still be in use.
//!
//! Not for what a human should decide: a snapshot whose description can't be read, or a name
//! under `snapshots/` that isn't a snapshot's, is reported and left. Nor for `data/`,
//! `data.restoring/` (`Store::recover_restore` settles that), the trash or `identity/`.

use std::ffi::OsStr;
use std::fs;
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use super::{BadRecord, Store};
use crate::durable::temp_prefix;
use crate::ids::{SnapshotId, WorkloadId};
use crate::protocol::SnapshotView;

/// What `Store::clear_leftovers` removed, and what it left.
#[derive(Debug, Default)]
pub struct Leftovers {
    pub removed: Vec<PathBuf>,
    /// Left for a human: snapshots whose description doesn't read, names that aren't snapshots'.
    pub unreadable: Vec<BadRecord>,
    /// What couldn't be removed, and why.
    pub failed: Vec<BadRecord>,
}

impl Store {
    /// Clears what a crash left: everything in the spool, temp files beside `ports.json` and the
    /// records of `workloads`, and those workloads' snapshots that have no `snapshot.json`. Only
    /// while nothing else can be writing them: at startup, under the lock. Blocking.
    pub fn clear_leftovers(&self, workloads: &[WorkloadId]) -> Leftovers {
        let mut found = Leftovers::default();
        found.remove_in(&self.spool_dir(), |_| true);
        found.remove_in(&self.root, |entry| is_temp_of(entry, "ports.json"));
        for id in workloads {
            found.remove_in(&self.workload_dir(id), |entry| is_temp_of(entry, "workload.json"));
            self.clear_unfinished_snapshots(id, &mut found);
        }
        found
    }

    fn clear_unfinished_snapshots(&self, id: &WorkloadId, found: &mut Leftovers) {
        let Some(entries) = found.list(&self.snapshots_dir(id)) else { return };
        for entry in entries {
            let path = entry.path();
            let kept = |problem: String| BadRecord { path: entry.path(), problem: format!("{problem}; left alone") };
            let Some(snapshot) = entry.file_name().to_str().and_then(|name| SnapshotId::parse(name).ok()) else {
                found.unreadable.push(kept("not a snapshot id".into()));
                continue;
            };
            match fs::read(path.join("snapshot.json")) {
                Ok(bytes) => match serde_json::from_slice::<SnapshotView>(&bytes) {
                    Ok(view) if view.id == snapshot && view.workload == *id => {}
                    Ok(_) => found.unreadable.push(kept("snapshot.json describes another snapshot".into())),
                    Err(e) => found.unreadable.push(kept(format!("snapshot.json is unreadable: {e}"))),
                },
                // A copy that never got its description: the snapshot doesn't exist, and a retry
                // starts over.
                Err(e) if e.kind() == io::ErrorKind::NotFound && entry.file_type().is_ok_and(|t| t.is_dir()) => {
                    found.remove(path);
                }
                Err(e) => found.unreadable.push(kept(format!("snapshot.json: {e}"))),
            }
        }
    }
}

impl Leftovers {
    /// The entries of `dir`: none if it doesn't exist, and a failure if it can't be read.
    fn list(&mut self, dir: &Path) -> Option<Vec<fs::DirEntry>> {
        match fs::read_dir(dir) {
            Ok(entries) => Some(entries.filter_map(Result::ok).collect()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => {
                self.failed.push(BadRecord { path: dir.to_owned(), problem: e.to_string() });
                None
            }
        }
    }

    fn remove_in(&mut self, dir: &Path, chosen: impl Fn(&fs::DirEntry) -> bool) {
        for entry in self.list(dir).into_iter().flatten().filter(|entry| chosen(entry)) {
            self.remove(entry.path());
        }
    }

    /// Removes `path`, a directory with all it holds; a link is removed, never followed.
    fn remove(&mut self, path: PathBuf) {
        let removed = match fs::symlink_metadata(&path) {
            Ok(meta) if meta.is_dir() => fs::remove_dir_all(&path),
            _ => fs::remove_file(&path),
        };
        match removed {
            Ok(()) => self.removed.push(path),
            Err(e) => self.failed.push(BadRecord { path, problem: e.to_string() }),
        }
    }
}

/// A file `durable::write_atomic` was writing in place of `name` when it was cut short (or an older
/// blocklyd, as `.<name>.tmp`).
fn is_temp_of(entry: &fs::DirEntry, name: &str) -> bool {
    let prefix = temp_prefix(name);
    entry.file_name().as_bytes().starts_with(OsStr::new(&prefix).as_bytes())
        && entry.file_type().is_ok_and(|t| t.is_file())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::record;

    fn snapshot_view(id: &str, snapshot: &str) -> SnapshotView {
        SnapshotView {
            id: SnapshotId::parse(snapshot).unwrap(),
            workload: WorkloadId::parse(id).unwrap(),
            epoch: None,
            created_at: "2026-10-01T00:00:00Z".into(),
            size_bytes: 1,
            files: 1,
            method: crate::protocol::Method::Copy,
            quiesced: false,
            spec_digest: "sha256:x".into(),
            duration_ms: 1,
        }
    }

    #[test]
    fn what_a_crash_left_is_removed_and_what_needs_a_human_is_left() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let (w, other) = (WorkloadId::parse("w").unwrap(), WorkloadId::parse("other").unwrap());
        store.save(&record("w")).unwrap();
        store.save(&record("other")).unwrap();
        fs::create_dir_all(store.spool_dir()).unwrap();
        fs::write(store.spool_dir().join("w-0b6f.tar.gz"), b"half an archive").unwrap();
        fs::write(store.workload_dir(&w).join(".workload.json.AbC123"), b"{").unwrap();
        fs::write(store.workload_dir(&w).join(".workload.json.tmp"), b"{").unwrap();
        fs::write(tmp.path().join(".ports.json.XyZ789"), b"[").unwrap();
        let (unfinished, finished, corrupt) = (
            "0b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10",
            "1b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10",
            "2b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10",
        );
        for snapshot in [unfinished, finished, corrupt] {
            let data = store.prepare_snapshot(&w, &SnapshotId::parse(snapshot).unwrap()).unwrap();
            fs::create_dir_all(data.join("world")).unwrap();
        }
        store.finish_snapshot(&snapshot_view("w", finished)).unwrap();
        fs::write(store.snapshots_dir(&w).join(corrupt).join("snapshot.json"), b"{not json").unwrap();
        fs::create_dir_all(store.snapshots_dir(&w).join("not a snapshot")).unwrap();
        // A workload left out (its record didn't read) keeps everything.
        store.prepare_snapshot(&other, &SnapshotId::parse(unfinished).unwrap()).unwrap();

        let found = store.clear_leftovers(std::slice::from_ref(&w));
        let mut removed = found.removed.clone();
        removed.sort();
        let mut expected = vec![
            tmp.path().join(".ports.json.XyZ789"),
            store.spool_dir().join("w-0b6f.tar.gz"),
            store.workload_dir(&w).join(".workload.json.AbC123"),
            store.workload_dir(&w).join(".workload.json.tmp"),
            store.snapshots_dir(&w).join(unfinished),
        ];
        expected.sort();
        assert_eq!(removed, expected);
        assert!(found.failed.is_empty(), "{:?}", found.failed);
        let mut unreadable: Vec<_> = found.unreadable.iter().map(|b| b.path.clone()).collect();
        unreadable.sort();
        assert_eq!(unreadable, [store.snapshots_dir(&w).join(corrupt), store.snapshots_dir(&w).join("not a snapshot")]);
        for kept in [corrupt, "not a snapshot"] {
            assert!(store.snapshots_dir(&w).join(kept).is_dir(), "{kept} is left alone");
        }
        assert_eq!(store.snapshots(&w), vec![snapshot_view("w", finished)], "the finished one stays");
        assert!(store.snapshots_dir(&other).join(unfinished).is_dir());
        assert!(store.workload_dir(&w).join("workload.json").is_file());
        assert!(store.spool_dir().is_dir(), "the spool is emptied, not removed");
        let again = store.clear_leftovers(std::slice::from_ref(&w));
        assert!(again.removed.is_empty() && again.unreadable.len() == 2, "a second pass finds the same: {again:?}");
    }
}
