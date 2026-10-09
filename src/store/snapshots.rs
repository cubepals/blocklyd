//! A workload's local snapshots on disk: `snapshots/<snapshot>/`, a copy of its data and, written
//! last, `snapshot.json`. Making the copy is `tree::copy_tree`, and deciding when is the manager's.

use std::fs;
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;

use super::{Store, StoreError, io_err};
use crate::durable;
use crate::ids::{SnapshotId, WorkloadId};
use crate::protocol::SnapshotView;

/// Local snapshots, beside the data they copy: on the same filesystem, so a copy can share its
/// blocks, and out of the workload's reach, since only `data/` is mounted.
impl Store {
    pub(crate) fn snapshots_dir(&self, id: &WorkloadId) -> PathBuf {
        self.workload_dir(id).join("snapshots")
    }

    /// `snapshots/<snapshot>`. A `SnapshotId` is one plain path component, like a `WorkloadId`.
    pub fn snapshot_dir(&self, id: &WorkloadId, snapshot: &SnapshotId) -> PathBuf {
        self.snapshots_dir(id).join(snapshot.as_str())
    }

    /// A finished snapshot's description, if it exists.
    pub(crate) fn snapshot(&self, id: &WorkloadId, snapshot: &SnapshotId) -> Option<SnapshotView> {
        let bytes = fs::read(self.snapshot_dir(id, snapshot).join("snapshot.json")).ok()?;
        serde_json::from_slice::<SnapshotView>(&bytes).ok().filter(|v| v.id == *snapshot && v.workload == *id)
    }

    /// The workload's finished snapshots, oldest first.
    pub(crate) fn snapshots(&self, id: &WorkloadId) -> Vec<SnapshotView> {
        let Ok(entries) = fs::read_dir(self.snapshots_dir(id)) else { return Vec::new() };
        let mut found: Vec<SnapshotView> = entries
            .filter_map(Result::ok)
            .filter_map(|e| SnapshotId::parse(&e.file_name().to_string_lossy()).ok())
            .filter_map(|snapshot| self.snapshot(id, &snapshot))
            .collect();
        found.sort_by(|a, b| a.created_at.cmp(&b.created_at).then_with(|| a.id.cmp(&b.id)));
        found
    }

    /// Makes room for a new snapshot: the parent directories, and nothing left of an unfinished
    /// one under the same id. Returns where its copy goes.
    pub fn prepare_snapshot(&self, id: &WorkloadId, snapshot: &SnapshotId) -> Result<PathBuf, StoreError> {
        let dir = self.snapshot_dir(id, snapshot);
        let parent = self.snapshots_dir(id);
        fs::DirBuilder::new().recursive(true).mode(0o700).create(&parent).map_err(io_err("creating", &parent))?;
        match fs::remove_dir_all(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(StoreError::Io { what: "clearing", path: dir, source: e }),
        }
        fs::DirBuilder::new().mode(0o700).create(&dir).map_err(io_err("creating", &dir))?;
        Ok(dir.join("data"))
    }

    /// Writes a snapshot's description, last, once its copy is on disk: from here on it exists.
    pub(crate) fn finish_snapshot(&self, view: &SnapshotView) -> Result<(), StoreError> {
        let path = self.snapshot_dir(&view.workload, &view.id).join("snapshot.json");
        let bytes = serde_json::to_vec_pretty(view).expect("serializes");
        Ok(durable::write_atomic(&path, &bytes, 0o600)?)
    }

    /// Removes a snapshot, finished or not. Returns whether there was one. Blocking.
    pub(crate) fn remove_snapshot(&self, id: &WorkloadId, snapshot: &SnapshotId) -> Result<bool, StoreError> {
        let dir = self.snapshot_dir(id, snapshot);
        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StoreError::Io { what: "removing", path: dir, source: e }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_snapshot_exists_once_its_description_is_written() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let id = WorkloadId::parse("w").unwrap();
        let snap = SnapshotId::parse("0b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10").unwrap();
        let data = store.prepare_snapshot(&id, &snap).unwrap();
        fs::create_dir(&data).unwrap();
        assert!(store.snapshot(&id, &snap).is_none(), "unfinished");
        assert!(store.snapshots(&id).is_empty());
        let view = SnapshotView {
            id: snap.clone(),
            workload: id.clone(),
            epoch: Some(3),
            created_at: "2026-10-01T00:00:00Z".into(),
            size_bytes: 1,
            files: 1,
            method: crate::protocol::Method::Copy,
            quiesced: false,
            spec_digest: "sha256:x".into(),
            duration_ms: 1,
        };
        store.finish_snapshot(&view).unwrap();
        assert_eq!(store.snapshot(&id, &snap), Some(view.clone()));
        assert_eq!(store.snapshots(&id), vec![view]);
        // Preparing the same id again starts over.
        store.prepare_snapshot(&id, &snap).unwrap();
        assert!(store.snapshot(&id, &snap).is_none());
        assert!(store.remove_snapshot(&id, &snap).unwrap());
        assert!(!store.remove_snapshot(&id, &snap).unwrap());
    }
}
