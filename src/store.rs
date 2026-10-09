//! What blocklyd keeps on disk, and the host's filesystem layout.
//!
//! ```text
//! <state_dir>/                       0700 root   (default /var/lib/blocklyd, FHS: /var/lib/<pkg>)
//!   blocklyd.lock                                    flock: one blocklyd per state dir
//!   ports.json                       0600 root    released host ports still in quarantine
//!   workloads/<id>/                  0700 root
//!     workload.json                  0600 root    blocklyd's record (no secret values)
//!     data/                          0750 uid:gid the workload's persistent data, bind-mounted
//!     snapshots/<snapshot>/                       local snapshots (tree.rs), never mounted
//!       snapshot.json                             written last: a snapshot without it is unfinished
//!       data/                                     the copy, sharing blocks with data/ where it can
//!   trash/<id>.<unix-ts>/                         deleted workloads, purged after retention
//!   trash/<id>-replaced[-n].<unix-ts>/            data a restore replaced, purged likewise
//!   workloads/<id>/data.restoring/                a restore being put together, swapped in once
//!                                                 complete (RESTORE_COMPLETE)
//!   spool/                                        archives on their way out or in, emptied at start
//!   identity/                        0700 root    fleet mode: the node's key and certificates
//! ```
//!
//! The record is local fact, not business state: a spec minus secrets, the host ports this host
//! handed out, the generation and when things happened. It exists so a workload whose container
//! is gone (retained, or lost by the runtime) is still known, and so a create interrupted by a
//! crash leaves something findable. Every container also carries its record in a label, so a
//! lost state directory is rebuilt from the runtime (`Manager::reconcile_inner`, manager.rs).
//!
//! Parts (`store/`):
//! - `leftovers.rs`: what a crash leaves (spool files, unfinished snapshots, temp files), cleared
//!   when blocklyd starts.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::durable::{self, WriteError};
use crate::ids::{SnapshotId, WorkloadId};
use crate::protocol::{Proto, SnapshotView, SpecRecord};

pub mod leftovers;

pub const RECORD_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Phase {
    /// Accepted and persisted; the container may not exist yet.
    Creating,
    /// Has (or should have) a container.
    Active,
    /// Compute removed on request, data kept.
    Retained,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AllocatedPort {
    pub name: String,
    pub protocol: Proto,
    pub container_port: u16,
    pub host_port: u16,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct WorkloadRecord {
    pub version: u32,
    pub id: WorkloadId,
    pub generation: u64,
    pub phase: Phase,
    pub spec: SpecRecord,
    pub spec_digest: String,
    pub ports: Vec<AllocatedPort>,
    pub container_name: String,
    pub container_id: Option<String>,
    /// The placement epoch this copy belongs to (see `protocol::EPOCH_HEADER`). None for a
    /// workload made without one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch: Option<u64>,
    /// The newer epoch that superseded this copy. Once set, the copy is never started again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by: Option<u64>,
    /// Set when the control plane asks for a stop or kill and cleared by a start: an exit after
    /// it is a stop, not a crash, even if blocklyd restarted in between.
    pub stop_requested_at: Option<String>,
    /// The host's boot (kernel boot id) in which blocklyd last saw this workload running, cleared
    /// once it saw it stop in that same boot. Still set for an earlier boot after a restart of
    /// the host means the host went down under it: it is resumed (see `Manager::resume`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub running_boot: Option<String>,
    /// Restarts blocklyd made after failures in the current run; a requested start or a new spec
    /// begins a new one. Kept here, not in memory, so a restart of blocklyd doesn't give a crash
    /// loop its retries again.
    #[serde(default)]
    pub restart_count: u32,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{what} {path}: {source}")]
    Io { what: &'static str, path: PathBuf, source: io::Error },
    /// The daemon can't hand a directory to the workloads' data owner: it isn't root, or runs
    /// without CAP_CHOWN.
    #[error(
        "can't give {path} to {}:{}, the workloads' data owner: {source}. blocklyd runs as root (with \
         CAP_CHOWN), or workloads.data_owner is its own uid:gid",
        owner.0,
        owner.1
    )]
    Ownership { path: PathBuf, owner: (u32, u32), source: io::Error },
    #[error("another blocklyd holds {0}; one blocklyd per state directory")]
    Locked(PathBuf),
}

fn io_err<'a>(what: &'static str, path: &'a Path) -> impl FnOnce(io::Error) -> StoreError + 'a {
    move |source| StoreError::Io { what, path: path.to_owned(), source }
}

impl From<WriteError> for StoreError {
    fn from(e: WriteError) -> Self {
        StoreError::Io { what: e.what, path: e.path, source: e.source }
    }
}

/// A record file that couldn't be read. Reported, never deleted: a human decides.
#[derive(Debug, Clone)]
pub struct BadRecord {
    pub path: PathBuf,
    pub problem: String,
}

#[derive(Clone, Debug)]
pub struct Store {
    root: PathBuf,
}

impl Store {
    /// Makes the layout (idempotent) and tightens the root's mode.
    pub fn open(root: &Path) -> Result<Self, StoreError> {
        for dir in [root.to_owned(), root.join("workloads"), root.join("trash")] {
            fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir).map_err(io_err("creating", &dir))?;
        }
        fs::set_permissions(root, fs::Permissions::from_mode(0o700)).map_err(io_err("securing", root))?;
        Ok(Self { root: root.to_owned() })
    }

    /// The store at `root` as it stands, making and changing nothing: for looking (`doctor`),
    /// not serving.
    pub fn existing(root: &Path) -> Self {
        Self { root: root.to_owned() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Holds an exclusive lock on `blocklyd.lock` for as long as the file lives. A second blocklyd on
    /// the same state directory would hand out the same ports twice; it refuses to start.
    pub fn lock(&self) -> Result<File, StoreError> {
        let path = self.root.join("blocklyd.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .mode(0o600)
            .open(&path)
            .map_err(io_err("opening", &path))?;
        match file.try_lock() {
            Ok(()) => Ok(file),
            Err(fs::TryLockError::WouldBlock) => Err(StoreError::Locked(path)),
            Err(fs::TryLockError::Error(source)) => Err(StoreError::Io { what: "locking", path, source }),
        }
    }

    pub fn workloads_dir(&self) -> PathBuf {
        self.root.join("workloads")
    }

    pub fn trash_dir(&self) -> PathBuf {
        self.root.join("trash")
    }

    /// `workloads/<id>`. A `WorkloadId` is one plain path component by construction, so this
    /// never leaves `workloads/`.
    pub fn workload_dir(&self, id: &WorkloadId) -> PathBuf {
        let dir = self.workloads_dir().join(id.as_str());
        debug_assert_eq!(dir.parent(), Some(self.workloads_dir().as_path()));
        dir
    }

    pub fn data_dir(&self, id: &WorkloadId) -> PathBuf {
        self.workload_dir(id).join("data")
    }

    fn record_path(&self, id: &WorkloadId) -> PathBuf {
        self.workload_dir(id).join("workload.json")
    }

    /// Every record on disk, and the ones that couldn't be read.
    pub fn load_all(&self) -> Result<(Vec<WorkloadRecord>, Vec<BadRecord>), StoreError> {
        let dir = self.workloads_dir();
        let mut records = Vec::new();
        let mut bad = Vec::new();
        for entry in fs::read_dir(&dir).map_err(io_err("listing", &dir))? {
            let entry = entry.map_err(io_err("listing", &dir))?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let Ok(id) = WorkloadId::parse(name) else {
                bad.push(BadRecord { path: entry.path(), problem: "directory name is not a workload id".into() });
                continue;
            };
            let path = self.record_path(&id);
            match fs::read(&path) {
                Ok(bytes) => match serde_json::from_slice::<WorkloadRecord>(&bytes) {
                    Ok(record) if record.id == id => records.push(record),
                    Ok(_) => bad.push(BadRecord { path, problem: "record id doesn't match its directory".into() }),
                    Err(e) => bad.push(BadRecord { path, problem: format!("unreadable: {e}") }),
                },
                // A directory without a record: orphaned data, reported by reconciliation.
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => bad.push(BadRecord { path, problem: e.to_string() }),
            }
        }
        records.sort_by(|a, b| a.id.cmp(&b.id));
        Ok((records, bad))
    }

    /// Workload directories that hold no record: data blocklyd doesn't know. Never deleted.
    pub fn orphan_dirs(&self, known: &std::collections::BTreeSet<WorkloadId>) -> Vec<PathBuf> {
        let Ok(entries) = fs::read_dir(self.workloads_dir()) else { return Vec::new() };
        entries
            .filter_map(Result::ok)
            .filter(|e| {
                let name = e.file_name();
                let name = name.to_string_lossy();
                match WorkloadId::parse(&name) {
                    Ok(id) => !known.contains(&id) && !e.path().join("workload.json").exists(),
                    Err(_) => true,
                }
            })
            .map(|e| e.path())
            .collect()
    }

    /// Atomic replace: a crash leaves the old record or the new one, never half of either. Two
    /// saves of one record at once each write a file of their own, and the last rename wins.
    pub fn save(&self, record: &WorkloadRecord) -> Result<(), StoreError> {
        let dir = self.workload_dir(&record.id);
        fs::DirBuilder::new().recursive(true).mode(0o700).create(&dir).map_err(io_err("creating", &dir))?;
        let bytes = serde_json::to_vec_pretty(record).expect("records serialize");
        Ok(durable::write_atomic(&self.record_path(&record.id), &bytes, 0o600)?)
    }

    /// Makes `data/` if missing and gives it to the workload's user. Returns whether it was
    /// made now, so a failed create only ever removes a directory it made itself.
    pub fn ensure_data_dir(&self, id: &WorkloadId, owner: (u32, u32)) -> Result<bool, StoreError> {
        let dir = self.data_dir(id);
        let parent = self.workload_dir(id);
        fs::DirBuilder::new().recursive(true).mode(0o700).create(&parent).map_err(io_err("creating", &parent))?;
        match fs::DirBuilder::new().mode(0o750).create(&dir) {
            Ok(()) => {
                std::os::unix::fs::chown(&dir, Some(owner.0), Some(owner.1))
                    .map_err(|source| StoreError::Ownership { path: dir.clone(), owner, source })?;
                Ok(true)
            }
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(false),
            Err(e) => Err(StoreError::Io { what: "creating", path: dir, source: e }),
        }
    }

    /// Whether this daemon can hand a directory to `owner`, as every create does. Checked when
    /// blocklyd starts, so a daemon without the privilege for it refuses to start rather than
    /// failing every create that follows.
    pub fn check_ownable(&self, owner: (u32, u32)) -> Result<(), StoreError> {
        let probe = self.root.join(format!(".ownership-check-{}", std::process::id()));
        let _ = fs::remove_dir(&probe);
        fs::DirBuilder::new().mode(0o700).create(&probe).map_err(io_err("creating", &probe))?;
        let given = std::os::unix::fs::chown(&probe, Some(owner.0), Some(owner.1));
        let _ = fs::remove_dir(&probe);
        given.map_err(|source| StoreError::Ownership { path: probe, owner, source })
    }

    /// Undoes a create that failed before anything ran: removes the record, and the data
    /// directory only if it is empty (`remove_dir` refuses otherwise), so data can't be lost.
    pub fn abandon(&self, id: &WorkloadId) {
        let _ = fs::remove_file(self.record_path(id));
        let _ = fs::remove_dir(self.data_dir(id));
        let _ = fs::remove_dir(self.workload_dir(id));
    }

    /// Archives in transit (exports being uploaded, restores being downloaded). On the same
    /// filesystem as the data, so free space checked for one is free space for the other.
    pub fn spool_dir(&self) -> PathBuf {
        self.root.join("spool")
    }

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

    /// Moves the whole workload directory (record and data) into the trash, in one rename.
    pub fn trash(&self, id: &WorkloadId, now_unix: i64) -> Result<Option<PathBuf>, StoreError> {
        let from = self.workload_dir(id);
        if !from.exists() {
            return Ok(None);
        }
        let to = self.trash_dir().join(format!("{id}.{now_unix}"));
        fs::rename(&from, &to).map_err(io_err("trashing", &from))?;
        File::open(self.workloads_dir()).and_then(|d| d.sync_all()).ok();
        Ok(Some(to))
    }

    /// Purges trash entries older than `retention_secs`. Returns what it removed.
    pub fn purge_trash(&self, now_unix: i64, retention_secs: i64) -> Vec<PathBuf> {
        let Ok(entries) = fs::read_dir(self.trash_dir()) else { return Vec::new() };
        let mut purged = Vec::new();
        for entry in entries.filter_map(Result::ok) {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(ts) = name.rsplit_once('.').and_then(|(_, ts)| ts.parse::<i64>().ok()) else { continue };
            if now_unix - ts >= retention_secs && fs::remove_dir_all(entry.path()).is_ok() {
                purged.push(entry.path());
            }
        }
        purged
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

/// Local snapshots, beside the data they copy: on the same filesystem, so a copy can share its
/// blocks, and out of the workload's reach, since only `data/` is mounted.
impl Store {
    pub fn snapshots_dir(&self, id: &WorkloadId) -> PathBuf {
        self.workload_dir(id).join("snapshots")
    }

    /// `snapshots/<snapshot>`. A `SnapshotId` is one plain path component, like a `WorkloadId`.
    pub fn snapshot_dir(&self, id: &WorkloadId, snapshot: &SnapshotId) -> PathBuf {
        self.snapshots_dir(id).join(snapshot.as_str())
    }

    /// A finished snapshot's description, if it exists.
    pub fn snapshot(&self, id: &WorkloadId, snapshot: &SnapshotId) -> Option<SnapshotView> {
        let bytes = fs::read(self.snapshot_dir(id, snapshot).join("snapshot.json")).ok()?;
        serde_json::from_slice::<SnapshotView>(&bytes).ok().filter(|v| v.id == *snapshot && v.workload == *id)
    }

    /// The workload's finished snapshots, oldest first.
    pub fn snapshots(&self, id: &WorkloadId) -> Vec<SnapshotView> {
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
    pub fn finish_snapshot(&self, view: &SnapshotView) -> Result<(), StoreError> {
        let path = self.snapshot_dir(&view.workload, &view.id).join("snapshot.json");
        let bytes = serde_json::to_vec_pretty(view).expect("serializes");
        Ok(durable::write_atomic(&path, &bytes, 0o600)?)
    }

    /// Removes a snapshot, finished or not. Returns whether there was one. Blocking.
    pub fn remove_snapshot(&self, id: &WorkloadId, snapshot: &SnapshotId) -> Result<bool, StoreError> {
        let dir = self.snapshot_dir(id, snapshot);
        match fs::remove_dir_all(&dir) {
            Ok(()) => Ok(true),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(StoreError::Io { what: "removing", path: dir, source: e }),
        }
    }
}

/// A released host port still in quarantine, as persisted in `ports.json`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RestingPort {
    pub protocol: Proto,
    pub port: u16,
    pub released_at_unix: i64,
}

impl Store {
    /// Quarantined ports survive restarts, so a restarted daemon can't hand out a port a route
    /// may still point at. Rewritten whole, atomically, on each release.
    pub fn save_resting_ports(&self, ports: &[RestingPort]) -> Result<(), StoreError> {
        let bytes = serde_json::to_vec_pretty(ports).expect("serializes");
        Ok(durable::write_atomic(&self.root.join("ports.json"), &bytes, 0o600)?)
    }

    pub fn load_resting_ports(&self) -> Vec<RestingPort> {
        fs::read(self.root.join("ports.json")).ok().and_then(|b| serde_json::from_slice(&b).ok()).unwrap_or_default()
    }
}

/// Bytes a directory tree occupies on disk (allocated blocks, not apparent sizes), without
/// following symlinks or leaving the filesystem. A workload's data can't lead it anywhere else:
/// it is `tree::disk_usage`, which never opens a directory by path. Blocking: call from
/// `spawn_blocking`.
pub fn disk_usage(root: &Path) -> io::Result<u64> {
    crate::tree::disk_usage(root)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::protocol::{Resources, RestartSpec, StopSpec, Storage};

    pub(crate) fn record(id: &str) -> WorkloadRecord {
        WorkloadRecord {
            version: RECORD_VERSION,
            id: WorkloadId::parse(id).unwrap(),
            generation: 1,
            phase: Phase::Active,
            spec: SpecRecord {
                image: "alpine:3.22".into(),
                entrypoint: None,
                env: Default::default(),
                secret_names: vec![],
                resources: Resources { memory_mb: 256, cpu_millis: None, cpu_weight: None, pids_limit: None },
                storage: Storage { mount_path: "/data".into(), size_gb: 1 },
                ports: vec![],
                stop: StopSpec::default(),
                restart: RestartSpec::default(),
                labels: Default::default(),
            },
            spec_digest: "sha256:x".into(),
            ports: vec![AllocatedPort {
                name: "game".into(),
                protocol: Proto::Tcp,
                container_port: 25565,
                host_port: 42000,
            }],
            container_name: format!("bly-{id}"),
            container_id: None,
            epoch: None,
            superseded_by: None,
            stop_requested_at: None,
            running_boot: None,
            restart_count: 0,
            created_at: "2026-09-28T00:00:00Z".into(),
            updated_at: "2026-09-28T00:00:00Z".into(),
        }
    }

    #[test]
    fn records_round_trip_and_bad_ones_are_reported_not_lost() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.save(&record("a")).unwrap();
        store.save(&record("b")).unwrap();
        fs::create_dir_all(store.workloads_dir().join("c")).unwrap();
        fs::write(store.workloads_dir().join("c").join("workload.json"), b"{not json").unwrap();
        let (records, bad) = store.load_all().unwrap();
        assert_eq!(records.iter().map(|r| r.id.as_str()).collect::<Vec<_>>(), ["a", "b"]);
        assert_eq!(bad.len(), 1);
        assert!(store.workloads_dir().join("c").join("workload.json").exists(), "never deleted");
    }

    #[test]
    fn saves_of_one_record_at_once_each_leave_it_whole() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        // Of different lengths, so a torn write would show.
        let written: Vec<WorkloadRecord> = (0..8)
            .map(|n| WorkloadRecord { running_boot: (n % 2 == 0).then(|| "b".repeat(n * 40)), ..record("w") })
            .collect();
        std::thread::scope(|s| {
            for r in &written {
                s.spawn(|| (0..50).for_each(|_| store.save(r).unwrap()));
            }
        });
        let (records, bad) = store.load_all().unwrap();
        assert!(bad.is_empty() && written.contains(&records[0]), "{bad:?}");
        let left: Vec<_> =
            fs::read_dir(store.workloads_dir().join("w")).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert_eq!(left, ["workload.json"]);
    }

    #[test]
    fn ports_in_quarantine_are_read_back_as_saved() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let resting = [RestingPort { protocol: Proto::Udp, port: 42001, released_at_unix: 1_000 }];
        store.save_resting_ports(&resting).unwrap();
        assert_eq!(store.load_resting_ports(), resting);
    }

    #[test]
    fn one_daemon_per_state_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let held = store.lock().unwrap();
        assert!(matches!(store.lock(), Err(StoreError::Locked(_))));
        drop(held);
        assert!(store.lock().is_ok());
    }

    #[test]
    fn whether_data_can_go_to_the_workloads_user_is_known_up_front() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let me = (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw());
        store.check_ownable(me).unwrap();
        assert_eq!(fs::read_dir(tmp.path()).unwrap().count(), 2, "the probe leaves nothing behind");
        // Only root may give a directory away; anyone else hears why, and what to change.
        if me.0 != 0 {
            let refused = store.check_ownable((me.0 + 1, me.1)).unwrap_err().to_string();
            assert!(refused.contains("workloads.data_owner"), "{refused}");
            let id = WorkloadId::parse("w1").unwrap();
            let failed = store.ensure_data_dir(&id, (me.0 + 1, me.1)).unwrap_err();
            assert!(matches!(failed, StoreError::Ownership { .. }), "{failed}");
        }
    }

    #[test]
    fn abandon_never_removes_data() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let id = WorkloadId::parse("w").unwrap();
        let uid = rustix::process::getuid().as_raw();
        let gid = rustix::process::getgid().as_raw();
        assert!(store.ensure_data_dir(&id, (uid, gid)).unwrap());
        fs::write(store.data_dir(&id).join("level.dat"), b"world").unwrap();
        store.abandon(&id);
        assert!(store.data_dir(&id).join("level.dat").exists());
    }

    #[test]
    fn trash_moves_everything_and_purges_only_when_old() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.save(&record("w")).unwrap();
        let id = WorkloadId::parse("w").unwrap();
        let moved = store.trash(&id, 1_000).unwrap().unwrap();
        assert!(!store.workload_dir(&id).exists());
        assert!(moved.join("workload.json").exists());
        assert!(store.purge_trash(1_000 + 60, 3600).is_empty());
        assert_eq!(store.purge_trash(1_000 + 3600, 3600).len(), 1);
        assert!(store.trash(&id, 2_000).unwrap().is_none(), "trashing twice is a no-op");
    }

    #[test]
    fn a_record_from_before_restart_counts_loads_with_none() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        let mut old = serde_json::to_value(record("w")).unwrap();
        old.as_object_mut().unwrap().remove("restartCount").expect("written");
        fs::create_dir_all(store.workloads_dir().join("w")).unwrap();
        fs::write(store.workloads_dir().join("w/workload.json"), serde_json::to_vec(&old).unwrap()).unwrap();
        let (records, bad) = store.load_all().unwrap();
        assert!(bad.is_empty(), "{bad:?}");
        assert_eq!(records[0].restart_count, 0);
    }

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

    #[test]
    fn orphan_dirs_are_found() {
        let tmp = tempfile::tempdir().unwrap();
        let store = Store::open(tmp.path()).unwrap();
        store.save(&record("known")).unwrap();
        fs::create_dir_all(store.workloads_dir().join("lost").join("data")).unwrap();
        let known = [WorkloadId::parse("known").unwrap()].into_iter().collect();
        let orphans = store.orphan_dirs(&known);
        assert_eq!(orphans.len(), 1);
        assert!(orphans[0].ends_with("lost"));
    }

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
            method: crate::tree::Method::Copy,
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
