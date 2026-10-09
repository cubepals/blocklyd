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
//! - `restore.rs`: putting a restore's new data in place of the old, and finishing one a crash
//!   interrupted.
//! - `snapshots.rs`: local snapshots, beside the data they copy.
//! - `quarantine.rs`: released host ports still resting, kept across restarts in `ports.json`.

use std::fs::{self, File, OpenOptions};
use std::io;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::durable::{self, WriteError};
use crate::ids::WorkloadId;
use crate::protocol::{Proto, SpecRecord};

pub mod leftovers;
mod quarantine;
pub mod restore;
mod snapshots;

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

/// A released host port still in quarantine, as persisted in `ports.json`.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RestingPort {
    pub protocol: Proto,
    pub port: u16,
    pub released_at_unix: i64,
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
}
