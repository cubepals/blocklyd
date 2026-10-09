//! Whether there is room on this host for what a verb is about to do: memory for a workload to
//! run, disk above the floor the host keeps. It only answers; the verbs that ask (`ensure.rs`,
//! `power.rs`, `restarts.rs`, `archive.rs`, `snapshots.rs`) decide what a refusal means. What
//! workloads actually use is sampled in `stats.rs`.

use super::*;

impl Manager {
    pub(super) fn check_disk(&self) -> Result<(), NodeError> {
        above_floor(self.store.root(), self.config.capacity.min_free_disk_mb)
    }

    /// Watches the disk's floor through a long write, from where `check_disk` left off.
    pub(super) fn floor_watch(&self) -> FloorWatch {
        FloorWatch {
            root: self.store.root().to_owned(),
            floor_mb: self.config.capacity.min_free_disk_mb,
            next: ROOM_CHECK_BYTES,
        }
    }

    /// Memory admission: running workloads, this one included, fit in what the host gives out.
    /// Refusing here is Blockly's `RuntimeFull`: "no room where it runs just now".
    pub(super) fn admit(&self, id: &WorkloadId, memory_mb: u64) -> Result<(), NodeError> {
        let running = self.running_memory_mb(Some(id));
        let limit = (self.allocatable_memory_mb() as f64 * self.config.capacity.memory_overcommit) as u64;
        if running + memory_mb > limit {
            return Err(NodeError::InsufficientCapacity(format!(
                "{running} MB of {limit} MB is taken by running workloads; this one needs {memory_mb} MB"
            )));
        }
        Ok(())
    }

    pub(super) fn running_memory_mb(&self, except: Option<&WorkloadId>) -> u64 {
        let state = self.state.lock().unwrap();
        state
            .records
            .values()
            .filter(|r| Some(&r.id) != except)
            .filter(|r| {
                state.observed.get(&r.id).and_then(|o| o.info.as_ref()).is_some_and(|i| {
                    matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused)
                })
            })
            .map(|r| u64::from(r.spec.resources.memory_mb))
            .sum()
    }

    pub(super) fn provisioned_memory_mb(&self) -> u64 {
        let state = self.state.lock().unwrap();
        state
            .records
            .values()
            .filter(|r| r.phase != Phase::Retained)
            .map(|r| u64::from(r.spec.resources.memory_mb))
            .sum()
    }

    /// Fails unless the disk keeps its floor after `need` more bytes.
    pub(super) fn admit_disk(&self, need: u64, what: &str) -> Result<(), NodeError> {
        if let Some((_, available)) = host::disk(self.store.root()) {
            let floor = self.config.capacity.min_free_disk_mb.saturating_mul(1024 * 1024);
            if available < need.saturating_add(floor) {
                return Err(NodeError::InsufficientDisk(format!(
                    "{what} needs about {} MB and this host keeps {} MB free",
                    need / (1024 * 1024),
                    self.config.capacity.min_free_disk_mb
                )));
            }
        }
        Ok(())
    }
}

/// How much a long write (a restore's download and unpack, a snapshot's copy, an export's spool)
/// puts on disk between two looks at free space: often enough to stop well short of a full disk,
/// not a statvfs per write.
const ROOM_CHECK_BYTES: u64 = 256 * 1024 * 1024;

/// Keeps a long write above the disk's floor. Told the bytes written so far, it looks at free
/// space every `ROOM_CHECK_BYTES`, and fails once the disk is below the floor.
pub(super) struct FloorWatch {
    root: std::path::PathBuf,
    floor_mb: u64,
    next: u64,
}

impl FloorWatch {
    pub(super) fn wrote(&mut self, total: u64) -> Result<(), NodeError> {
        if total < self.next {
            return Ok(());
        }
        self.next = total.saturating_add(ROOM_CHECK_BYTES);
        above_floor(&self.root, self.floor_mb)
    }
}

/// Fails when free disk under `root` is below the floor this host keeps (`min_free_disk_mb`).
fn above_floor(root: &std::path::Path, floor_mb: u64) -> Result<(), NodeError> {
    if let Some((_, available)) = host::disk(root)
        && available < floor_mb.saturating_mul(1024 * 1024)
    {
        return Err(NodeError::InsufficientDisk(format!(
            "only {} MB free under {}; this host keeps at least {floor_mb} MB free",
            available / (1024 * 1024),
            root.display(),
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_long_write_looks_at_the_floor_every_few_hundred_mib_not_every_write() {
        let dir = tempfile::tempdir().unwrap();
        // A floor no disk is above: every look fails.
        let mut watch = FloorWatch { root: dir.path().to_owned(), floor_mb: 1_000_000_000, next: ROOM_CHECK_BYTES };
        assert!(watch.wrote(64 * 1024).is_ok(), "no look yet");
        assert!(watch.wrote(ROOM_CHECK_BYTES - 1).is_ok(), "nor yet");
        assert!(matches!(watch.wrote(ROOM_CHECK_BYTES), Err(NodeError::InsufficientDisk(_))));
        // A floor every disk is above: every look passes.
        let mut watch = FloorWatch { root: dir.path().to_owned(), floor_mb: 0, next: ROOM_CHECK_BYTES };
        assert!((1..=4).all(|n| watch.wrote(n * ROOM_CHECK_BYTES).is_ok()));
    }
}
