//! Writes a workload's record to disk when its newest state is in memory already: a change made
//! by whoever holds no workload lock (an observation, from a runtime event) or must not fail on
//! the disk (a restart counted). Writes of records and of the port quarantine take turns
//! (`Manager::disk_writes`): `save_record` and `persist_resting_ports` in `manager.rs`, and the
//! trash in `delete.rs`, take the same turn. Whichever write lands last is what a restarted
//! blocklyd reads, so each writes the state as it is when its turn comes, never a copy taken
//! before: a verb's newer record is never overwritten by an older one, a record trashed meanwhile
//! is not brought back, and two writers never share a half-written file.

use super::*;

impl Manager {
    /// Writes the workload's record as memory holds it now, if it still has one.
    pub(super) fn write_record(&self, id: &WorkloadId) -> Result<(), NodeError> {
        let _turn = self.disk_writes.lock().unwrap();
        let Some(record) = self.state.lock().unwrap().records.get(id).cloned() else { return Ok(()) };
        Ok(self.store.save(&record)?)
    }
}
