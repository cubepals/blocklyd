//! Writes a workload's record and the port quarantine to disk, on the blocking pool: each write
//! syncs twice (`durable::write_atomic`), which an async thread must not wait for. `save_record`
//! writes a record a verb changed and then holds it in memory; `write_record` writes one whose
//! newest state is in memory already: a change made by whoever holds no workload lock (an
//! observation, from a runtime event) or must not fail on the disk (a restart counted). Writes of
//! records and of the port quarantine take turns (`Manager::disk_writes`), and the trash in
//! `delete.rs` takes the same turn. Whichever write lands last is what a restarted
//! blocklyd reads, so each writes the state as it is when its turn comes, never a copy taken
//! before: a verb's newer record is never overwritten by an older one, a record trashed meanwhile
//! is not brought back, and two writers never share a half-written file.

use super::*;

impl Manager {
    /// Writes `record` and then holds it in memory as the workload's current one.
    pub(super) async fn save_record(&self, record: &WorkloadRecord) -> Result<(), NodeError> {
        let (turn, state, store, record) =
            (self.disk_writes.clone(), self.state.clone(), self.store.clone(), record.clone());
        blocking(move || {
            let _turn = turn.lock().unwrap();
            store.save(&record)?;
            state.lock().unwrap().records.insert(record.id.clone(), record);
            Ok(())
        })
        .await?
    }

    /// Writes the workload's record as memory holds it now, if it still has one.
    pub(super) async fn write_record(&self, id: &WorkloadId) -> Result<(), NodeError> {
        let (turn, state, store, id) = (self.disk_writes.clone(), self.state.clone(), self.store.clone(), id.clone());
        blocking(move || {
            let _turn = turn.lock().unwrap();
            let Some(record) = state.lock().unwrap().records.get(&id).cloned() else { return Ok(()) };
            Ok(store.save(&record)?)
        })
        .await?
    }

    /// Writes the port quarantine to disk, as it is when its turn comes. A failure is logged, not
    /// fatal: at worst a restart forgets which ports were resting.
    pub(super) async fn persist_resting_ports(&self) {
        let (turn, state, store) = (self.disk_writes.clone(), self.state.clone(), self.store.clone());
        let written = blocking(move || {
            let _turn = turn.lock().unwrap();
            let now_unix = now().unix_timestamp();
            let resting: Vec<crate::store::RestingPort> = state
                .lock()
                .unwrap()
                .ports
                .resting()
                .into_iter()
                .map(|(protocol, port, ago)| crate::store::RestingPort {
                    protocol,
                    port,
                    released_at_unix: now_unix - ago.as_secs() as i64,
                })
                .collect();
            store.save_resting_ports(&resting)
        })
        .await;
        if let Err(e) = written.and_then(|saved| Ok(saved?)) {
            tracing::warn!(error = %e, "couldn't persist the port quarantine");
        }
    }
}

/// Runs blocking file work on tokio's blocking pool, off the async threads. A panic in it goes on
/// in the caller, as it would have inline.
pub(super) async fn blocking<T: Send + 'static>(work: impl FnOnce() -> T + Send + 'static) -> Result<T, NodeError> {
    tokio::task::spawn_blocking(work).await.map_err(|e| match e.try_into_panic() {
        Ok(panic) => std::panic::resume_unwind(panic),
        Err(e) => NodeError::Internal(format!("the runtime dropped blocking work: {e}")),
    })
}
