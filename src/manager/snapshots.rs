//! Local snapshots of a workload's data: taken, listed, deleted, uploaded. Packing and sending
//! an upload is `pack_and_put` in `archive.rs`; restoring from a snapshot is `restore` there.

use super::*;

impl Manager {
    // ─── snapshots ─────────────────────────────────────────────────────────────────────────

    /// A copy of the workload's data beside it, on this node: what a backup is first, before it
    /// is uploaded, and what a restore on this node reads. Taken from the current copy only, like
    /// an export, and from a running workload only while its saving is paused.
    pub async fn snapshot(
        self: &Arc<Self>,
        id: WorkloadId,
        request: SnapshotRequest,
        epoch: Option<u64>,
    ) -> Result<SnapshotResponse, NodeError> {
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.snapshot_locked(&id, request, epoch).await;
        self.metrics.operation("snapshot", &result, started.elapsed());
        result
    }

    async fn snapshot_locked(
        &self,
        id: &WorkloadId,
        request: SnapshotRequest,
        epoch: Option<u64>,
    ) -> Result<SnapshotResponse, NodeError> {
        let started = Instant::now();
        let record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Exact)?;
        if let Some(existing) = self.store.snapshot(id, &request.id) {
            return Ok(SnapshotResponse { created: false, snapshot: existing });
        }
        if record.phase == Phase::Creating {
            return Err(NodeError::Conflict {
                code: "not_created",
                message: "the workload's creation didn't finish; PUT its spec again".into(),
            });
        }
        let running = self.observe(&record).await?.is_some_and(|i| {
            matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused)
        });
        if running && !request.quiesced {
            return Err(NodeError::Conflict {
                code: "not_quiesced",
                message: "stop the workload, or pause its saving and say so (quiesced), before a snapshot".into(),
            });
        }
        let data = self.store.data_dir(id);
        // The data's full size, even where the copy will share blocks with it: a shared copy is
        // free only until the world changes, and then takes as much as a plain one. Counted as
        // free, copies could pile up on a node near its floor and need that space later.
        let need = {
            let data = data.clone();
            tokio::task::spawn_blocking(move || crate::transfer::tree_bytes(&data)).await.unwrap_or(0)
        };
        self.admit_disk(need, "a snapshot")?;
        let into = self.store.prepare_snapshot(id, &request.id)?;
        let copied = {
            let (into, beside) = (into.clone(), self.store.snapshot_dir(id, &request.id));
            // Watched as it is written: snapshots of other workloads may be admitted beside it.
            let mut watch = self.floor_watch();
            tokio::task::spawn_blocking(move || {
                // On disk before snapshot.json says the snapshot exists.
                let disk = crate::durable::FilesystemSync::begin(&beside)?;
                let copied = crate::tree::copy_tree(&data, &into, None, &[], &mut |copied| {
                    watch.wrote(copied).map_err(|e| e.to_string())
                })?;
                disk.finish().map(|()| copied)
            })
            .await
            .map_err(|e| NodeError::Internal(format!("copying panicked: {e}")))?
        };
        let copied = match copied {
            Ok(copied) => copied,
            Err(e) => {
                let _ = self.store.remove_snapshot(id, &request.id);
                return Err(archive::copy_failed("copying the data", e));
            }
        };
        let view = SnapshotView {
            id: request.id.clone(),
            workload: id.clone(),
            epoch: record.epoch,
            created_at: now_str(),
            size_bytes: copied.walked.bytes,
            files: copied.walked.files,
            method: copied.method,
            quiesced: running,
            spec_digest: record.spec_digest.clone(),
            duration_ms: started.elapsed().as_millis() as u64,
        };
        if let Err(e) = self.store.finish_snapshot(&view) {
            let _ = self.store.remove_snapshot(id, &request.id);
            return Err(e.into());
        }
        tracing::info!(
            workload = %id,
            snapshot = %view.id,
            bytes = view.size_bytes,
            method = view.method.as_str(),
            took_ms = view.duration_ms,
            "snapshot taken"
        );
        Ok(SnapshotResponse { created: true, snapshot: view })
    }

    pub fn snapshots(&self, id: &WorkloadId) -> Result<SnapshotList, NodeError> {
        self.record(id)?;
        Ok(SnapshotList { snapshots: self.store.snapshots(id) })
    }

    /// Removes a snapshot. Nothing to remove is not an error: the workload or the snapshot may
    /// already be gone, with the workload's data, and the control plane only needs it gone.
    pub async fn delete_snapshot(
        self: &Arc<Self>,
        id: WorkloadId,
        snapshot: SnapshotId,
    ) -> Result<SnapshotDeleteResponse, NodeError> {
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        if self.state.lock().unwrap().uploading.contains(&(id.clone(), snapshot.clone())) {
            return Err(NodeError::Conflict {
                code: "snapshot_busy",
                message: "the snapshot is being uploaded; delete it once that ends".into(),
            });
        }
        let store = self.store.clone();
        let (id2, snapshot2) = (id.clone(), snapshot.clone());
        let existed = tokio::task::spawn_blocking(move || store.remove_snapshot(&id2, &snapshot2))
            .await
            .map_err(|e| NodeError::Internal(format!("removing panicked: {e}")))??;
        if existed {
            tracing::info!(workload = %id, %snapshot, "snapshot removed");
        }
        Ok(SnapshotDeleteResponse { existed })
    }

    /// A snapshot as a gzip tarball, PUT to a presigned URL. It doesn't hold the workload's lock:
    /// a snapshot never changes, and an upload of a large one shouldn't hold up a stop.
    pub async fn upload_snapshot(
        self: &Arc<Self>,
        id: WorkloadId,
        snapshot: SnapshotId,
        request: UploadRequest,
    ) -> Result<ExportResponse, NodeError> {
        let started = Instant::now();
        let key = (id.clone(), snapshot.clone());
        let result = async {
            if let Some(parts) = &request.parts {
                parts.validate().map_err(NodeError::Invalid)?;
            }
            {
                let lock = self.lock_for(&id);
                let _guard = lock.lock().await;
                if self.store.snapshot(&id, &snapshot).is_none() {
                    return Err(NodeError::SnapshotNotFound(snapshot.to_string()));
                }
                if !self.state.lock().unwrap().uploading.insert(key.clone()) {
                    return Err(NodeError::Conflict {
                        code: "snapshot_busy",
                        message: "this snapshot is being uploaded already".into(),
                    });
                }
            }
            let root = self.store.snapshot_dir(&id, &snapshot).join("data");
            let packed =
                self.pack_and_put(root, Vec::new(), &request.url, &request.headers, request.parts.as_ref()).await;
            self.state.lock().unwrap().uploading.remove(&key);
            let (packed, parts) = packed?;
            tracing::info!(
                workload = %id,
                %snapshot,
                bytes = packed.size_bytes,
                parts = parts.as_ref().map_or(0, Vec::len),
                "snapshot uploaded"
            );
            Ok(ExportResponse {
                size_bytes: packed.size_bytes,
                sha256: packed.sha256,
                format: "tar.gz".into(),
                entries: packed.entries,
                duration_ms: started.elapsed().as_millis() as u64,
                parts,
            })
        }
        .await;
        self.metrics.operation("upload", &result, started.elapsed());
        result
    }
}
