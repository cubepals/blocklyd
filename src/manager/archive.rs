//! Carries a world's data between this node and an object store: export out, restore in, over
//! presigned URLs. Local snapshots are `snapshots.rs`, which uploads through `pack_and_put`
//! here; whether the disk has room is asked of `room.rs`.

use super::*;

impl Manager {
    // ─── data transfer ─────────────────────────────────────────────────────────────────────

    /// The workload's data as a gzip tarball, PUT to a presigned URL. Only the current copy
    /// exports (its exact epoch, never a superseded one), so a stale copy can't become the newest
    /// backup. A running workload exports only if the caller says it paused its saving.
    pub async fn export(
        self: &Arc<Self>,
        id: WorkloadId,
        request: ExportRequest,
        epoch: Option<u64>,
    ) -> Result<ExportResponse, NodeError> {
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.export_locked(&id, request, epoch).await;
        self.metrics.operation("export", &result, started.elapsed());
        result
    }

    async fn export_locked(
        &self,
        id: &WorkloadId,
        request: ExportRequest,
        epoch: Option<u64>,
    ) -> Result<ExportResponse, NodeError> {
        let started = Instant::now();
        crate::protocol::validate_exclude(&request.exclude).map_err(NodeError::Invalid)?;
        if let Some(parts) = &request.parts {
            parts.validate().map_err(NodeError::Invalid)?;
        }
        let record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Exact)?;
        let running = self.observe(&record).await?.is_some_and(|i| {
            matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused)
        });
        if running && !request.quiesced {
            return Err(NodeError::Conflict {
                code: "not_quiesced",
                message: "stop the workload, or pause its saving and say so (quiesced), before exporting".into(),
            });
        }
        let (packed, parts) = self
            .pack_and_put(
                self.store.data_dir(id),
                request.exclude,
                &request.url,
                &request.headers,
                request.parts.as_ref(),
            )
            .await?;
        tracing::info!(
            workload = %id,
            bytes = packed.size_bytes,
            sha256 = %packed.sha256,
            parts = parts.as_ref().map_or(0, Vec::len),
            "exported"
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

    /// Packs `root` into a spool file and sends it to the store: in one PUT to `url`, or in
    /// `parts` when it is larger than one PUT carries here (`transfer.max_put_mb`). Without parts
    /// such an archive is refused before anything is sent. The spool goes either way.
    pub(super) async fn pack_and_put(
        &self,
        root: std::path::PathBuf,
        exclude: Vec<String>,
        url: &str,
        extra_headers: &BTreeMap<String, String>,
        parts: Option<&PartsTarget>,
    ) -> Result<(crate::tarball::Packed, Option<Vec<PutPart>>), NodeError> {
        let tls = tls_for(url)?;
        let need = {
            let root = root.clone();
            tokio::task::spawn_blocking(move || crate::tarball::tree_bytes(&root)).await.unwrap_or(0)
        };
        self.admit_disk(need, "the archive's spool")?;
        let spool_dir = self.store.spool_dir();
        std::fs::create_dir_all(&spool_dir).map_err(|e| NodeError::Internal(format!("spool: {e}")))?;
        let spool = spool_dir.join(format!("{}.tar.gz", uuid::Uuid::new_v4()));
        let _cleanup = RemoveOnDrop(spool.clone());
        let packed = {
            let spool = spool.clone();
            // Watched as it is written, as a restore is: admitted alone, it may not be alone.
            let mut watch = self.floor_watch();
            tokio::task::spawn_blocking(move || {
                crate::tarball::pack(&root, &spool, &exclude, &mut |read| watch.wrote(read).map_err(|e| e.to_string()))
            })
            .await
            .map_err(|e| NodeError::Internal(format!("packing panicked: {e}")))?
            .map_err(|e| copy_failed("packing the data", e))?
        };
        let limit = self.config.transfer.max_put_bytes();
        if let (Some(parts), true) = (parts, packed.size_bytes > limit) {
            let put = put_parts(&spool, packed.size_bytes, parts).await?;
            return Ok((packed, Some(put)));
        }
        one_put(packed.size_bytes, limit)?;
        let (body, length) =
            crate::http_client::file_body(&spool).await.map_err(|e| NodeError::Internal(format!("spool: {e}")))?;
        let mut headers: Vec<(&str, String)> = extra_headers.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        headers.push(("content-length", length.to_string()));
        let response =
            crate::http_client::send(hyper::Method::PUT, url, &headers, body, tls, Duration::from_secs(3 * 3600))
                .await
                .map_err(|e| NodeError::Transfer(format!("uploading the archive: {e}")))?;
        if !response.status().is_success() {
            let status = response.status();
            let text = crate::http_client::read_body(response, 4096).await.unwrap_or_default();
            return Err(NodeError::Transfer(format!(
                "the store refused the upload: HTTP {status}: {}",
                String::from_utf8_lossy(&text).chars().take(300).collect::<String>()
            )));
        }
        Ok((packed, None))
    }

    /// Replaces a stopped workload's data: from an archive at a presigned URL, checked against its
    /// sha256, or from one of the workload's snapshots on this node. The new data is put together
    /// beside the current data before anything is replaced, so a bad or unsafe archive costs
    /// nothing; the data it replaces goes to the trash.
    pub async fn restore(
        self: &Arc<Self>,
        id: WorkloadId,
        request: RestoreRequest,
        epoch: Option<u64>,
    ) -> Result<RestoreResponse, NodeError> {
        request.validate().map_err(NodeError::Invalid)?;
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.restore_locked(&id, request, epoch).await;
        self.metrics.operation("restore", &result, started.elapsed());
        result
    }

    async fn restore_locked(
        &self,
        id: &WorkloadId,
        request: RestoreRequest,
        epoch: Option<u64>,
    ) -> Result<RestoreResponse, NodeError> {
        let started = Instant::now();
        let record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Exact)?;
        if record.phase != Phase::Active {
            return Err(NodeError::Conflict {
                code: "not_created",
                message: "PUT the workload's spec before restoring into it".into(),
            });
        }
        let running = self.observe(&record).await?.is_some_and(|i| {
            matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused)
        });
        if running {
            return Err(NodeError::Conflict {
                code: "not_stopped",
                message: "stop the workload before restoring into it".into(),
            });
        }
        let restoring = self.store.restoring_dir(id);
        // What an earlier restore left unfinished is settled first, as a start would settle it.
        self.settle_restore(id)?;
        let owner = self.config.data_owner_ids();
        let made = match (&request.url, &request.snapshot) {
            (Some(url), _) => self.unpack_from(id, url, request.sha256.as_deref(), &restoring, owner).await,
            (None, Some(snapshot)) => self.copy_from_snapshot(id, snapshot, &restoring, owner).await,
            (None, None) => unreachable!("validated"),
        };
        let made = match made {
            Ok(made) => made,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&restoring);
                return Err(e);
            }
        };
        let previous = match self.store.swap_in_restored(id, now().unix_timestamp()) {
            Ok(previous) => previous,
            Err(e) => {
                // Settled as a crash here would be at the next start: the old data or the new is
                // in place, whole.
                let _ = self.settle_restore(id);
                return Err(e.into());
            }
        };
        self.state.lock().unwrap().disk.remove(id);
        tracing::info!(workload = %id, bytes = made.size_bytes, entries = made.entries, snapshot = ?request.snapshot, "restored");
        Ok(RestoreResponse {
            previous_data: previous.map(|p| p.to_string_lossy().into_owned()),
            duration_ms: started.elapsed().as_millis() as u64,
            ..made
        })
    }

    /// Settles what a restore left on disk when it didn't finish (`Store::recover_restore`), and
    /// says so. Returns the issue to report when that put the restored data in place: whoever asked
    /// for the restore may not have heard that it succeeded.
    pub(super) fn settle_restore(&self, id: &WorkloadId) -> Result<Option<Issue>, NodeError> {
        match self.store.recover_restore(id, now().unix_timestamp())? {
            Recovery::None => Ok(None),
            Recovery::Finished { previous } => {
                tracing::warn!(workload = %id, ?previous, "finished an interrupted restore: the restored data is in place");
                Ok(Some(Issue::new(
                    "restore_finished",
                    "a restore was interrupted after its data was complete, and finished when blocklyd started",
                )))
            }
            Recovery::Discarded => {
                tracing::warn!(workload = %id, "removed an interrupted restore that never replaced the data");
                Ok(None)
            }
        }
    }

    /// Downloads an archive to the spool, checks it, and unpacks it into `into`, keeping the disk
    /// above its floor all the while.
    async fn unpack_from(
        &self,
        id: &WorkloadId,
        url: &str,
        expected: Option<&str>,
        into: &std::path::Path,
        owner: (u32, u32),
    ) -> Result<RestoreResponse, NodeError> {
        use http_body_util::BodyExt;
        use std::io::Write;
        let tls = tls_for(url)?;
        // Admitted against the disk's floor, as snapshots and exports are: nothing is downloaded
        // onto a disk already below it, and an archive that says how large it is must fit above
        // it. The rest is watched as it lands.
        self.check_disk()?;
        let response = crate::http_client::send(
            hyper::Method::GET,
            url,
            &[],
            crate::http_client::empty(),
            tls,
            Duration::from_secs(3600),
        )
        .await
        .map_err(|e| NodeError::Transfer(format!("downloading the archive: {e}")))?;
        if !response.status().is_success() {
            return Err(NodeError::Transfer(format!("the store answered HTTP {}", response.status())));
        }
        let announced =
            response.headers().get(hyper::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()?.parse().ok());
        if let Some(length) = announced {
            self.admit_disk(length, "the archive's download")?;
        }
        let spool_dir = self.store.spool_dir();
        std::fs::create_dir_all(&spool_dir).map_err(|e| NodeError::Internal(format!("spool: {e}")))?;
        let spool = spool_dir.join(format!("{id}-{}.tar.gz", uuid::Uuid::new_v4()));
        let _cleanup = RemoveOnDrop(spool.clone());
        let mut file = crate::tarball::Hashing::new(
            std::fs::File::create(&spool).map_err(|e| NodeError::Internal(format!("spool: {e}")))?,
        );
        let mut body = response.into_body();
        let mut watch = self.floor_watch();
        // A body that stops coming never ends by itself, and this holds the workload's lock: it
        // fails once nothing arrives for a while, or once nobody is waiting for it any more.
        let idle = crate::tarball::download_idle();
        let deadline = tokio::time::Instant::now() + crate::tarball::DOWNLOAD_LIMIT;
        let stalled = |_| {
            let why = if tokio::time::Instant::now() < deadline {
                format!("nothing arrived for {idle:?}")
            } else {
                format!("not finished within {:?}", crate::tarball::DOWNLOAD_LIMIT)
            };
            NodeError::Transfer(format!("downloading the archive: {why}"))
        };
        while let Some(frame) = tokio::time::timeout_at(deadline.min(tokio::time::Instant::now() + idle), body.frame())
            .await
            .map_err(stalled)?
        {
            let frame = frame.map_err(|e| NodeError::Transfer(format!("downloading the archive: {e}")))?;
            if let Ok(data) = frame.into_data() {
                file.write_all(&data).map_err(|e| write_failed("spool", e))?;
                watch.wrote(file.bytes())?;
                if file.bytes() > crate::tarball::MAX_UNPACKED_BYTES {
                    return Err(NodeError::InvalidArchive("the archive is larger than any world".into()));
                }
            }
        }
        let (spooled, sha256, size_bytes) = file.finish();
        spooled.sync_all().map_err(|e| write_failed("spool", e))?;
        if let Some(expected) = expected
            && !expected.eq_ignore_ascii_case(&sha256)
        {
            return Err(NodeError::ChecksumMismatch { expected: expected.to_owned(), actual: sha256 });
        }
        let unpacked = {
            let (spool, into, beside) = (spool.clone(), into.to_owned(), self.store.workload_dir(id));
            let mut watch = self.floor_watch();
            tokio::task::spawn_blocking(move || {
                // On disk before anything calls it complete (`Store::swap_in_restored`).
                let disk = crate::durable::FilesystemSync::begin(&beside)?;
                let unpacked = crate::tarball::unpack(&spool, &into, owner, &mut |written| {
                    watch.wrote(written).map_err(|e| e.to_string())
                })?;
                disk.finish()?;
                Ok(unpacked)
            })
            .await
            .map_err(|e| NodeError::Internal(format!("unpacking panicked: {e}")))?
        };
        let unpacked = unpacked.map_err(|e| match e {
            crate::tarball::UnpackError::NoRoom(message) => NodeError::InsufficientDisk(message),
            crate::tarball::UnpackError::Io(io) => write_failed("unpacking", io),
            other => NodeError::InvalidArchive(other.to_string()),
        })?;
        Ok(RestoreResponse {
            size_bytes,
            sha256: Some(sha256),
            entries: unpacked.entries,
            skipped: unpacked.skipped,
            unpacked_bytes: unpacked.bytes,
            previous_data: None,
            duration_ms: 0,
        })
    }

    /// Copies one of the workload's snapshots into `into`, sharing blocks where it can.
    async fn copy_from_snapshot(
        &self,
        id: &WorkloadId,
        snapshot: &SnapshotId,
        into: &std::path::Path,
        owner: (u32, u32),
    ) -> Result<RestoreResponse, NodeError> {
        let view =
            self.store.snapshot(id, snapshot).ok_or_else(|| NodeError::SnapshotNotFound(snapshot.to_string()))?;
        // Its full size, even where the copy shares blocks (see `snapshot_locked`).
        self.admit_disk(view.size_bytes, "restoring the snapshot")?;
        let from = self.store.snapshot_dir(id, snapshot).join("data");
        let (into, beside) = (into.to_owned(), self.store.workload_dir(id));
        let mut watch = self.floor_watch();
        let copied = tokio::task::spawn_blocking(move || {
            // On disk before anything calls it complete (`Store::swap_in_restored`).
            let disk = crate::durable::FilesystemSync::begin(&beside)?;
            let copied = crate::tree::copy_tree(&from, &into, Some(owner), &[], &mut |copied| {
                watch.wrote(copied).map_err(|e| e.to_string())
            })?;
            disk.finish().map(|()| copied)
        })
        .await
        .map_err(|e| NodeError::Internal(format!("copying panicked: {e}")))?
        .map_err(|e| copy_failed("copying the snapshot", e))?;
        Ok(RestoreResponse {
            size_bytes: copied.walked.bytes,
            sha256: None,
            entries: copied.walked.entries(),
            skipped: copied.walked.skipped,
            unpacked_bytes: copied.walked.bytes,
            previous_data: None,
            duration_ms: 0,
        })
    }
}

/// Refuses an archive one presigned PUT can't carry, before any of it is sent: the store would
/// refuse it only once it had it all, and every retry would send it all again.
fn one_put(size_bytes: u64, limit_bytes: u64) -> Result<(), NodeError> {
    if size_bytes > limit_bytes {
        return Err(NodeError::ArchiveTooLarge { size_bytes, limit_bytes });
    }
    Ok(())
}

/// How many times one part is sent before the archive fails: the store's own hiccups pass.
const PART_TRIES: u32 = 3;

/// Sends the spool in parts, `part_size` bytes each but the last, each to its own presigned URL,
/// and returns what finishes the upload: each part's number and ETag. Parts too few for the
/// archive are refused before anything is sent, as one PUT too small is. A part the store failed
/// to take (a dropped connection, a 5xx) is sent again; one it refused fails the archive, and the
/// control plane drops the upload.
async fn put_parts(spool: &std::path::Path, size: u64, parts: &PartsTarget) -> Result<Vec<PutPart>, NodeError> {
    let needed = crate::tarball::parts_needed(size, parts.part_size);
    if needed > parts.urls.len() as u64 {
        return Err(NodeError::ArchiveTooLarge { size_bytes: size, limit_bytes: parts.capacity() });
    }
    let mut put = Vec::with_capacity(needed as usize);
    for (index, url) in parts.urls.iter().take(needed as usize).enumerate() {
        let number = index as u64 + 1;
        let offset = index as u64 * parts.part_size;
        let length = parts.part_size.min(size - offset);
        let tls = tls_for(url)?;
        let mut tries = 0;
        let etag = loop {
            tries += 1;
            match put_part(spool, url, &parts.headers, offset, length, tls.clone()).await {
                Ok(etag) => break etag,
                Err((message, transient)) if transient && tries < PART_TRIES => {
                    tracing::warn!(part = number, of = needed, error = %message, "sending a part again");
                    tokio::time::sleep(Duration::from_secs(u64::from(tries))).await;
                }
                Err((message, _)) => {
                    return Err(NodeError::Transfer(format!("uploading part {number} of {needed}: {message}")));
                }
            }
        };
        put.push(PutPart { number, etag });
    }
    Ok(put)
}

/// One part: `length` bytes of the spool from `offset`, PUT to its URL. The ETag the store answers
/// with, or why not, and whether trying again could help.
async fn put_part(
    spool: &std::path::Path,
    url: &str,
    extra_headers: &BTreeMap<String, String>,
    offset: u64,
    length: u64,
    tls: Option<Arc<rustls::ClientConfig>>,
) -> Result<String, (String, bool)> {
    let body = crate::http_client::file_range_body(spool, offset, length)
        .await
        .map_err(|e| (format!("reading the spool: {e}"), false))?;
    let mut headers: Vec<(&str, String)> = extra_headers.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    headers.push(("content-length", length.to_string()));
    let response =
        crate::http_client::send(hyper::Method::PUT, url, &headers, body, tls, Duration::from_secs(3 * 3600))
            .await
            .map_err(|e| (e.to_string(), e.is_transient()))?;
    let status = response.status();
    let etag = response.headers().get(hyper::header::ETAG).and_then(|v| v.to_str().ok()).map(str::to_owned);
    // A few bytes, but a store that stops sending them would hold the export forever: a stall is
    // a part the store failed to take, and it is sent again like one.
    let idle = crate::tarball::download_idle();
    let text = tokio::time::timeout(idle, crate::http_client::read_body(response, 4096))
        .await
        .map_err(|_| (format!("the store's answer stopped: nothing arrived for {idle:?}"), true))?
        .unwrap_or_default();
    if !status.is_success() {
        let said = String::from_utf8_lossy(&text).chars().take(300).collect::<String>();
        let transient = status.is_server_error() || status.as_u16() == 429 || status.as_u16() == 408;
        return Err((format!("the store refused it: HTTP {status}: {said}"), transient));
    }
    etag.filter(|e| !e.is_empty()).ok_or_else(|| ("the store answered without an ETag".to_owned(), false))
}

/// A write that failed for want of space reached the disk's floor the hard way; any other failure
/// is blocklyd's own.
fn write_failed(what: &str, e: std::io::Error) -> NodeError {
    let message = format!("{what}: {e}");
    if e.kind() == std::io::ErrorKind::StorageFull {
        NodeError::InsufficientDisk(message)
    } else {
        NodeError::Internal(message)
    }
}

/// A copy or a pack its floor watch stopped reached the disk's floor, as one a write failed for
/// want of space did; any other failure is blocklyd's own.
pub(super) fn copy_failed(what: &str, e: std::io::Error) -> NodeError {
    match crate::tree::NoRoom::of(&e) {
        Some(refused) => NodeError::InsufficientDisk(refused.to_owned()),
        None => write_failed(what, e),
    }
}

/// Removes a spool file however the transfer ends.
struct RemoveOnDrop(std::path::PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// TLS for a presigned URL: none for http (a local store), the system's roots for https.
fn tls_for(url: &str) -> Result<Option<Arc<rustls::ClientConfig>>, NodeError> {
    if url.starts_with("https://") {
        crate::http_client::public_tls().map(Some).map_err(NodeError::Transfer)
    } else if url.starts_with("http://") {
        Ok(None)
    } else {
        Err(NodeError::Invalid(vec![FieldError { field: "url".into(), problem: "must be an http(s) URL".into() }]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_copy_the_floor_or_a_full_disk_stopped_is_insufficient_disk_and_any_other_failure_internal() {
        let refused = crate::tree::NoRoom::ask(&mut |_| Err("only 3 MB free".into()), 1).unwrap_err();
        assert!(matches!(copy_failed("copying", refused), NodeError::InsufficientDisk(m) if m == "only 3 MB free"));
        let full = std::io::Error::from(std::io::ErrorKind::StorageFull);
        assert!(matches!(copy_failed("copying", full), NodeError::InsufficientDisk(_)));
        let failed = std::io::Error::other("Input/output error");
        assert!(matches!(copy_failed("copying", failed), NodeError::Internal(m) if m == "copying: Input/output error"));
    }

    #[test]
    fn an_archive_one_put_cant_carry_is_refused_before_it_is_sent() {
        let limit = crate::protocol::MAX_SINGLE_PUT_BYTES;
        assert_eq!(limit, 5 * 1024 * 1024 * 1024 - 5 * 1024 * 1024, "R2's: 5 GiB less 5 MiB");
        assert_eq!(crate::config::TransferConfig::default().max_put_bytes(), limit, "the default is R2's");
        assert!(one_put(limit, limit).is_ok());
        let err = one_put(limit + 1, limit).unwrap_err();
        assert!(
            matches!(err, NodeError::ArchiveTooLarge { size_bytes, limit_bytes } if size_bytes == limit + 1 && limit_bytes == limit),
            "{err:?}"
        );
    }
}
