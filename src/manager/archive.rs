// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

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
                code: ConflictCode::NotQuiesced,
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
        std::fs::create_dir_all(&spool_dir).map_err(|source| NodeError::Io { what: "spool".into(), source })?;
        let spool = spool_dir.join(format!("{}.tar.gz", uuid::Uuid::new_v4()));
        let _cleanup = RemoveOnDrop(spool.clone());
        let packed = {
            let spool = spool.clone();
            // Watched as it is written, as a restore is: admitted alone, it may not be alone.
            let mut watch = self.floor_watch();
            blocking(move || {
                crate::tarball::pack(&root, &spool, &exclude, &mut |read| watch.wrote(read).map_err(|e| e.to_string()))
            })
            .await?
            .map_err(|e| copy_failed("packing the data", e))?
        };
        let limit = self.config.transfer.max_put_bytes();
        if let (Some(parts), true) = (parts, packed.size_bytes > limit) {
            let put = put_parts(&spool, packed.size_bytes, parts, self.config.transfer.idle()).await?;
            return Ok((packed, Some(put)));
        }
        one_put(packed.size_bytes, limit)?;
        let (body, length) = crate::http_client::file_body(&spool)
            .await
            .map_err(|source| NodeError::Io { what: "spool".into(), source })?;
        let mut headers: Vec<(&str, String)> = extra_headers.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
        headers.push(("content-length", length.to_string()));
        let response =
            crate::http_client::send(hyper::Method::PUT, url, &headers, body, tls, crate::tarball::TRANSFER_LIMIT)
                .await
                .map_err(|e| NodeError::TransferBroke { what: "uploading the archive", source: e.into() })?;
        if !response.status().is_success() {
            let status = response.status();
            // A few bytes, but a store that stops sending them would hold the export, and the
            // workload with it, forever: past the idle time the refusal is told without them.
            let idle = self.config.transfer.idle();
            let text = tokio::time::timeout(idle, crate::http_client::read_body(response, 4096))
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or_default();
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
        let source = RestoreSource::try_from(request).map_err(NodeError::Invalid)?;
        let lock = self.lock_for(&id);
        let _guard = lock.lock().await;
        let started = Instant::now();
        let result = self.restore_locked(&id, source, epoch).await;
        self.metrics.operation("restore", &result, started.elapsed());
        result
    }

    async fn restore_locked(
        &self,
        id: &WorkloadId,
        source: RestoreSource,
        epoch: Option<u64>,
    ) -> Result<RestoreResponse, NodeError> {
        let started = Instant::now();
        let record = self.record(id)?;
        check_epoch(&record, epoch, EpochRule::Exact)?;
        if record.phase != Phase::Active {
            return Err(NodeError::Conflict {
                code: ConflictCode::NotCreated,
                message: "PUT the workload's spec before restoring into it".into(),
            });
        }
        let running = self.observe(&record).await?.is_some_and(|i| {
            matches!(i.status, ContainerStatus::Running | ContainerStatus::Restarting | ContainerStatus::Paused)
        });
        if running {
            return Err(NodeError::Conflict {
                code: ConflictCode::NotStopped,
                message: "stop the workload before restoring into it".into(),
            });
        }
        let restoring = self.store.restoring_dir(id);
        // What an earlier restore left unfinished is settled first, as a start would settle it.
        self.settle_restore(id).await?;
        let owner = self.config.data_owner_ids();
        let made = match &source {
            RestoreSource::Url { url, sha256 } => self.unpack_from(id, url, sha256.as_deref(), &restoring, owner).await,
            RestoreSource::Snapshot(snapshot) => self.copy_from_snapshot(id, snapshot, &restoring, owner).await,
        };
        let made = match made {
            Ok(made) => made,
            Err(e) => {
                let _ = blocking(move || std::fs::remove_dir_all(&restoring)).await;
                return Err(e);
            }
        };
        let swapped = {
            let (store, id) = (self.store.clone(), id.clone());
            blocking(move || store.swap_in_restored(&id, now().unix_timestamp())).await?
        };
        let previous = match swapped {
            Ok(previous) => previous,
            Err(e) => {
                // Settled as a crash here would be at the next start: the old data or the new is
                // in place, whole.
                let _ = self.settle_restore(id).await;
                return Err(e.into());
            }
        };
        self.state.lock().unwrap().disk.remove(id);
        tracing::info!(workload = %id, bytes = made.size_bytes, entries = made.entries, from = ?source, "restored");
        Ok(RestoreResponse {
            previous_data: previous.map(|p| p.to_string_lossy().into_owned()),
            duration_ms: started.elapsed().as_millis() as u64,
            ..made
        })
    }

    /// Settles what a restore left on disk when it didn't finish (`Store::recover_restore`), and
    /// says so. Returns the issue to report when that put the restored data in place: whoever asked
    /// for the restore may not have heard that it succeeded.
    pub(super) async fn settle_restore(&self, id: &WorkloadId) -> Result<Option<Issue>, NodeError> {
        let (store, of) = (self.store.clone(), id.clone());
        match blocking(move || store.recover_restore(&of, now().unix_timestamp())).await?? {
            Recovery::None => Ok(None),
            Recovery::Finished { previous } => {
                tracing::warn!(workload = %id, ?previous, "finished an interrupted restore: the restored data is in place");
                Ok(Some(Issue::new(
                    IssueCode::RestoreFinished,
                    "a restore was interrupted after its data was complete, and finished when blocklyd started",
                )))
            }
            Recovery::Discarded => {
                tracing::warn!(workload = %id, "removed an interrupted restore that never replaced the data");
                Ok(None)
            }
        }
    }

    /// Unpacks an archive into `into` as it downloads, and checks its sha256 once the whole
    /// archive has arrived, keeping the disk above its floor all the while.
    async fn unpack_from(
        &self,
        id: &WorkloadId,
        url: &str,
        expected: Option<&str>,
        into: &std::path::Path,
        owner: (u32, u32),
    ) -> Result<RestoreResponse, NodeError> {
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
            Duration::from_hours(1),
        )
        .await
        .map_err(|e| NodeError::TransferBroke { what: "downloading the archive", source: e.into() })?;
        if !response.status().is_success() {
            return Err(NodeError::Transfer(format!("the store answered HTTP {}", response.status())));
        }
        let announced =
            response.headers().get(hyper::header::CONTENT_LENGTH).and_then(|v| v.to_str().ok()?.parse().ok());
        if let Some(length) = announced {
            self.admit_disk(length, "the archive's download")?;
        }
        // Unpacked as it lands, into `into` beside the data, and hashed on the way: the archive is
        // never on disk whole, and the unpack doesn't wait for the download. Nothing replaces the
        // data until the hash matches (`restore_locked` swaps only after this returns), so an
        // archive that turns out wrong costs only what `into` holds, which is then discarded.
        let (frames, landed) = tokio::sync::mpsc::channel::<bytes::Bytes>(DOWNLOAD_FRAMES);
        let checked = expected.is_some();
        let unpacking = {
            let (into, beside, mut watch) = (into.to_owned(), self.store.workload_dir(id), self.floor_watch());
            blocking(move || {
                use crate::tarball::UnpackError;
                let mut download =
                    crate::tarball::Hashing::new(Landed { frames: landed, current: bytes::Bytes::new() });
                let unpacked = (|| {
                    // On disk before anything calls it complete (`Store::swap_in_restored`).
                    let disk = crate::durable::FilesystemSync::begin(&beside)?;
                    let unpacked = crate::tarball::unpack_reading(&mut download, &into, owner, &mut |written| {
                        watch.wrote(written).map_err(|e| e.to_string())
                    })?;
                    disk.finish()?;
                    Ok::<_, UnpackError>(unpacked)
                })();
                // The rest is read through too: what the tarball ends before (its padding, gzip's
                // trailer), so the hash is the whole archive's as the store has it; and, when a
                // checksum was given, what follows an unpack that failed, so an archive that isn't
                // the one asked for is said to be that, as it was when the download was checked
                // first.
                if unpacked.is_ok() || checked {
                    let most = crate::tarball::MAX_UNPACKED_BYTES;
                    let rest = std::io::copy(&mut std::io::Read::take(&mut download, most), &mut std::io::sink())
                        .map_err(|e| UnpackError::Damaged(e.to_string()));
                    let rest = match rest {
                        Ok(read) if read == most => {
                            Err(UnpackError::TooLarge("the archive goes on past its tarball".into()))
                        }
                        other => other,
                    };
                    if let Err(e) = rest {
                        return (unpacked.and(Err(e)), download.finish());
                    }
                }
                (unpacked, download.finish())
            })
        };
        let (received, unpacked) = tokio::join!(self.receive(response.into_body(), frames), unpacking);
        // A download that broke says so, before the unpack it cut short says the archive was damaged.
        received?;
        let (unpacked, (_, sha256, size_bytes)) = unpacked?;
        if let Some(expected) = expected
            && !expected.eq_ignore_ascii_case(&sha256)
        {
            return Err(NodeError::ChecksumMismatch { expected: expected.to_owned(), actual: sha256 });
        }
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

    /// Hands a download's frames to the unpack as they arrive. A body that stops coming never ends
    /// by itself, and this holds the workload's lock: it fails once nothing arrives for a while,
    /// or once nobody is waiting for it any more. A closed channel means the unpack stopped, which
    /// says why itself.
    async fn receive(
        &self,
        mut body: hyper::body::Incoming,
        frames: tokio::sync::mpsc::Sender<bytes::Bytes>,
    ) -> Result<(), NodeError> {
        use http_body_util::BodyExt;
        let idle = self.config.transfer.idle();
        let deadline = tokio::time::Instant::now() + crate::tarball::TRANSFER_LIMIT;
        let stalled = move |_| {
            let why = if tokio::time::Instant::now() < deadline {
                format!("nothing arrived for {idle:?}")
            } else {
                format!("not finished within {:?}", crate::tarball::TRANSFER_LIMIT)
            };
            NodeError::Transfer(format!("downloading the archive: {why}"))
        };
        while let Some(frame) = tokio::time::timeout_at(deadline.min(tokio::time::Instant::now() + idle), body.frame())
            .await
            .map_err(stalled)?
        {
            let frame =
                frame.map_err(|e| NodeError::TransferBroke { what: "downloading the archive", source: e.into() })?;
            if let Ok(data) = frame.into_data()
                && frames.send(data).await.is_err()
            {
                break;
            }
        }
        Ok(())
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
        let copied = blocking(move || {
            // On disk before anything calls it complete (`Store::swap_in_restored`).
            let disk = crate::durable::FilesystemSync::begin(&beside)?;
            let copied = crate::tree::copy_tree(&from, &into, Some(owner), &[], &mut |copied| {
                watch.wrote(copied).map_err(|e| e.to_string())
            })?;
            disk.finish().map(|()| copied)
        })
        .await?
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

/// Downloaded frames waiting for the unpack: a few, so the network runs ahead of the disk only a
/// little.
const DOWNLOAD_FRAMES: usize = 16;

/// A download as the unpack reads it: its frames as they land, blocking until the next one comes,
/// and its end once the receiver has handed over the last.
struct Landed {
    frames: tokio::sync::mpsc::Receiver<bytes::Bytes>,
    current: bytes::Bytes,
}

impl std::io::Read for Landed {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        while self.current.is_empty() {
            match self.frames.blocking_recv() {
                Some(next) => self.current = next,
                // Closed: the download ended, or broke, which the receiver says.
                None => return Ok(0),
            }
        }
        let n = buf.len().min(self.current.len());
        buf[..n].copy_from_slice(&self.current.split_to(n));
        Ok(n)
    }
}

/// How many times one part is sent before the archive fails: the store's own hiccups pass.
const PART_TRIES: u32 = 3;

/// Sends the spool in parts, `part_size` bytes each but the last, each to its own presigned URL,
/// and returns what finishes the upload: each part's number and ETag. Parts too few for the
/// archive are refused before anything is sent, as one PUT too small is. A part the store failed
/// to take (a dropped connection, a 5xx) is sent again; one it refused fails the archive, and the
/// control plane drops the upload.
async fn put_parts(
    spool: &std::path::Path,
    size: u64,
    parts: &PartsTarget,
    idle: Duration,
) -> Result<Vec<PutPart>, NodeError> {
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
            match put_part(spool, url, &parts.headers, offset, length, tls.clone(), idle).await {
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
    idle: Duration,
) -> Result<String, (String, bool)> {
    let body = crate::http_client::file_range_body(spool, offset, length)
        .await
        .map_err(|e| (format!("reading the spool: {e}"), false))?;
    let mut headers: Vec<(&str, String)> = extra_headers.iter().map(|(k, v)| (k.as_str(), v.clone())).collect();
    headers.push(("content-length", length.to_string()));
    let response =
        crate::http_client::send(hyper::Method::PUT, url, &headers, body, tls, crate::tarball::TRANSFER_LIMIT)
            .await
            .map_err(|e| (e.to_string(), e.is_transient()))?;
    let status = response.status();
    let etag = response.headers().get(hyper::header::ETAG).and_then(|v| v.to_str().ok()).map(str::to_owned);
    // A few bytes, but a store that stops sending them would hold the export forever: a stall is
    // a part the store failed to take, and it is sent again like one.
    let text = tokio::time::timeout(idle, crate::http_client::read_body(response, 4096))
        .await
        .map_err(|_| (format!("the store's answer stopped: nothing arrived for {idle:?}"), true))?
        .unwrap_or_default();
    if !status.is_success() {
        let said = String::from_utf8_lossy(&text).chars().take(300).collect::<String>();
        let transient = status.is_server_error()
            || status == hyper::StatusCode::TOO_MANY_REQUESTS
            || status == hyper::StatusCode::REQUEST_TIMEOUT;
        return Err((format!("the store refused it: HTTP {status}: {said}"), transient));
    }
    etag.filter(|e| !e.is_empty()).ok_or_else(|| ("the store answered without an ETag".to_owned(), false))
}

/// A write that failed for want of space reached the disk's floor the hard way; any other failure
/// is blocklyd's own.
fn write_failed(what: &str, e: std::io::Error) -> NodeError {
    if e.kind() == std::io::ErrorKind::StorageFull {
        NodeError::InsufficientDisk(format!("{what}: {e}"))
    } else {
        NodeError::Io { what: what.to_owned(), source: e }
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
        let eio = std::io::Error::from_raw_os_error(libc::EIO);
        let said = format!("copying: {eio}");
        let failed = copy_failed("copying", eio);
        assert!(matches!(failed, NodeError::Io { .. }), "{failed:?}");
        assert_eq!(failed.to_string(), said, "as the API answers it");
    }

    #[test]
    fn an_archive_one_put_cant_carry_is_refused_before_it_is_sent() {
        let limit = crate::protocol::MAX_SINGLE_PUT_BYTES;
        assert_eq!(crate::config::TransferConfig::default().max_put_bytes(), limit, "the default is R2's");
        assert!(one_put(limit, limit).is_ok());
        let err = one_put(limit + 1, limit).unwrap_err();
        assert!(
            matches!(err, NodeError::ArchiveTooLarge { size_bytes, limit_bytes } if size_bytes == limit + 1 && limit_bytes == limit),
            "{err:?}"
        );
    }
}
