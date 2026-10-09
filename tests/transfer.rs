//! Export and restore through the manager, against the in-memory runtime and a tiny object store
//! served in-process (PUT stores, GET serves), the way presigned URLs behave.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::put;
use blocklyd::manager::{NodeError, Precondition, Records};
use blocklyd::protocol::{ExportRequest, RestoreRequest};
use blocklyd::store::restore::RESTORE_COMPLETE;
use sha2::Digest;
use support::{fixture, id, spec};

type Objects = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// What the store answers a PUT with, as S3 does: the stored bytes' ETag, quoted.
fn etag(bytes: &[u8]) -> String {
    format!("\"{}\"", &hex::encode(sha2::Sha256::digest(bytes))[..32])
}

async fn store() -> (String, Objects) {
    let objects: Objects = Arc::default();
    let app = Router::new()
        .route(
            "/{key}",
            put(|State(o): State<Objects>, Path(key): Path<String>, body: Bytes| async move {
                let tag = etag(&body);
                o.lock().unwrap().insert(key, body.to_vec());
                (StatusCode::OK, [(axum::http::header::ETAG, tag)])
            })
            .get(|State(o): State<Objects>, Path(key): Path<String>| async move {
                match o.lock().unwrap().get(&key) {
                    Some(bytes) => Ok(bytes.clone()),
                    None => Err(StatusCode::NOT_FOUND),
                }
            }),
        )
        .with_state(objects.clone())
        // A store takes parts of 5 MiB and more.
        .layer(axum::extract::DefaultBodyLimit::disable());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (url, objects)
}

fn export_to(url: String) -> ExportRequest {
    serde_json::from_value(serde_json::json!({ "url": url })).unwrap()
}

fn restore_from(url: String, sha256: Option<String>) -> RestoreRequest {
    serde_json::from_value(serde_json::json!({ "url": url, "sha256": sha256 })).unwrap()
}

#[tokio::test]
async fn a_world_moves_through_the_store_and_its_hash_holds() {
    let f = fixture("").await;
    let (base, objects) = store().await;
    let made = f.manager.ensure(id("src"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    std::fs::write(data.join("server.properties"), "level-name=world\n").unwrap();
    std::fs::create_dir_all(data.join("world")).unwrap();
    std::fs::write(data.join("world/level.dat"), b"the world").unwrap();

    // The key carries the query string a presigned URL would.
    let exported =
        f.manager.export(id("src"), export_to(format!("{base}/a.tar.gz?X-Amz-Signature=x")), Some(1)).await.unwrap();
    let stored = objects.lock().unwrap().get("a.tar.gz").cloned().expect("uploaded");
    assert_eq!(exported.sha256, hex::encode(sha2::Sha256::digest(&stored)));
    assert_eq!(exported.size_bytes, stored.len() as u64);
    assert_eq!(exported.format, "tar.gz");

    // Restored into another workload: a move to another node, as far as the node can tell.
    let target = f.manager.ensure(id("dst"), spec(), Precondition::None, Some(2)).await.unwrap();
    let target_data = std::path::PathBuf::from(&target.workload.locate.data_dir);
    std::fs::write(target_data.join("old.txt"), b"replaced").unwrap();
    let restored = f
        .manager
        .restore(id("dst"), restore_from(format!("{base}/a.tar.gz"), Some(exported.sha256.clone())), Some(2))
        .await
        .unwrap();
    assert_eq!(restored.sha256.as_deref(), Some(exported.sha256.as_str()));
    assert_eq!(std::fs::read(target_data.join("world/level.dat")).unwrap(), b"the world");
    assert!(!target_data.join("old.txt").exists(), "the data was replaced, not merged");
    let previous = std::path::PathBuf::from(restored.previous_data.expect("the old data went to the trash"));
    assert_eq!(std::fs::read(previous.join("old.txt")).unwrap(), b"replaced");
    assert!(!target_data.join(RESTORE_COMPLETE).exists(), "the swap leaves nothing of its own in the data");
    left_nothing(&f.manager, "dst");
}

/// Nothing a restore made is left beside the data: no spool file, no unpacked data.
fn left_nothing(m: &blocklyd::manager::Manager, w: &str) {
    let spooled: Vec<_> =
        std::fs::read_dir(m.store.spool_dir()).map(|d| d.filter_map(Result::ok).collect()).unwrap_or_default();
    assert!(spooled.is_empty(), "{spooled:?}");
    assert!(!m.store.restoring_dir(&id(w)).exists());
}

/// A store that answers with an archive of `length` bytes, by its headers, then sends 1 KiB of it
/// and nothing more, holding the connection open: a store edge that hung, or a path that drops
/// packets without a reset.
async fn announcing(length: u64) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/huge.tar.gz", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((mut socket, _)) = listener.accept().await {
            let _ = socket.read(&mut [0u8; 4096]).await;
            let head = format!("HTTP/1.1 200 OK\r\ncontent-length: {length}\r\n\r\n");
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(&[0u8; 1024]).await;
            held.push(socket);
        }
    });
    url
}

#[tokio::test]
async fn a_download_that_stops_coming_fails_and_lets_go_of_the_workload() {
    // Two minutes without a byte is a stall on a host; a second is, here.
    blocklyd::tarball::set_download_idle(std::time::Duration::from_secs(1));
    let f = fixture("").await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    std::fs::write(data.join("level.dat"), b"mine").unwrap();
    let url = announcing(1 << 20).await;
    let manager = f.manager.clone();
    let restore = tokio::spawn(async move { manager.restore(id("w"), restore_from(url, None), Some(1)).await });
    support::eventually("the download to begin", async || {
        let spooled = std::fs::read_dir(f.manager.store.spool_dir()).map_or(0, Iterator::count);
        (spooled > 0, spooled)
    })
    .await;
    // A stop sent meanwhile waits for the restore, which holds the workload, and then goes ahead.
    let stopping = f.manager.stop(id("w"), None, Some(1));
    tokio::time::timeout(std::time::Duration::from_secs(20), stopping)
        .await
        .expect("the stalled download held the workload")
        .unwrap();
    let err = restore.await.unwrap().unwrap_err();
    assert!(matches!(&err, NodeError::Transfer(m) if m.contains("nothing arrived")), "{err:?}");
    assert_eq!(std::fs::read(data.join("level.dat")).unwrap(), b"mine", "nothing was replaced");
    left_nothing(&f.manager, "w");
}

#[tokio::test]
async fn a_restore_onto_a_disk_below_its_floor_is_refused_and_changes_nothing() {
    let f = fixture("").await;
    let (base, _) = store().await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    world(&data);
    f.manager.export(id("w"), export_to(format!("{base}/w.tar.gz")), Some(1)).await.unwrap();
    std::fs::write(data.join("world/level.dat"), b"day two").unwrap();
    // The same node, now keeping a floor no disk is above.
    let full = support::manager_on(f.dir.path(), f.fake.clone(), "[capacity]\nmin_free_disk_mb = 1000000000\n");
    assert!(full.reconcile(Records::FromDisk).await.error.is_none());
    let err = full.restore(id("w"), restore_from(format!("{base}/w.tar.gz"), None), Some(1)).await.unwrap_err();
    assert!(matches!(err, NodeError::InsufficientDisk(_)), "{err:?}");
    assert_eq!(std::fs::read(data.join("world/level.dat")).unwrap(), b"day two", "the data is as it was");
    left_nothing(&full, "w");
}

#[tokio::test]
async fn a_download_the_disk_has_no_room_for_is_refused_before_it_is_spooled() {
    let f = fixture("").await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    std::fs::write(data.join("level.dat"), b"mine").unwrap();
    // It says it is a petabyte: no disk here has room for that above its floor.
    let url = announcing(1 << 50).await;
    let err = f.manager.restore(id("w"), restore_from(url, None), Some(1)).await.unwrap_err();
    assert!(matches!(err, NodeError::InsufficientDisk(_)), "{err:?}");
    assert_eq!(std::fs::read(data.join("level.dat")).unwrap(), b"mine");
    left_nothing(&f.manager, "w");
}

#[tokio::test]
async fn an_archive_too_large_for_one_upload_is_refused_with_its_sizes() {
    // What the control plane takes as final: a status, a code and both sizes.
    let limit = blocklyd::protocol::MAX_SINGLE_PUT_BYTES;
    let response = NodeError::ArchiveTooLarge { size_bytes: 6 << 30, limit_bytes: limit }.into_response();
    assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "archive_too_large");
    assert_eq!(body["error"]["details"], serde_json::json!({ "sizeBytes": 6u64 << 30, "limitBytes": limit }));
}

#[tokio::test]
async fn a_wrong_checksum_changes_nothing() {
    let f = fixture("").await;
    let (base, objects) = store().await;
    objects.lock().unwrap().insert("bad.tar.gz".into(), b"not what was promised".to_vec());
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    std::fs::write(data.join("keep.txt"), b"mine").unwrap();
    let err = f
        .manager
        .restore(id("w"), restore_from(format!("{base}/bad.tar.gz"), Some("00".repeat(32))), Some(1))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::ChecksumMismatch { .. }), "{err:?}");
    assert_eq!(std::fs::read(data.join("keep.txt")).unwrap(), b"mine");
    // Without a checksum the damaged archive is refused when unpacked, still before any change.
    let err = f.manager.restore(id("w"), restore_from(format!("{base}/bad.tar.gz"), None), Some(1)).await.unwrap_err();
    assert!(matches!(err, NodeError::InvalidArchive(_)), "{err:?}");
    assert_eq!(std::fs::read(data.join("keep.txt")).unwrap(), b"mine");
}

#[tokio::test]
async fn a_running_workload_exports_only_when_quiesced_and_never_restores() {
    let f = fixture("").await;
    let (base, _) = store().await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    f.manager.start(id("w"), Some(1)).await.unwrap();
    let err = f.manager.export(id("w"), export_to(format!("{base}/x.tar.gz")), Some(1)).await.unwrap_err();
    assert!(matches!(err, NodeError::Conflict { code: "not_quiesced", .. }), "{err:?}");
    let mut quiesced = export_to(format!("{base}/x.tar.gz"));
    quiesced.quiesced = true;
    f.manager.export(id("w"), quiesced, Some(1)).await.unwrap();
    let err = f.manager.restore(id("w"), restore_from(format!("{base}/x.tar.gz"), None), Some(1)).await.unwrap_err();
    assert!(matches!(err, NodeError::Conflict { code: "not_stopped", .. }), "{err:?}");
}

#[tokio::test]
async fn a_superseded_copy_can_never_become_the_newest_backup() {
    let f = fixture("").await;
    let (base, objects) = store().await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    f.manager.fence(id("w"), 4).await.unwrap();
    let err = f.manager.export(id("w"), export_to(format!("{base}/stale.tar.gz")), Some(3)).await.unwrap_err();
    assert!(matches!(err, NodeError::Superseded { by: 4 }), "{err:?}");
    assert!(objects.lock().unwrap().is_empty(), "nothing reached the store");
}

fn snapshot_request(id: &str, quiesced: bool) -> blocklyd::protocol::SnapshotRequest {
    serde_json::from_value(serde_json::json!({ "id": id, "quiesced": quiesced })).unwrap()
}

const SNAP: &str = "0b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10";

fn world(data: &std::path::Path) {
    std::fs::write(data.join("server.properties"), "level-name=world\n").unwrap();
    std::fs::create_dir_all(data.join("world/region")).unwrap();
    std::fs::write(data.join("world/level.dat"), b"day one").unwrap();
    std::fs::write(data.join("world/region/r.0.0.mca"), vec![9u8; 50_000]).unwrap();
    std::fs::create_dir_all(data.join("libraries/net")).unwrap();
    std::fs::write(data.join("libraries/net/server.jar"), b"remade on start").unwrap();
}

fn names_in(archive: &[u8]) -> Vec<String> {
    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(archive));
    tar.entries().unwrap().map(|e| e.unwrap().path().unwrap().to_string_lossy().into_owned()).collect()
}

#[tokio::test]
async fn a_snapshot_is_taken_once_restored_in_place_and_uploaded() {
    let f = fixture("").await;
    let (base, objects) = store().await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    world(&data);

    let taken = f.manager.snapshot(id("w"), snapshot_request(SNAP, false), Some(1)).await.unwrap();
    assert!(taken.created);
    assert_eq!(taken.snapshot.files, 4);
    assert_eq!(taken.snapshot.epoch, Some(1));
    // The same id again is the same snapshot, not a second one.
    let again = f.manager.snapshot(id("w"), snapshot_request(SNAP, false), Some(1)).await.unwrap();
    assert!(!again.created);
    assert_eq!(again.snapshot, taken.snapshot);
    assert_eq!(f.manager.snapshots(&id("w")).unwrap().snapshots.len(), 1);

    // The world moves on, then goes back to the snapshot.
    std::fs::write(data.join("world/level.dat"), b"day two, griefed").unwrap();
    let restored = f
        .manager
        .restore(id("w"), serde_json::from_value(serde_json::json!({ "snapshot": SNAP })).unwrap(), Some(1))
        .await
        .unwrap();
    assert_eq!(restored.sha256, None);
    assert_eq!(std::fs::read(data.join("world/level.dat")).unwrap(), b"day one");
    let previous = std::path::PathBuf::from(restored.previous_data.unwrap());
    assert_eq!(std::fs::read(previous.join("world/level.dat")).unwrap(), b"day two, griefed");
    assert!(!data.join(RESTORE_COMPLETE).exists());
    left_nothing(&f.manager, "w");

    // Uploaded, it is an archive like any export, which a restore elsewhere takes.
    let sent = f
        .manager
        .upload_snapshot(
            id("w"),
            blocklyd::ids::SnapshotId::parse(SNAP).unwrap(),
            serde_json::from_value(serde_json::json!({ "url": format!("{base}/snap.tar.gz") })).unwrap(),
        )
        .await
        .unwrap();
    let stored = objects.lock().unwrap().get("snap.tar.gz").cloned().unwrap();
    assert_eq!(sent.sha256, hex::encode(sha2::Sha256::digest(&stored)));
    assert!(names_in(&stored).iter().any(|n| n.ends_with("world/level.dat")));
    let other = f.manager.ensure(id("elsewhere"), spec(), Precondition::None, Some(2)).await.unwrap();
    f.manager
        .restore(id("elsewhere"), restore_from(format!("{base}/snap.tar.gz"), Some(sent.sha256.clone())), Some(2))
        .await
        .unwrap();
    let other_data = std::path::PathBuf::from(&other.workload.locate.data_dir);
    assert_eq!(std::fs::read(other_data.join("world/level.dat")).unwrap(), b"day one");

    // Deleting is idempotent, and a delete of the whole workload takes its snapshots along.
    let snap = blocklyd::ids::SnapshotId::parse(SNAP).unwrap();
    assert!(f.manager.delete_snapshot(id("w"), snap.clone()).await.unwrap().existed);
    assert!(!f.manager.delete_snapshot(id("w"), snap.clone()).await.unwrap().existed);
    assert!(!f.manager.delete_snapshot(id("never"), snap).await.unwrap().existed);
}

#[tokio::test]
async fn snapshots_follow_the_export_rules() {
    let f = fixture("").await;
    f.manager.ensure(id("w"), spec(), Precondition::None, Some(3)).await.unwrap();
    f.manager.start(id("w"), Some(3)).await.unwrap();
    let err = f.manager.snapshot(id("w"), snapshot_request(SNAP, false), Some(3)).await.unwrap_err();
    assert!(matches!(err, NodeError::Conflict { code: "not_quiesced", .. }), "{err:?}");
    let quiesced = f.manager.snapshot(id("w"), snapshot_request(SNAP, true), Some(3)).await.unwrap();
    assert!(quiesced.snapshot.quiesced);
    // A running workload's data is never replaced, from a snapshot or otherwise.
    let err = f
        .manager
        .restore(id("w"), serde_json::from_value(serde_json::json!({ "snapshot": SNAP })).unwrap(), Some(3))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::Conflict { code: "not_stopped", .. }), "{err:?}");
    // Only the current copy is copied.
    f.manager.fence(id("w"), 4).await.unwrap();
    let other = "1b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10";
    let err = f.manager.snapshot(id("w"), snapshot_request(other, true), Some(3)).await.unwrap_err();
    assert!(matches!(err, NodeError::Superseded { by: 4 }), "{err:?}");
    // A snapshot the node doesn't hold is named as such.
    let err = f
        .manager
        .restore(id("w"), serde_json::from_value(serde_json::json!({ "snapshot": other })).unwrap(), Some(3))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::Superseded { .. } | NodeError::SnapshotNotFound(_)), "{err:?}");
}

#[tokio::test]
async fn a_snapshot_is_restored_only_with_room_for_all_of_it() {
    let f = fixture("").await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    world(&data);
    let taken = f.manager.snapshot(id("w"), snapshot_request(SNAP, false), Some(1)).await.unwrap();
    // A snapshot of more than this disk holds. A copy sharing its blocks would take nothing at
    // first, and all of it once the world moves on: it is admitted at all of it, either way.
    let mut view = taken.snapshot.clone();
    view.size_bytes = 1 << 50;
    let snapshot = blocklyd::ids::SnapshotId::parse(SNAP).unwrap();
    let described = f.manager.store.snapshot_dir(&id("w"), &snapshot).join("snapshot.json");
    std::fs::write(described, serde_json::to_vec(&view).unwrap()).unwrap();
    std::fs::write(data.join("world/level.dat"), b"day two").unwrap();
    let err = f
        .manager
        .restore(id("w"), serde_json::from_value(serde_json::json!({ "snapshot": SNAP })).unwrap(), Some(1))
        .await
        .unwrap_err();
    assert!(matches!(err, NodeError::InsufficientDisk(_)), "{err:?}");
    assert_eq!(std::fs::read(data.join("world/level.dat")).unwrap(), b"day two");
    left_nothing(&f.manager, "w");
}

#[tokio::test]
async fn a_move_leaves_out_what_the_server_makes_again() {
    let f = fixture("").await;
    let (base, objects) = store().await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    world(std::path::Path::new(&made.workload.locate.data_dir));
    let request: ExportRequest = serde_json::from_value(serde_json::json!({
        "url": format!("{base}/move.tar.gz"),
        "exclude": ["libraries"],
    }))
    .unwrap();
    let exported = f.manager.export(id("w"), request, Some(1)).await.unwrap();
    let names = names_in(&objects.lock().unwrap().get("move.tar.gz").cloned().unwrap());
    assert!(names.iter().any(|n| n.ends_with("world/region/r.0.0.mca")));
    assert!(!names.iter().any(|n| n.contains("libraries")), "{names:?}");
    assert_eq!(exported.entries, names.len() as u64);
    // Only plain top-level names.
    for bad in ["../etc", "a/b", "", ".."] {
        let request: ExportRequest = serde_json::from_value(serde_json::json!({
            "url": format!("{base}/x.tar.gz"),
            "exclude": [bad],
        }))
        .unwrap();
        let err = f.manager.export(id("w"), request, Some(1)).await.unwrap_err();
        assert!(matches!(err, NodeError::Invalid(_)), "{bad:?}: {err:?}");
    }
}

#[tokio::test]
async fn an_export_never_follows_a_link_out_of_the_data() {
    let f = fixture("").await;
    let (base, objects) = store().await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    let outside = f.dir.path().join("host-secrets");
    std::fs::create_dir_all(&outside).unwrap();
    std::fs::write(outside.join("node.key"), b"PRIVATE KEY").unwrap();
    // What a plugin in the container could plant: a directory link and a file link.
    std::os::unix::fs::symlink(&outside, data.join("world")).unwrap();
    std::os::unix::fs::symlink(outside.join("node.key"), data.join("key")).unwrap();
    f.manager.export(id("w"), export_to(format!("{base}/x.tar.gz")), Some(1)).await.unwrap();
    let stored = objects.lock().unwrap().get("x.tar.gz").cloned().unwrap();
    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(stored.as_slice()));
    for entry in tar.entries().unwrap() {
        let mut entry = entry.unwrap();
        let mut body = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut body).unwrap();
        assert!(!body.windows(11).any(|w| w == b"PRIVATE KEY"), "{:?} carried the host's file", entry.path());
    }
    // The same for a snapshot.
    f.manager.snapshot(id("w"), snapshot_request(SNAP, false), Some(1)).await.unwrap();
    let snap = f.manager.store.snapshot_dir(&id("w"), &blocklyd::ids::SnapshotId::parse(SNAP).unwrap());
    assert!(std::fs::symlink_metadata(snap.join("data/world")).unwrap().file_type().is_symlink());
}

/// Bytes gzip can't shrink: an archive of them is as large as they are.
fn noise(len: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect()
}

const MIB: usize = 1024 * 1024;

/// A world of about `mib` MiB, most of it incompressible.
fn big_world(data: &std::path::Path, mib: usize) {
    std::fs::write(data.join("server.properties"), "level-name=world\n").unwrap();
    std::fs::create_dir_all(data.join("world/region")).unwrap();
    std::fs::write(data.join("world/level.dat"), b"the big world").unwrap();
    std::fs::write(data.join("world/region/r.0.0.mca"), noise(mib * MIB, 7)).unwrap();
}

fn in_parts(base: &str, part_size: usize, urls: usize) -> serde_json::Value {
    serde_json::json!({
        "partSize": part_size,
        "urls": (1..=urls).map(|n| format!("{base}/part-{n}?X-Amz-Signature=x")).collect::<Vec<_>>(),
        "headers": { "x-blockly-test": "parts" },
    })
}

/// The parts the store holds, joined in order: the object a multipart upload completes to.
fn joined(objects: &Objects, count: usize) -> Vec<u8> {
    let held = objects.lock().unwrap();
    (1..=count).flat_map(|n| held.get(&format!("part-{n}")).cloned().unwrap_or_default()).collect()
}

#[tokio::test]
async fn an_archive_larger_than_one_put_goes_in_parts_and_restores_whole() {
    // One PUT carries at most 1 MiB here, so an 11 MiB world goes in parts of 5 MiB: three.
    let f = fixture("[transfer]\nmax_put_mb = 1\n").await;
    let (base, objects) = store().await;
    let made = f.manager.ensure(id("big"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    big_world(&data, 11);

    let request: ExportRequest = serde_json::from_value(serde_json::json!({
        "url": format!("{base}/whole.tar.gz"),
        "parts": in_parts(&base, 5 * MIB, 4),
    }))
    .unwrap();
    let exported = f.manager.export(id("big"), request, Some(1)).await.unwrap();
    let parts = exported.parts.clone().expect("it went in parts");
    assert_eq!(parts.iter().map(|p| p.number).collect::<Vec<_>>(), [1, 2, 3], "as many as it needs, in order");
    assert!(objects.lock().unwrap().get("whole.tar.gz").is_none(), "nothing went in one PUT");
    assert!(objects.lock().unwrap().get("part-4").is_none(), "a URL left over is never called");
    {
        let held = objects.lock().unwrap();
        assert_eq!(held["part-1"].len(), 5 * MIB);
        assert_eq!(held["part-2"].len(), 5 * MIB);
        // Each part's ETag is the one the store answered that part with.
        for part in &parts {
            assert_eq!(part.etag, etag(&held[&format!("part-{}", part.number)]));
        }
    }
    let whole = joined(&objects, 3);
    assert_eq!(whole.len() as u64, exported.size_bytes);
    assert_eq!(exported.sha256, hex::encode(sha2::Sha256::digest(&whole)), "the hash is of the whole archive");

    // Joined, the parts are an archive like any other.
    objects.lock().unwrap().insert("joined.tar.gz".into(), whole);
    let target = f.manager.ensure(id("dst"), spec(), Precondition::None, Some(2)).await.unwrap();
    f.manager
        .restore(id("dst"), restore_from(format!("{base}/joined.tar.gz"), Some(exported.sha256.clone())), Some(2))
        .await
        .unwrap();
    let target_data = std::path::PathBuf::from(&target.workload.locate.data_dir);
    assert_eq!(std::fs::read(target_data.join("world/region/r.0.0.mca")).unwrap(), noise(11 * MIB, 7));
    left_nothing(&f.manager, "dst");
}

#[tokio::test]
async fn parts_too_few_for_the_archive_are_refused_before_anything_is_sent() {
    let f = fixture("[transfer]\nmax_put_mb = 1\n").await;
    let (base, objects) = store().await;
    let made = f.manager.ensure(id("big"), spec(), Precondition::None, Some(1)).await.unwrap();
    big_world(std::path::Path::new(&made.workload.locate.data_dir), 11);

    // Two parts of 5 MiB hold 10 MiB, short of the archive: refused with both sizes, so the
    // control plane can ask again with enough.
    let request: ExportRequest = serde_json::from_value(serde_json::json!({
        "url": format!("{base}/whole.tar.gz"),
        "parts": in_parts(&base, 5 * MIB, 2),
    }))
    .unwrap();
    let err = f.manager.export(id("big"), request, Some(1)).await.unwrap_err();
    assert!(
        matches!(err, NodeError::ArchiveTooLarge { size_bytes, limit_bytes }
            if size_bytes > 10 * MIB as u64 && limit_bytes == 10 * MIB as u64),
        "{err:?}"
    );
    // And without parts at all, one PUT's own limit says no.
    let err = f.manager.export(id("big"), export_to(format!("{base}/whole.tar.gz")), Some(1)).await.unwrap_err();
    assert!(matches!(err, NodeError::ArchiveTooLarge { limit_bytes, .. } if limit_bytes == MIB as u64), "{err:?}");
    assert!(objects.lock().unwrap().is_empty(), "nothing was sent");
    left_nothing(&f.manager, "big");
}

#[tokio::test]
async fn a_part_the_store_failed_to_take_is_sent_again() {
    blocklyd::tarball::set_download_idle(std::time::Duration::from_secs(1));
    let f = fixture("[transfer]\nmax_put_mb = 1\n").await;
    // A store that answers the first PUT of part 2 with a 503, then takes it; and whose first
    // answer to part 3 stops after its head, so the node never hears the end of it.
    let held: Objects = Arc::default();
    let failed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stalled = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/{key}",
            put({
                let (failed, stalled) = (failed.clone(), stalled.clone());
                move |State(o): State<Objects>, Path(key): Path<String>, body: Bytes| async move {
                    if key == "part-2" && failed.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        return (StatusCode::SERVICE_UNAVAILABLE, [(axum::http::header::ETAG, String::new())])
                            .into_response();
                    }
                    let tag = etag(&body);
                    o.lock().unwrap().insert(key.clone(), body.to_vec());
                    if key == "part-3" && stalled.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                        let never = futures_util::stream::pending::<Result<Bytes, std::io::Error>>();
                        let answer = axum::body::Body::from_stream(never);
                        return (StatusCode::OK, [(axum::http::header::ETAG, tag)], answer).into_response();
                    }
                    (StatusCode::OK, [(axum::http::header::ETAG, tag)]).into_response()
                }
            }),
        )
        .with_state(held.clone())
        .layer(axum::extract::DefaultBodyLimit::disable());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    let made = f.manager.ensure(id("big"), spec(), Precondition::None, Some(1)).await.unwrap();
    big_world(std::path::Path::new(&made.workload.locate.data_dir), 11);
    let request: ExportRequest = serde_json::from_value(serde_json::json!({
        "url": format!("{base}/whole.tar.gz"),
        "parts": in_parts(&base, 5 * MIB, 3),
    }))
    .unwrap();
    let exported = f.manager.export(id("big"), request, Some(1)).await.unwrap();
    assert_eq!(failed.load(std::sync::atomic::Ordering::SeqCst), 2, "refused once, taken the second time");
    assert_eq!(stalled.load(std::sync::atomic::Ordering::SeqCst), 2, "its answer stalled once, then came whole");
    assert_eq!(exported.parts.map(|p| p.len()), Some(3));
    assert_eq!(exported.sha256, hex::encode(sha2::Sha256::digest(joined(&held, 3))));
}

#[tokio::test]
async fn parts_the_store_cant_take_are_refused_and_a_small_archive_still_goes_whole() {
    let f = fixture("[transfer]\nmax_put_mb = 1\n").await;
    let (base, objects) = store().await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    std::fs::write(data.join("server.properties"), "level-name=world\n").unwrap();

    // S3 takes no part under 5 MiB but the last, and at most 10,000 of them.
    for parts in [in_parts(&base, MIB, 2), serde_json::json!({ "partSize": 5 * MIB, "urls": [] })] {
        let request: ExportRequest =
            serde_json::from_value(serde_json::json!({ "url": format!("{base}/w.tar.gz"), "parts": parts })).unwrap();
        let err = f.manager.export(id("w"), request, Some(1)).await.unwrap_err();
        assert!(matches!(err, NodeError::Invalid(_)), "{err:?}");
    }
    // A world one PUT carries goes in one PUT, whatever parts are offered.
    let request: ExportRequest = serde_json::from_value(serde_json::json!({
        "url": format!("{base}/w.tar.gz"),
        "parts": in_parts(&base, 5 * MIB, 2),
    }))
    .unwrap();
    let exported = f.manager.export(id("w"), request, Some(1)).await.unwrap();
    assert_eq!(exported.parts, None);
    let held = objects.lock().unwrap();
    assert_eq!(exported.sha256, hex::encode(sha2::Sha256::digest(&held["w.tar.gz"])));
    assert!(held.keys().all(|k| !k.starts_with("part-")));
}

#[tokio::test]
async fn a_snapshot_larger_than_one_put_uploads_in_parts() {
    let f = fixture("[transfer]\nmax_put_mb = 1\n").await;
    let (base, objects) = store().await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    big_world(std::path::Path::new(&made.workload.locate.data_dir), 6);
    f.manager.snapshot(id("w"), snapshot_request(SNAP, false), Some(1)).await.unwrap();
    let sent = f
        .manager
        .upload_snapshot(
            id("w"),
            blocklyd::ids::SnapshotId::parse(SNAP).unwrap(),
            serde_json::from_value(serde_json::json!({
                "url": format!("{base}/snap.tar.gz"),
                "parts": in_parts(&base, 5 * MIB, 2),
            }))
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(sent.parts.as_ref().map(Vec::len), Some(2));
    assert_eq!(sent.sha256, hex::encode(sha2::Sha256::digest(joined(&objects, 2))));
    assert!(names_in(&joined(&objects, 2)).iter().any(|n| n.ends_with("world/region/r.0.0.mca")));
}
