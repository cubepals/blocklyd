// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! A restore that unpacks as it downloads, given archives in each codec it reads (gzip, zstd)
//! that are sound, or damaged past what tar reads: a wrong codec checksum, cut short, padded with
//! bytes after the codec's end, or not the archive the sha256 names. A sound one replaces the
//! data; every damaged one is refused, and the data is just as it was, with nothing left beside it.

mod support;

use std::io::Write;

use blocklyd::manager::{NodeError, Precondition};
use blocklyd::protocol::RestoreRequest;
use sha2::Digest;
use support::{fixture, id, spec};

/// A world's tarball, padded as GNU tar pads it, so tar stops reading well before the codec does.
fn tarball() -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (name, data) in [("server.properties", &b"level-name=world\n"[..]), ("world/level.dat", &b"restored"[..])] {
        let mut header = tar::Header::new_gnu();
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        builder.append_data(&mut header, name, data).unwrap();
    }
    let mut tar = builder.into_inner().unwrap();
    tar.resize(tar.len().next_multiple_of(10_240), 0);
    tar
}

fn gzipped(data: &[u8]) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gz.write_all(data).unwrap();
    gz.finish().unwrap()
}

/// One zstd frame with its content checksum, as Blockly's writers make them.
fn zstd(data: &[u8]) -> Vec<u8> {
    let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
    encoder.include_checksum(true).unwrap();
    encoder.write_all(data).unwrap();
    encoder.finish().unwrap()
}

/// A store that serves `bytes` once, at the URL it returns.
async fn serving(bytes: Vec<u8>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/w", listener.local_addr().unwrap());
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = socket.read(&mut [0u8; 4096]).await;
        let ok = axum::http::StatusCode::OK;
        let head = format!(
            "HTTP/1.1 {} {}\r\ncontent-length: {}\r\n\r\n",
            ok.as_u16(),
            ok.canonical_reason().unwrap_or_default(),
            bytes.len()
        );
        socket.write_all(head.as_bytes()).await.unwrap();
        socket.write_all(&bytes).await.unwrap();
        socket.flush().await.unwrap();
    });
    url
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(sha2::Sha256::digest(bytes))
}

/// Restores `bytes`, named by `named` (its sha256, or another), into a workload whose data holds
/// `keep.txt`: what the restore answered.
async fn restored(bytes: Vec<u8>, named: String) -> (Result<(), NodeError>, std::path::PathBuf, support::Fixture) {
    let f = fixture("").await;
    let made = f.manager.ensure(id("w"), spec(), Precondition::None, Some(1)).await.unwrap();
    let data = std::path::PathBuf::from(&made.workload.locate.data_dir);
    std::fs::write(data.join("keep.txt"), b"mine").unwrap();
    let url = serving(bytes).await;
    let request: RestoreRequest = serde_json::from_value(serde_json::json!({ "url": url, "sha256": named })).unwrap();
    let answer = f.manager.restore(id("w"), request, Some(1)).await.map(|_| ());
    (answer, data, f)
}

/// The data as it was, and nothing of the restore left beside it.
fn untouched(data: &std::path::Path, f: &support::Fixture) {
    assert_eq!(std::fs::read(data.join("keep.txt")).unwrap(), b"mine");
    assert!(!data.join("world/level.dat").exists());
    assert!(!f.manager.store.restoring_dir(&id("w")).exists());
}

/// Each codec, by name, with its sound archive.
fn codecs() -> [(&'static str, Vec<u8>); 2] {
    [("gzip", gzipped(&tarball())), ("zstd", zstd(&tarball()))]
}

#[tokio::test]
async fn a_sound_archive_in_either_codec_replaces_the_data() {
    for (codec, archive) in codecs() {
        let (answer, data, _f) = restored(archive.clone(), sha256(&archive)).await;
        answer.unwrap_or_else(|e| panic!("{codec}: {e:?}"));
        assert_eq!(std::fs::read(data.join("world/level.dat")).unwrap(), b"restored", "{codec}");
    }
}

#[tokio::test]
async fn a_wrong_codec_checksum_is_refused_and_changes_nothing() {
    for (codec, mut archive) in codecs() {
        // The codec's checksum is its last four bytes in both: gzip's CRC32 is followed by the
        // length, so the length's last byte is changed there, which gzip checks as well.
        let last = archive.len() - 1;
        archive[last] ^= 0xff;
        let (answer, data, f) = restored(archive.clone(), sha256(&archive)).await;
        assert!(matches!(answer, Err(NodeError::InvalidArchive(_))), "{codec}: {answer:?}");
        untouched(&data, &f);
    }
}

#[tokio::test]
async fn an_archive_cut_short_is_refused_and_changes_nothing() {
    for (codec, mut archive) in codecs() {
        archive.truncate(archive.len() - 6);
        let (answer, data, f) = restored(archive.clone(), sha256(&archive)).await;
        assert!(matches!(answer, Err(NodeError::InvalidArchive(_))), "{codec}: {answer:?}");
        untouched(&data, &f);
    }
}

#[tokio::test]
async fn bytes_after_the_codecs_end_are_refused_and_change_nothing() {
    for (codec, mut archive) in codecs() {
        archive.extend_from_slice(&[0u8; 64]);
        let (answer, data, f) = restored(archive.clone(), sha256(&archive)).await;
        assert!(matches!(answer, Err(NodeError::InvalidArchive(_))), "{codec}: {answer:?}");
        untouched(&data, &f);
    }
}

#[tokio::test]
async fn an_archive_the_sha256_doesnt_name_is_refused_and_changes_nothing() {
    for (codec, archive) in codecs() {
        let (answer, data, f) = restored(archive, "00".repeat(32)).await;
        assert!(matches!(answer, Err(NodeError::ChecksumMismatch { .. })), "{codec}: {answer:?}");
        untouched(&data, &f);
    }
}
