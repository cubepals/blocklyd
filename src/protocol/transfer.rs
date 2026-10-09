//! Moving a workload's data: export to a presigned URL (in parts when it is large), restore from
//! one or from a snapshot, local snapshots, and a snapshot's upload. It does not make or unpack
//! archives: that is `crate::tarball`. Stopping, fencing and deleting are in `lifecycle.rs`.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::FieldError;
use crate::ids::{SnapshotId, WorkloadId};

/// The largest archive one presigned PUT may carry. S3-compatible stores refuse a single PUT above
/// about 5 GiB; Cloudflare R2, the strictest, above 5 GiB less 5 MiB
/// (<https://developers.cloudflare.com/r2/platform/limits/>). A larger archive goes in parts, as
/// the store's multipart upload, when the request offers them (`PartsTarget`).
pub const MAX_SINGLE_PUT_BYTES: u64 = 5 * 1024 * 1024 * 1024 - 5 * 1024 * 1024;
/// The bounds S3 puts on a multipart upload: every part but the last is at least 5 MiB, none is
/// larger than one PUT may be, and there are at most 10,000 of them.
pub(crate) const MIN_PART_BYTES: u64 = 5 * 1024 * 1024;
pub(crate) const MAX_PART_BYTES: u64 = MAX_SINGLE_PUT_BYTES;
pub(crate) const MAX_PARTS: u64 = 10_000;

/// The URL without its query string: safe to log.
pub(crate) fn redact(url: &str) -> String {
    url.split('?').next().unwrap_or(url).to_owned()
}

/// `POST /v1/workloads/{id}/export`: the workload's data as a gzip tarball, PUT to a presigned
/// URL. The URL is used once, and never stored or logged.
#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ExportRequest {
    pub(crate) url: String,
    /// Sent with the PUT, as the store's presigning asked for (a content type, say).
    #[serde(default)]
    pub(crate) headers: BTreeMap<String, String>,
    /// The caller paused the workload's own saving (for Minecraft: `save-off`, `save-all
    /// flush`), so its files are consistent while it runs. Without it only a stopped workload
    /// exports.
    #[serde(default)]
    pub quiesced: bool,
    /// Names at the top of the data the archive leaves out: what the image makes again when it
    /// is missing (a server jar, the libraries it unpacks), so a move carries only the world.
    #[serde(default)]
    pub(crate) exclude: Vec<String>,
    /// Where the archive goes instead when it is larger than one PUT carries.
    #[serde(default)]
    pub(crate) parts: Option<PartsTarget>,
}

/// Top-level names a request may leave out of an archive: plain names, nothing that leads
/// elsewhere.
pub(crate) fn validate_exclude(exclude: &[String]) -> Result<(), Vec<FieldError>> {
    let bad = |problem: &str| Err(vec![FieldError { field: "exclude".into(), problem: problem.into() }]);
    if exclude.len() > 64 {
        return bad("at most 64 names");
    }
    for name in exclude {
        if name.is_empty() || name.len() > 255 || name == "." || name == ".." || name.contains(['/', '\0']) {
            return bad("each is one plain name at the top of the data");
        }
    }
    Ok(())
}

impl fmt::Debug for ExportRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExportRequest")
            .field("url", &redact(&self.url))
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("quiesced", &self.quiesced)
            .field("exclude", &self.exclude)
            .field("parts", &self.parts)
            .finish()
    }
}

/// An archive larger than one PUT carries goes to the store in parts, as its multipart upload:
/// the control plane begins the upload and presigns a URL for each part, and finishes it with the
/// parts the node reports. Every part but the last is `part_size` bytes, and the archive takes as
/// many of `urls`, from the first, as it needs; a URL left over is never called.
#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartsTarget {
    pub(crate) part_size: u64,
    pub(crate) urls: Vec<String>,
    /// Sent with every part's PUT.
    #[serde(default)]
    pub(crate) headers: BTreeMap<String, String>,
}

impl PartsTarget {
    pub(crate) fn validate(&self) -> Result<(), Vec<FieldError>> {
        let mut errors = Vec::new();
        let (min, max) = (MIN_PART_BYTES, MAX_PART_BYTES);
        if !(min..=max).contains(&self.part_size) {
            errors.push(FieldError {
                field: "parts.partSize".into(),
                problem: format!("between {min} and {max} bytes, as the store takes them"),
            });
        }
        let most = MAX_PARTS;
        if self.urls.is_empty() || self.urls.len() as u64 > most {
            errors.push(FieldError { field: "parts.urls".into(), problem: format!("1 to {most} of them") });
        }
        if errors.is_empty() { Ok(()) } else { Err(errors) }
    }

    /// How many bytes these parts carry at most.
    pub(crate) fn capacity(&self) -> u64 {
        self.part_size.saturating_mul(self.urls.len() as u64)
    }
}

impl fmt::Debug for PartsTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartsTarget")
            .field("part_size", &self.part_size)
            .field("urls", &self.urls.len())
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// A part the node put: its number, from 1, and the ETag the store answered with, which the
/// control plane needs to finish the upload.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct PutPart {
    pub number: u64,
    pub etag: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct ExportResponse {
    pub size_bytes: u64,
    /// Of the archive as uploaded: what a restore checks.
    pub sha256: String,
    /// `tar.gz`: paths relative to the data root, like every archive Blockly keeps.
    pub format: String,
    /// Directories, files and links in the archive.
    pub entries: u64,
    pub(crate) duration_ms: u64,
    /// Present when the archive went in parts: what finishes the upload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parts: Option<Vec<PutPart>>,
}

/// `POST /v1/workloads/{id}/restore`: replaces a stopped workload's data, from an archive at a
/// presigned URL or from one of its own snapshots on this node.
#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RestoreRequest {
    #[serde(default)]
    pub(crate) url: Option<String>,
    /// The archive's sha256; a download that doesn't match is refused before anything changes.
    #[serde(default)]
    pub(crate) sha256: Option<String>,
    /// A snapshot this node holds of the workload, instead of a URL.
    #[serde(default)]
    pub(crate) snapshot: Option<SnapshotId>,
}

/// Where a restore takes its data from: a `RestoreRequest` that names exactly one.
#[derive(Clone)]
pub(crate) enum RestoreSource {
    /// An archive at a presigned URL, checked against its sha256 when the request gives one.
    Url { url: String, sha256: Option<String> },
    /// One of the workload's snapshots on this node.
    Snapshot(SnapshotId),
}

impl TryFrom<RestoreRequest> for RestoreSource {
    type Error = Vec<FieldError>;

    fn try_from(request: RestoreRequest) -> Result<Self, Self::Error> {
        match (request.url, request.snapshot) {
            (Some(url), None) => Ok(Self::Url { url, sha256: request.sha256 }),
            (None, Some(snapshot)) if request.sha256.is_none() => Ok(Self::Snapshot(snapshot)),
            (None, Some(_)) => {
                Err(vec![FieldError { field: "sha256".into(), problem: "is for an archive's URL".into() }])
            }
            _ => Err(vec![FieldError { field: "url".into(), problem: "send a url or a snapshot, one of them".into() }]),
        }
    }
}

impl fmt::Debug for RestoreSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Url { url, sha256 } => {
                f.debug_struct("Url").field("url", &redact(url)).field("sha256", sha256).finish()
            }
            Self::Snapshot(snapshot) => f.debug_tuple("Snapshot").field(snapshot).finish(),
        }
    }
}

impl fmt::Debug for RestoreRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RestoreRequest")
            .field("url", &self.url.as_deref().map(redact))
            .field("sha256", &self.sha256)
            .field("snapshot", &self.snapshot)
            .finish()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct RestoreResponse {
    pub(crate) size_bytes: u64,
    /// The archive's, for a restore from a URL; none for a snapshot.
    pub sha256: Option<String>,
    pub(crate) entries: u64,
    /// Links, devices and the like, which are never unpacked.
    pub(crate) skipped: u64,
    pub(crate) unpacked_bytes: u64,
    /// Where the data it replaced went (the trash, for its retention).
    pub previous_data: Option<String>,
    pub(crate) duration_ms: u64,
}

/// `POST /v1/workloads/{id}/snapshots`: a copy of the workload's data on this node, kept beside
/// it. Sharing blocks with the data where the filesystem can, it takes moments and no space
/// until the data changes. The id is the caller's: asking again with the same one returns the
/// snapshot already made.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SnapshotRequest {
    pub(crate) id: SnapshotId,
    /// As for an export: the caller paused the workload's saving, so a running one may be copied.
    #[serde(default)]
    pub(crate) quiesced: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum Method {
    /// The copy shares the source's blocks until either changes: instant, and free until then.
    Reflink,
    /// Every byte was copied.
    Copy,
}

impl Method {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reflink => "reflink",
            Self::Copy => "copy",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SnapshotView {
    pub id: SnapshotId,
    pub workload: WorkloadId,
    /// The epoch of the copy it was taken from.
    pub epoch: Option<u64>,
    pub created_at: String,
    /// The files' bytes. With shared blocks, most of them aren't new on disk.
    pub size_bytes: u64,
    pub files: u64,
    pub method: Method,
    /// The workload was running, with its saving paused, when it was taken.
    pub quiesced: bool,
    pub spec_digest: String,
    pub duration_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SnapshotResponse {
    /// False when a snapshot with this id already existed; it is returned as it was made.
    pub created: bool,
    pub snapshot: SnapshotView,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SnapshotList {
    pub snapshots: Vec<SnapshotView>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase")]
pub struct SnapshotDeleteResponse {
    pub existed: bool,
}

/// `POST /v1/workloads/{id}/snapshots/{snapshot}/upload`: the snapshot as a gzip tarball, PUT to
/// a presigned URL. The snapshot doesn't change, so this needs no quiet and no epoch.
#[derive(Clone, Deserialize, Serialize)]
#[cfg_attr(test, derive(schemars::JsonSchema))]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UploadRequest {
    pub(crate) url: String,
    #[serde(default)]
    pub(crate) headers: BTreeMap<String, String>,
    /// Where the archive goes instead when it is larger than one PUT carries.
    #[serde(default)]
    pub(crate) parts: Option<PartsTarget>,
}

impl fmt::Debug for UploadRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UploadRequest")
            .field("url", &redact(&self.url))
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .field("parts", &self.parts)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signatures_never_reach_a_log() {
        assert_eq!(
            redact("https://r2.example/archives/a.tar.gz?X-Amz-Signature=secret&X-Amz-Credential=key"),
            "https://r2.example/archives/a.tar.gz"
        );
    }
}
