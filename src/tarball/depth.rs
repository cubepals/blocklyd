// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! How deep a restored archive may nest: no deeper than blocklyd's walks go (`tree::MAX_DEPTH`),
//! or what it holds couldn't be measured, snapshotted or held to its quota afterwards. Not for
//! the rest of what a restore refuses (`tarball.rs`).

use std::path::Path;

use super::UnpackError;
use crate::tree::MAX_DEPTH;

/// Refuses an entry at `relative` (as the archive names it, `raw`) nested deeper than a walk
/// enters: the folders it lies in, and itself if it is a `directory`, as the walks count them
/// going in. A file may sit in the deepest folder a walk enters.
pub(super) fn within(raw: &Path, relative: &Path, directory: bool) -> Result<(), UnpackError> {
    let parts = relative.components().count();
    let folders = if directory { parts } else { parts.saturating_sub(1) };
    if folders < MAX_DEPTH {
        return Ok(());
    }
    Err(UnpackError::TooLarge(format!(
        "{} is nested {folders} folders deep, and a world may nest at most {}",
        raw.display(),
        MAX_DEPTH - 1
    )))
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};

    use flate2::Compression;
    use flate2::write::GzEncoder;

    use super::super::{pack, unpack};
    use super::*;

    fn me() -> (u32, u32) {
        (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw())
    }

    #[test]
    fn a_tree_nested_deeper_than_a_walk_enters_is_refused() {
        // One entry under `folders` folders: a file, or a folder itself when `file` is empty.
        let unpacked = |folders: usize, file: &str| {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("a.tar.gz");
            let mut builder = tar::Builder::new(GzEncoder::new(File::create(&path).unwrap(), Compression::fast()));
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(if file.is_empty() { tar::EntryType::Directory } else { tar::EntryType::Regular });
            header.set_mode(0o755);
            header.set_size(0);
            // A long name as GNU tar writes it, past the header's 100 bytes.
            builder.append_data(&mut header, format!("{}{file}", "d/".repeat(folders)), &b""[..]).unwrap();
            builder.into_inner().unwrap().finish().unwrap();
            unpack(&path, &dir.path().join("out"), me(), &mut |_| Ok(()))
        };
        for refused in [unpacked(MAX_DEPTH, "level.dat").unwrap_err(), unpacked(MAX_DEPTH, "").unwrap_err()] {
            assert!(matches!(refused, UnpackError::TooLarge(_)), "{refused}");
            assert!(refused.to_string().contains("nested 128 folders deep"), "{refused}");
        }
        unpacked(MAX_DEPTH - 1, "level.dat").unwrap();
        unpacked(MAX_DEPTH - 1, "").unwrap();
    }

    #[test]
    fn what_pack_makes_at_the_deepest_unpacks() {
        let src = tempfile::tempdir().unwrap();
        let deepest = src.path().join("d/".repeat(MAX_DEPTH - 1));
        fs::create_dir_all(&deepest).unwrap();
        fs::write(deepest.join("level.dat"), b"level").unwrap();
        let out = tempfile::tempdir().unwrap();
        let archive = out.path().join("w.tar.gz");
        pack(src.path(), &archive, &[], &mut |_| Ok(())).unwrap();
        let dest = out.path().join("back");
        unpack(&archive, &dest, me(), &mut |_| Ok(())).unwrap();
        assert_eq!(fs::read(dest.join("d/".repeat(MAX_DEPTH - 1)).join("level.dat")).unwrap(), b"level");
    }
}
