//! What a world's data costs to snapshot, archive and restore on this host's disk: the work a
//! backup, a move and a restore do, without Docker or the network. Ignored by default:
//!
//!     cargo test --release --test storage_bench -- --ignored --nocapture
//!
//! BENCH_DIR (default: the system temp dir) picks the filesystem; BENCH_WORLD_MB (default 1024)
//! the world's size. The world is shaped like a Minecraft one: region files of about 1 MB of
//! incompressible bytes (chunks are compressed already), and a few thousand small files.

use std::fs;
use std::io::Write;
use std::path::Path;
use std::time::Instant;

fn world(root: &Path, megabytes: usize) {
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut noise = |len: usize| {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            out.extend_from_slice(&seed.to_le_bytes());
        }
        out.truncate(len);
        out
    };
    for dimension in ["world/region", "world/DIM-1/region", "world/DIM1/region"] {
        fs::create_dir_all(root.join(dimension)).unwrap();
    }
    for i in 0..megabytes {
        let dimension = ["world/region", "world/DIM-1/region", "world/DIM1/region"][i % 3];
        let mut file = fs::File::create(root.join(dimension).join(format!("r.{i}.0.mca"))).unwrap();
        file.write_all(&noise(1024 * 1024)).unwrap();
    }
    fs::create_dir_all(root.join("world/playerdata")).unwrap();
    for i in 0..2000 {
        fs::write(root.join(format!("world/playerdata/{i:08x}.dat")), noise(2048)).unwrap();
    }
    fs::write(root.join("server.properties"), "level-name=world\n").unwrap();
    fs::create_dir_all(root.join("libraries")).unwrap();
    fs::write(root.join("libraries/server.jar"), noise(50 * 1024 * 1024)).unwrap();
}

fn used(path: &Path) -> u64 {
    blocklyd::tree::disk_usage(path).unwrap_or(0)
}

fn free(path: &Path) -> u64 {
    blocklyd::host::disk(path).map_or(0, |(_, available)| available)
}

#[test]
#[ignore = "a benchmark: run it on purpose, on the disk it should measure"]
fn snapshot_archive_and_restore_costs() {
    let megabytes: usize = std::env::var("BENCH_WORLD_MB").ok().and_then(|v| v.parse().ok()).unwrap_or(1024);
    let base = std::env::var("BENCH_DIR").map(Into::into).unwrap_or_else(|_| std::env::temp_dir());
    let dir = tempfile::tempdir_in(base).unwrap();
    let data = dir.path().join("data");
    fs::create_dir(&data).unwrap();
    world(&data, megabytes);
    let me = (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw());
    let mb = |bytes: u64| bytes as f64 / (1024.0 * 1024.0);
    let rate = |bytes: u64, secs: f64| mb(bytes) / secs;
    println!("world: {:.0} MB on disk, reflink: {}", mb(used(&data)), blocklyd::tree::reflink_supported(dir.path()));

    let before = free(dir.path());
    let started = Instant::now();
    let copied = blocklyd::tree::copy_tree(&data, &dir.path().join("snapshot"), None, &[], &mut |_| Ok(())).unwrap();
    let took = started.elapsed().as_secs_f64();
    println!(
        "snapshot: {:.2} s ({:.0} MB/s), method {}, {} files, disk taken {:.0} MB",
        took,
        rate(copied.walked.bytes, took),
        copied.method.as_str(),
        copied.walked.files,
        mb(before.saturating_sub(free(dir.path())))
    );

    let started = Instant::now();
    let packed =
        blocklyd::tarball::pack(&dir.path().join("snapshot"), &dir.path().join("all.tar.gz"), &[], &mut |_| Ok(()))
            .unwrap();
    let took = started.elapsed().as_secs_f64();
    println!(
        "archive (everything): {:.2} s ({:.0} MB/s in), {:.0} MB out, {} entries",
        took,
        rate(copied.walked.bytes, took),
        mb(packed.size_bytes),
        packed.entries
    );

    let started = Instant::now();
    let moved =
        blocklyd::tarball::pack(&data, &dir.path().join("move.tar.gz"), &["libraries".to_owned()], &mut |_| Ok(()))
            .unwrap();
    let took = started.elapsed().as_secs_f64();
    println!("archive (a move, without libraries/): {:.2} s, {:.0} MB out", took, mb(moved.size_bytes));

    let started = Instant::now();
    let unpacked =
        blocklyd::tarball::unpack(&dir.path().join("all.tar.gz"), &dir.path().join("restored"), me, &mut |_| Ok(()))
            .unwrap();
    let took = started.elapsed().as_secs_f64();
    println!("restore from an archive: {:.2} s ({:.0} MB/s out)", took, rate(unpacked.bytes, took));

    let started = Instant::now();
    blocklyd::tree::copy_tree(
        &dir.path().join("snapshot"),
        &dir.path().join("from-snapshot"),
        Some(me),
        &[],
        &mut |_| Ok(()),
    )
    .unwrap();
    let took = started.elapsed().as_secs_f64();
    println!("restore from a snapshot on the node: {:.2} s", took);
}
