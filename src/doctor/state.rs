//! Whether the state directory can be used: blocklyd can hand directories in it to the workloads'
//! user (the one state check that stops it from starting), no other blocklyd holds it, files in
//! it can share blocks, and its disk has room above the floor.

use std::collections::HashSet;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

use super::check::Check;
use crate::config::Config;
use crate::protocol::Proto;
use crate::store::{Store, StoreError};

pub(crate) fn checks(config: &Config, config_path: &Path) -> Vec<Check> {
    let dir = &config.state_dir;
    let metadata = match std::fs::metadata(dir) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return vec![Check::warn(
                "state_dir",
                format!("{} doesn't exist yet; blocklyd makes it when it starts", dir.display()),
                "Nothing needed, or run blocklyd doctor --fix to make it now",
            )];
        }
        Err(e) => {
            return vec![
                Check::fail(
                    "state_dir",
                    format!("can't look at {}: {e}", dir.display()),
                    "Check its parent's permissions",
                )
                .gating(),
            ];
        }
    };
    if !metadata.is_dir() {
        return vec![
            Check::fail("state_dir", format!("{} isn't a directory", dir.display()), "Move it aside, or set state_dir")
                .gating(),
        ];
    }
    let store = Store::existing(dir);
    vec![
        Check::pass("state_dir", format!("{} (mode {:o})", dir.display(), metadata.permissions().mode() & 0o7777)),
        ownable(&store, config.data_owner_ids()),
        lock(&store, config_path),
        reflink(dir),
        disk(dir, config.capacity.min_free_disk_mb),
    ]
}

fn ownable(store: &Store, owner: (u32, u32)) -> Check {
    match store.check_ownable(owner) {
        Ok(()) => {
            Check::pass("state_dir.ownable", format!("blocklyd can give data directories to {}:{}", owner.0, owner.1))
        }
        Err(e) => Check::fail(
            "state_dir.ownable",
            e.to_string(),
            "Run blocklyd as root (it needs CAP_CHOWN), or set workloads.data_owner to its own uid:gid",
        )
        .gating(),
    }
}

/// A lock held by the blocklyd this configuration runs is how it should be; any other holder
/// keeps blocklyd from starting.
fn lock(store: &Store, config_path: &Path) -> Check {
    let path = match store.lock() {
        Ok(_held) => return Check::pass("state_dir.lock", "no other blocklyd holds it"),
        Err(StoreError::Locked(path)) => path,
        Err(e) => return Check::warn("state_dir.lock", e.to_string(), "Check the state directory's permissions"),
    };
    let holder = lock_holder(&path);
    if let Some((pid, args)) = &holder
        && serves(args, config_path)
    {
        return Check::pass("state_dir.lock", format!("held by the running blocklyd (pid {pid})"));
    }
    let who = holder.map_or("another process".to_owned(), |(pid, args)| format!("pid {pid} ({})", args.join(" ")));
    Check::fail(
        "state_dir.lock",
        format!("{who} holds {}; one blocklyd per state directory", path.display()),
        "Stop the other blocklyd, or give this one its own state_dir",
    )
}

/// The pid holding a flock on `path`, from /proc/locks, and its command line.
fn lock_holder(path: &Path) -> Option<(u32, Vec<String>)> {
    let metadata = std::fs::metadata(path).ok()?;
    let (major, minor) = (rustix::fs::major(metadata.dev()), rustix::fs::minor(metadata.dev()));
    let wanted = format!("{major:02x}:{minor:02x}:{}", metadata.ino());
    let locks = std::fs::read_to_string("/proc/locks").ok()?;
    // `1: FLOCK  ADVISORY  WRITE 1234 00:2e:5678 0 EOF`
    let pid: u32 = locks.lines().find_map(|line| {
        let fields: Vec<&str> = line.split_whitespace().collect();
        (fields.get(1) == Some(&"FLOCK") && fields.get(5) == Some(&wanted.as_str()))
            .then(|| fields.get(4)?.parse().ok())
            .flatten()
    })?;
    let cmdline = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap_or_default();
    let args = cmdline.split(|b| *b == 0).filter(|a| !a.is_empty()).map(|a| String::from_utf8_lossy(a).into_owned());
    Some((pid, args.collect()))
}

/// Whether a command line is `blocklyd serve` with this configuration.
fn serves(args: &[String], config_path: &Path) -> bool {
    let is_blocklyd = args.first().is_some_and(|a| Path::new(a).file_name().is_some_and(|n| n == "blocklyd"));
    if !is_blocklyd || !args.iter().any(|a| a == "serve") {
        return false;
    }
    let configured = args
        .iter()
        .position(|a| a == "--config")
        .and_then(|i| args.get(i + 1).cloned())
        .or_else(|| args.iter().find_map(|a| a.strip_prefix("--config=").map(str::to_owned)))
        .unwrap_or_else(|| "/etc/blocklyd/blocklyd.toml".into());
    let canonical = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_owned());
    canonical(Path::new(&configured)) == canonical(config_path)
}

fn reflink(dir: &Path) -> Check {
    if crate::tree::reflink_supported(dir) {
        return Check::pass("state_dir.reflink", "files can share blocks: snapshots and restores are cheap");
    }
    Check::warn(
        "state_dir.reflink",
        "this filesystem can't share blocks (ext4 can't): snapshots and restores make full copies",
        "Nothing needed; XFS (reflink=1) or Btrfs under state_dir would make them cheap",
    )
}

fn disk(dir: &Path, min_free_mb: u64) -> Check {
    let Some((_, available)) = crate::host::disk(dir) else {
        return Check::warn("state_dir.disk", format!("can't read free space under {}", dir.display()), "Check df");
    };
    let free_mb = available / (1024 * 1024);
    if free_mb >= min_free_mb {
        return Check::pass("state_dir.disk", format!("{free_mb} MB free, above the {min_free_mb} MB floor"));
    }
    Check::warn(
        "state_dir.disk",
        format!("{free_mb} MB free, below the {min_free_mb} MB floor: blocklyd refuses new servers"),
        "Free space on that disk, or lower capacity.min_free_disk_mb",
    )
}

/// Ports this host's workloads hold, from their records: in use by blocklyd itself, not taken.
pub(crate) fn held_ports(state_dir: &Path) -> HashSet<(Proto, u16)> {
    let Ok((records, _)) = Store::existing(state_dir).load_all() else { return HashSet::new() };
    records.iter().flat_map(|r| r.ports.iter().map(|p| (p.protocol, p.host_port))).collect()
}

/// Whether this state directory holds any workload (`--fix` leaves such a host alone).
pub(crate) fn has_workloads(state_dir: &Path) -> bool {
    let workloads: PathBuf = Store::existing(state_dir).workloads_dir();
    std::fs::read_dir(workloads).is_ok_and(|mut entries| entries.next().is_some())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doctor::Status;

    fn config(state_dir: &Path) -> Config {
        let (uid, gid) = (rustix::process::getuid().as_raw(), rustix::process::getgid().as_raw());
        let toml = format!(
            "node_id = \"n1\"\ndeployment_id = \"dev\"\nstate_dir = \"{}\"\n[api]\nlisten = \"127.0.0.1:7443\"\n\
             [api.tls]\ncert = \"c\"\nkey = \"k\"\nclient_ca = \"ca\"\nallowed_clients = [\"x\"]\n\
             [workloads]\nuser = \"1000:1000\"\ndata_owner = \"{}:{}\"\n[capacity]\nmin_free_disk_mb = 1\n",
            state_dir.display(),
            if uid == 0 { 1000 } else { uid },
            if gid == 0 { 1000 } else { gid },
        );
        toml::from_str(&toml).unwrap()
    }

    fn by_name<'a>(checks: &'a [Check], name: &str) -> &'a Check {
        checks.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("no {name} in {checks:?}"))
    }

    #[test]
    fn a_missing_state_dir_is_a_warning_blocklyd_fixes_itself() {
        let dir = tempfile::tempdir().unwrap();
        let checks = checks(&config(&dir.path().join("state")), Path::new("/etc/blocklyd/blocklyd.toml"));
        assert_eq!(checks.len(), 1);
        assert_eq!((checks[0].status, checks[0].gates_start), (Status::Warn, false));
    }

    #[test]
    fn a_usable_state_dir_passes_and_reflink_is_never_worse_than_a_warning() {
        let dir = tempfile::tempdir().unwrap();
        Store::open(dir.path()).unwrap();
        let checks = checks(&config(dir.path()), Path::new("/etc/blocklyd/blocklyd.toml"));
        for name in ["state_dir", "state_dir.ownable", "state_dir.lock", "state_dir.disk"] {
            assert_eq!(by_name(&checks, name).status, Status::Pass, "{name}: {checks:?}");
        }
        assert_ne!(by_name(&checks, "state_dir.reflink").status, Status::Fail);
    }

    #[test]
    fn a_file_where_the_state_dir_goes_stops_blocklyd() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("state");
        std::fs::write(&file, "").unwrap();
        let checks = checks(&config(&file), Path::new("/etc/blocklyd/blocklyd.toml"));
        assert_eq!((checks[0].status, checks[0].gates_start), (Status::Fail, true));
    }

    #[test]
    fn a_data_owner_blocklyd_cant_give_to_fails_and_stops_blocklyd() {
        if rustix::process::getuid().is_root() {
            return; // root can give anything to anyone
        }
        let dir = tempfile::tempdir().unwrap();
        let check = ownable(&Store::open(dir.path()).unwrap(), (4242, 4242));
        assert_eq!((check.status, check.gates_start), (Status::Fail, true));
    }

    #[test]
    fn a_lock_held_by_something_other_than_this_blocklyd_fails() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let _held = store.lock().unwrap();
        let check = lock(&store, Path::new("/etc/blocklyd/blocklyd.toml"));
        assert_eq!((check.status, check.gates_start), (Status::Fail, false));
        assert!(check.detail.contains(&format!("pid {}", std::process::id())), "{}", check.detail);
    }

    #[test]
    fn serving_this_config_is_this_blocklyd() {
        let args = |a: &str| a.split(' ').map(str::to_owned).collect::<Vec<_>>();
        let config = Path::new("/etc/blocklyd/blocklyd.toml");
        assert!(serves(&args("/usr/local/bin/blocklyd serve --config /etc/blocklyd/blocklyd.toml"), config));
        assert!(serves(&args("blocklyd serve"), config));
        assert!(!serves(&args("/usr/local/bin/blocklyd serve --config /srv/other.toml"), config));
        assert!(!serves(&args("/usr/bin/flock blocklyd serve"), config));
    }

    #[test]
    fn disk_below_the_floor_warns() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(disk(dir.path(), 1).status, Status::Pass);
        assert_eq!(disk(dir.path(), u64::MAX / (1024 * 1024)).status, Status::Warn);
    }

    #[test]
    fn workloads_and_their_ports_come_from_records() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!has_workloads(dir.path()));
        assert!(held_ports(dir.path()).is_empty());
        Store::open(dir.path()).unwrap();
        std::fs::create_dir(dir.path().join("workloads/w")).unwrap();
        assert!(has_workloads(dir.path()));
    }
}
