//! Facts about this host and this process, read straight from Linux (/proc, statvfs).

use std::path::Path;

#[derive(Clone, Debug, Default)]
pub struct HostFacts {
    pub hostname: String,
    pub kernel: String,
    pub cpus: u32,
    pub load_average: Option<[f64; 3]>,
    pub memory_total_bytes: Option<u64>,
    pub memory_available_bytes: Option<u64>,
}

fn read(path: &str) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_owned())
}

/// `/proc/meminfo` values are in kB.
fn meminfo(field: &str) -> Option<u64> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    text.lines()
        .find(|l| l.starts_with(field) && l[field.len()..].starts_with(':'))
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|kb| kb.parse::<u64>().ok())
        .map(|kb| kb * 1024)
}

pub fn facts() -> HostFacts {
    let load_average = read("/proc/loadavg").and_then(|l| {
        let mut parts = l.split_whitespace().take(3).map(|p| p.parse::<f64>().ok());
        Some([parts.next()??, parts.next()??, parts.next()??])
    });
    HostFacts {
        hostname: read("/proc/sys/kernel/hostname").unwrap_or_default(),
        kernel: read("/proc/sys/kernel/osrelease").unwrap_or_default(),
        cpus: std::thread::available_parallelism().map_or(1, |n| n.get() as u32),
        load_average,
        memory_total_bytes: meminfo("MemTotal"),
        memory_available_bytes: meminfo("MemAvailable"),
    }
}

/// The kernel's boot id: new on every boot, so a reboot can be told from a daemon restart.
pub fn boot_id() -> Option<String> {
    read("/proc/sys/kernel/random/boot_id").filter(|id| !id.is_empty())
}

/// (total, available to unprivileged writers) bytes of the filesystem holding `path`.
pub fn disk(path: &Path) -> Option<(u64, u64)> {
    let stat = rustix::fs::statvfs(path).ok()?;
    Some((stat.f_blocks * stat.f_frsize, stat.f_bavail * stat.f_frsize))
}

#[derive(Clone, Debug, Default)]
pub struct ProcessFacts {
    pub rss_bytes: Option<u64>,
    pub cpu_seconds: Option<f64>,
}

/// This process's resident memory and CPU time: blocklyd's own footprint.
pub fn process() -> ProcessFacts {
    let page = rustix::param::page_size() as u64;
    let rss_bytes = read("/proc/self/statm")
        .and_then(|s| s.split_whitespace().nth(1).and_then(|p| p.parse::<u64>().ok()))
        .map(|pages| pages * page);
    let ticks = rustix::param::clock_ticks_per_second() as f64;
    let cpu_seconds = read("/proc/self/stat").and_then(|stat| {
        let after = &stat[stat.rfind(')')? + 1..];
        let fields: Vec<&str> = after.split_whitespace().collect();
        // After the command name: state is field 3 (index 0 here); utime 14 and stime 15.
        let utime: f64 = fields.get(11)?.parse().ok()?;
        let stime: f64 = fields.get(12)?.parse().ok()?;
        Some((utime + stime) / ticks)
    });
    ProcessFacts { rss_bytes, cpu_seconds }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_this_host() {
        let f = facts();
        assert!(f.cpus >= 1);
        assert!(f.memory_total_bytes.unwrap() > 0);
        assert!(disk(Path::new("/")).is_some());
        let p = process();
        assert!(p.rss_bytes.unwrap() > 0);
        assert!(p.cpu_seconds.is_some());
    }
}
