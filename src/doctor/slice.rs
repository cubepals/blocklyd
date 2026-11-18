// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Whether workloads run in their own slice with its weight (`workloads.cgroup_parent`,
//! `deploy/blockly.slice`): Docker must place them with the systemd cgroup driver, and the slice's
//! unit must be installed, or they share system.slice with blocklyd by count and a full host leaves
//! blocklyd a sliver of the CPU. Warnings only: workloads run either way.

use super::check::Check;

/// The check for this host: its installed unit, and the driver Docker reported (None when Docker
/// didn't answer).
pub(crate) fn installed(slice: &str, driver: Option<&str>) -> Check {
    let unit = (!slice.is_empty())
        .then(|| std::fs::read_to_string(std::path::Path::new("/etc/systemd/system").join(slice)).ok())
        .flatten();
    check(slice, driver, unit.as_deref())
}

/// `slice` is the configured name (empty: none asked for), `driver` Docker's cgroup driver,
/// `unit` the installed unit's text, None when there is none.
fn check(slice: &str, driver: Option<&str>, unit: Option<&str>) -> Check {
    if slice.is_empty() {
        return Check::pass("workloads.slice", "workloads run in Docker's default cgroup, as configured");
    }
    if let Some(driver) = driver.filter(|d| *d != "systemd") {
        return Check::warn(
            "workloads.slice",
            format!("Docker's cgroup driver is {driver}, so workloads can't run in {slice}"),
            "Use systemd's cgroup driver (Docker's default on a systemd host with cgroup v2)",
        );
    }
    match unit {
        Some(text) if text.lines().any(|l| l.trim_start().starts_with("CPUWeight=")) => {
            Check::pass("workloads.slice", format!("workloads run in {slice}, with its CPU weight"))
        }
        _ => Check::warn(
            "workloads.slice",
            format!("{slice} isn't installed with a CPU weight: workloads share system.slice with blocklyd by count"),
            format!("Install the release's blockly.slice as /etc/systemd/system/{slice}, then systemctl daemon-reload"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::join::SLICE_FILE;
    use crate::doctor::Status;

    #[test]
    fn the_installed_slice_on_systemd_passes_and_anything_short_of_it_warns() {
        assert_eq!(check("blockly.slice", Some("systemd"), Some(SLICE_FILE)).status, Status::Pass);
        assert_eq!(check("", Some("cgroupfs"), None).status, Status::Pass);
        assert_eq!(check("blockly.slice", Some("cgroupfs"), Some(SLICE_FILE)).status, Status::Warn);
        assert_eq!(check("blockly.slice", Some("systemd"), None).status, Status::Warn);
        assert_eq!(check("blockly.slice", Some("systemd"), Some("[Slice]\n")).status, Status::Warn);
    }
}
