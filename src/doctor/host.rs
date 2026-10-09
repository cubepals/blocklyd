//! Whether the host has what blocklyd assumes of it besides Docker: a machine id of its own, and a
//! synchronised clock (leases, certificates and heartbeats all read it).

use super::check::Check;

/// `machine_id` is /etc/machine-id's contents, None when it can't be read.
pub(crate) fn machine_id(machine_id: Option<&str>) -> Check {
    match machine_id.map(str::trim) {
        Some(id) if !id.is_empty() && id != "uninitialized" => Check::pass("machine_id", "/etc/machine-id is set"),
        _ => Check::warn(
            "machine_id",
            "/etc/machine-id is missing or empty, as on a copied image: two hosts with one identity are refused by \
             the control plane (node.duplicate_identity)",
            "Run systemd-machine-id-setup (or reboot once) before this node enrolls",
        ),
    }
}

pub(crate) fn clock() -> Check {
    clock_verdict(synchronised())
}

fn clock_verdict(synchronised: Option<bool>) -> Check {
    match synchronised {
        Some(true) => Check::pass("clock", "the clock is synchronised"),
        Some(false) => Check::warn(
            "clock",
            "the clock isn't synchronised: leases and certificates depend on it",
            "Turn on time synchronisation (timedatectl set-ntp true)",
        ),
        None => Check::warn(
            "clock",
            "couldn't tell whether the clock is synchronised",
            "Check that timedatectl show reports NTPSynchronized=yes",
        ),
    }
}

/// The kernel's own answer (adjtimex, read only) where the process may ask; systemd's otherwise.
/// The unit's ProtectClock refuses adjtimex, so under systemd the answer is timedatectl's.
fn synchronised() -> Option<bool> {
    // SAFETY: adjtimex with modes 0 only reads, into a zeroed struct this function owns.
    let (state, status) = unsafe {
        let mut timex: libc::timex = std::mem::zeroed();
        let state = libc::adjtimex(&mut timex);
        (state, timex.status)
    };
    if state >= 0 {
        return Some(state != libc::TIME_ERROR && status & libc::STA_UNSYNC == 0);
    }
    let out = std::process::Command::new("timedatectl")
        .args(["show", "--property=NTPSynchronized", "--value"])
        .output()
        .ok()?;
    match String::from_utf8_lossy(&out.stdout).trim() {
        "yes" => Some(true),
        "no" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::doctor::Status;

    #[test]
    fn a_machine_id_of_its_own_passes() {
        assert_eq!(machine_id(Some("4f1c2a9e0b5d4c3e8f7a6b5c4d3e2f1a\n")).status, Status::Pass);
        for copied in [None, Some(""), Some("\n"), Some("uninitialized\n")] {
            let check = machine_id(copied);
            assert_eq!(check.status, Status::Warn);
            assert!(check.detail.contains("node.duplicate_identity"));
        }
    }

    #[test]
    fn an_unsynchronised_clock_warns() {
        assert_eq!(clock_verdict(Some(true)).status, Status::Pass);
        assert_eq!(clock_verdict(Some(false)).status, Status::Warn);
        assert_eq!(clock_verdict(None).status, Status::Warn);
        clock(); // asks this host; any answer is fine
    }
}
