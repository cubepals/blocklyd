// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! The seccomp profile every workload runs under: Docker's default (moby/profiles, kept as
//! published in `seccomp/moby-default.json`) with the ioctls that set a file's flags taken away.
//!
//! Those ioctls are how a file's owner moves it out of its project quota: clearing
//! `FS_XFLAG_PROJINHERIT` or setting its project id (`FS_IOC_FSSETXATTR`), or clearing
//! `FS_PROJINHERIT_FL` (`FS_IOC_SETFLAGS`). The kernel lets an owner do either from the host's user
//! namespace, which Docker's containers share, with no capability at all. Reading flags stays
//! allowed, as does every other ioctl.
//!
//! seccomp can't say "any command but these" in one rule: libseccomp keeps one comparison per
//! argument. So `ioctl` is allowed by a rule per aligned block of the commands left over, each a
//! single masked compare. The masks cover the low 32 bits only: the kernel reads the command as an
//! `unsigned int`, and musl passes it sign-extended. Not for the rest of the container's hardening
//! (`docker.rs`).

use std::sync::LazyLock;

use linux_raw_sys::ioctl::{FS_IOC_FSSETXATTR, FS_IOC_SETFLAGS, FS_IOC32_SETFLAGS};
use serde_json::{Value, json};

/// What a container made under this profile is labelled with (`labels::LABEL_CONFINED`). A change to
/// what the profile refuses changes it, so containers made before are known.
pub(crate) const CONFINEMENT: &str = "moby-seccomp-v0.3.0/no-fs-flags";

/// Docker's default profile, moby/profiles at seccomp/v0.3.0.
const MOBY_DEFAULT: &str = include_str!("seccomp/moby-default.json");

/// The ioctl commands a workload may not make: each sets a file's flags or project.
pub(crate) const REFUSED_IOCTLS: [u32; 3] = [FS_IOC_FSSETXATTR, FS_IOC_SETFLAGS, FS_IOC32_SETFLAGS];

/// The profile as Docker takes it in `security_opt` (`seccomp=<this>`).
pub(crate) fn profile() -> &'static str {
    static PROFILE: LazyLock<String> = LazyLock::new(|| build(MOBY_DEFAULT, &REFUSED_IOCTLS).to_string());
    &PROFILE
}

fn build(default: &str, refused: &[u32]) -> Value {
    let mut profile: Value = serde_json::from_str(default).expect("the vendored profile is JSON");
    let rules = profile["syscalls"].as_array_mut().expect("the vendored profile lists syscalls");
    for rule in rules.iter_mut() {
        if let Some(names) = rule["names"].as_array_mut() {
            names.retain(|n| n != "ioctl");
        }
    }
    let allowed = allowed_blocks(refused).into_iter().map(|(base, bits)| {
        json!({
            "names": ["ioctl"],
            "action": "SCMP_ACT_ALLOW",
            "args": [{
                "index": 1,
                "value": u64::from(u32::MAX) & !((1u64 << bits) - 1),
                "valueTwo": base,
                "op": "SCMP_CMP_MASKED_EQ",
            }],
        })
    });
    rules.splice(0..0, allowed);
    profile
}

/// Every 32-bit command but `refused`, as aligned blocks `(base, bits)`: the commands whose high
/// `32 - bits` bits equal `base`'s.
fn allowed_blocks(refused: &[u32]) -> Vec<(u64, u32)> {
    let mut refused: Vec<u64> = refused.iter().copied().map(u64::from).collect();
    refused.sort_unstable();
    refused.dedup();
    let mut blocks = Vec::new();
    let mut from = 0;
    for gap_end in refused.iter().copied().chain([1u64 << 32]) {
        while from < gap_end {
            let mut bits = if from == 0 { 32 } else { from.trailing_zeros().min(32) };
            while (1u64 << bits) > gap_end - from {
                bits -= 1;
            }
            blocks.push((from, bits));
            from += 1u64 << bits;
        }
        from = gap_end + 1;
    }
    blocks
}

#[cfg(test)]
mod tests {
    use super::*;

    /// What the kernel would decide for `command`, as `build`'s rules say.
    fn allowed(profile: &Value, command: u64) -> bool {
        profile["syscalls"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["names"].as_array().unwrap().contains(&json!("ioctl")))
            .any(|r| {
                let arg = &r["args"][0];
                let (mask, base) = (arg["value"].as_u64().unwrap(), arg["valueTwo"].as_u64().unwrap());
                r["action"] == "SCMP_ACT_ALLOW" && command & mask == base
            })
    }

    #[test]
    fn setting_flags_is_refused_and_every_other_ioctl_allowed() {
        let profile: Value = serde_json::from_str(profile()).unwrap();
        for refused in REFUSED_IOCTLS.map(u64::from) {
            assert!(!allowed(&profile, refused), "{refused:#x}");
            // The kernel reads only the low 32 bits.
            assert!(!allowed(&profile, refused | 0xffff_ffff_0000_0000), "{refused:#x} sign-extended");
            assert!(allowed(&profile, refused - 1) && allowed(&profile, refused + 1), "{refused:#x}'s neighbours");
        }
        let reads = [
            linux_raw_sys::ioctl::FS_IOC_FSGETXATTR,
            linux_raw_sys::ioctl::FS_IOC_GETFLAGS,
            linux_raw_sys::ioctl::FIONREAD,
        ];
        for read in reads.map(u64::from) {
            assert!(allowed(&profile, read) && allowed(&profile, read | 0xffff_ffff_0000_0000), "{read:#x}");
        }
        assert!(allowed(&profile, 0) && allowed(&profile, u64::from(u32::MAX)));
    }

    #[test]
    fn the_blocks_cover_every_command_but_the_refused_ones() {
        let refused = [5, 6, 0x8000_0000];
        let blocks = allowed_blocks(&refused);
        let covered: u64 = blocks.iter().map(|(_, bits)| 1u64 << bits).sum();
        assert_eq!(covered, (1u64 << 32) - 3);
        for (base, bits) in &blocks {
            assert_eq!(base % (1u64 << bits), 0, "aligned");
            assert!(
                refused.iter().all(|r| u64::from(*r) >> bits != base >> bits),
                "{base:#x}/{bits} holds a refused one"
            );
        }
    }

    #[test]
    fn nothing_else_in_dockers_profile_changes() {
        let default: Value = serde_json::from_str(MOBY_DEFAULT).unwrap();
        let profile: Value = serde_json::from_str(profile()).unwrap();
        assert_eq!(profile["defaultAction"], default["defaultAction"]);
        let names = |p: &Value| -> Vec<String> {
            let mut all: Vec<String> = p["syscalls"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|r| r["names"].as_array().unwrap().iter().map(|n| n.as_str().unwrap().to_owned()))
                .filter(|n| n != "ioctl")
                .collect();
            all.sort();
            all
        };
        assert_eq!(names(&profile), names(&default));
    }
}
