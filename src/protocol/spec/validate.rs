// SPDX-FileCopyrightText: 2026 The Cubepals Authors
//
// SPDX-License-Identifier: FSL-1.1-ALv2

//! Whether a spec is one this host will run: the shape of every field, checked against the
//! host's own bounds (`SpecPolicy`), with every problem reported at once. It sits under `spec`
//! because it reads secret values, which only the spec's module may see. What a spec holds, and
//! its defaults, are in `spec.rs`.

use super::WorkloadSpec;
use crate::protocol::{FieldError, RESERVED_LABEL_PREFIX};

/// The host's own bounds. The protocol validates shape; the host decides what it allows.
#[derive(Clone, Debug)]
pub(crate) struct SpecPolicy {
    pub(crate) allowed_images: Vec<String>,
    pub(crate) min_memory_mb: u32,
    pub(crate) max_memory_mb: u32,
    pub(crate) max_cpu_millis: u32,
}

const MAX_ENV: usize = 256;
const MAX_ENV_VALUE: usize = 64 * 1024;
const MAX_ENV_TOTAL: usize = 512 * 1024;
const MAX_ENTRYPOINT_ARGS: usize = 64;
const MAX_ARG: usize = 64 * 1024;
const MAX_PORTS: usize = 8;
const MAX_LABELS: usize = 32;

fn env_key_ok(key: &str) -> bool {
    let b = key.as_bytes();
    !b.is_empty()
        && b.len() <= 128
        && (b[0].is_ascii_alphabetic() || b[0] == b'_')
        && b.iter().all(|c| c.is_ascii_alphanumeric() || *c == b'_')
}

fn port_name_ok(name: &str) -> bool {
    let b = name.as_bytes();
    !b.is_empty()
        && b.len() <= 16
        && b[0].is_ascii_lowercase()
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'-')
}

fn label_key_ok(key: &str) -> bool {
    let b = key.as_bytes();
    !b.is_empty()
        && b.len() <= 128
        && b[0].is_ascii_alphanumeric()
        && b.iter().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || b"._/-".contains(c))
}

/// An absolute container path of plain components: no `.`, `..`, empty or odd characters.
pub(crate) fn mount_path_ok(path: &str) -> bool {
    if !path.starts_with('/') || path.len() > 128 || path == "/" {
        return false;
    }
    path[1..].split('/').all(|part| {
        !part.is_empty()
            && part != "."
            && part != ".."
            && part.bytes().all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
    })
}

/// Image references: registry/name:tag or @digest, conservative characters only.
fn image_ok(image: &str) -> bool {
    !image.is_empty()
        && image.len() <= 255
        && image.bytes().all(|c| c.is_ascii_alphanumeric() || b"._-/:@".contains(&c))
        && !image.starts_with(['-', '/', ':', '@'])
}

fn no_nul(s: &str) -> bool {
    !s.contains('\0')
}

/// The problems found so far, in the order the fields are checked.
#[derive(Default)]
struct Problems(Vec<FieldError>);

impl Problems {
    fn add(&mut self, field: impl Into<String>, problem: impl Into<String>) {
        self.0.push(FieldError { field: field.into(), problem: problem.into() });
    }

    /// One variable of `env` or `secrets`, which share their rules; `section` names which.
    fn check_variable(&mut self, section: &str, key: &str, value: &str) {
        if !env_key_ok(key) {
            self.add(format!("{section}.{key}"), "names are letters, digits and _, not starting with a digit");
        }
        if value.len() > MAX_ENV_VALUE || !no_nul(value) {
            self.add(format!("{section}.{key}"), format!("values are at most {MAX_ENV_VALUE} bytes, without NUL"));
        }
    }
}

impl WorkloadSpec {
    /// Every problem at once, so a client fixes a request in one round.
    pub(crate) fn validate(&self, policy: &SpecPolicy) -> Result<(), Vec<FieldError>> {
        let mut problems = Problems::default();
        self.check_image(policy, &mut problems);
        self.check_entrypoint(&mut problems);
        self.check_env(&mut problems);
        self.check_resources(policy, &mut problems);
        self.check_storage(&mut problems);
        self.check_ports(&mut problems);
        self.check_stop_and_restart(&mut problems);
        self.check_labels(&mut problems);
        if problems.0.is_empty() { Ok(()) } else { Err(problems.0) }
    }

    fn check_image(&self, policy: &SpecPolicy, problems: &mut Problems) {
        if !image_ok(&self.image) {
            problems.add("image", "must be an image reference of letters, digits and ._-/:@");
        } else if !policy.allowed_images.iter().any(|prefix| self.image.starts_with(prefix.as_str())) {
            problems
                .add("image", format!("is not allowed on this host (allowed: {})", policy.allowed_images.join(", ")));
        }
    }

    fn check_entrypoint(&self, problems: &mut Problems) {
        let Some(entrypoint) = &self.entrypoint else { return };
        if entrypoint.is_empty() || entrypoint.len() > MAX_ENTRYPOINT_ARGS {
            problems.add("entrypoint", format!("must hold 1 to {MAX_ENTRYPOINT_ARGS} arguments"));
        }
        if entrypoint.iter().any(|a| a.len() > MAX_ARG || !no_nul(a)) {
            problems.add("entrypoint", format!("arguments must be at most {MAX_ARG} bytes, without NUL"));
        }
    }

    /// `env` and `secrets` together: both become the container's environment, so they share
    /// its limits and a name can't be in both.
    fn check_env(&self, problems: &mut Problems) {
        if self.env.len() + self.secrets.len() > MAX_ENV {
            problems.add("env", format!("at most {MAX_ENV} variables, secrets included"));
        }
        for (key, value) in &self.env {
            problems.check_variable("env", key, value);
        }
        for (key, value) in &self.secrets {
            problems.check_variable("secrets", key, &value.0);
            if self.env.contains_key(key) {
                problems.add(format!("secrets.{key}"), "is also in env; a name is one or the other");
            }
        }
        let env = self.env.iter().map(|(k, v)| k.len() + v.len());
        let secrets = self.secrets.iter().map(|(k, v)| k.len() + v.0.len());
        if env.chain(secrets).sum::<usize>() > MAX_ENV_TOTAL {
            problems.add("env", format!("all variables together must stay under {MAX_ENV_TOTAL} bytes"));
        }
    }

    fn check_resources(&self, policy: &SpecPolicy, problems: &mut Problems) {
        let r = &self.resources;
        if r.memory_mb < policy.min_memory_mb || r.memory_mb > policy.max_memory_mb {
            problems.add(
                "resources.memoryMb",
                format!("must be between {} and {} on this host", policy.min_memory_mb, policy.max_memory_mb),
            );
        }
        if let Some(cpu) = r.cpu_millis
            && (cpu < 100 || cpu > policy.max_cpu_millis)
        {
            problems
                .add("resources.cpuMillis", format!("must be between 100 and {} on this host", policy.max_cpu_millis));
        }
        if let Some(weight) = r.cpu_weight
            && !(2..=262_144).contains(&weight)
        {
            problems.add("resources.cpuWeight", "must be between 2 and 262144");
        }
        if let Some(pids) = r.pids_limit
            && !(64..=65_536).contains(&pids)
        {
            problems.add("resources.pidsLimit", "must be between 64 and 65536");
        }
    }

    fn check_storage(&self, problems: &mut Problems) {
        if !mount_path_ok(&self.storage.mount_path) {
            problems.add("storage.mountPath", "must be an absolute path of plain components, not /");
        }
        if !(1..=4096).contains(&self.storage.size_gb) {
            problems.add("storage.sizeGb", "must be between 1 and 4096");
        }
    }

    /// Each port on its own, and the ports against each other: a name, or a protocol's
    /// container port, is used once.
    fn check_ports(&self, problems: &mut Problems) {
        if self.ports.len() > MAX_PORTS {
            problems.add("ports", format!("at most {MAX_PORTS}"));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut inside = std::collections::BTreeSet::new();
        for (i, port) in self.ports.iter().enumerate() {
            if !port_name_ok(&port.name) {
                problems.add(format!("ports[{i}].name"), "is 1-16 of a-z, 0-9 and '-', starting with a letter");
            }
            if !names.insert(port.name.as_str()) {
                problems.add(format!("ports[{i}].name"), "is used twice");
            }
            if port.container_port == 0 {
                problems.add(format!("ports[{i}].containerPort"), "must be 1-65535");
            }
            if !inside.insert((port.protocol, port.container_port)) {
                problems.add(format!("ports[{i}].containerPort"), "is used twice for the same protocol");
            }
            if port.audience.is_empty() {
                problems.add(format!("ports[{i}].audience"), "names at least one of edge, control");
            }
        }
    }

    fn check_stop_and_restart(&self, problems: &mut Problems) {
        if !(1..=600).contains(&self.stop.timeout_seconds) {
            problems.add("stop.timeoutSeconds", "must be between 1 and 600");
        }
        if self.restart.max_retries > 10 {
            problems.add("restart.maxRetries", "must be at most 10");
        }
    }

    fn check_labels(&self, problems: &mut Problems) {
        if self.labels.len() > MAX_LABELS {
            problems.add("labels", format!("at most {MAX_LABELS}"));
        }
        for (key, value) in &self.labels {
            if !label_key_ok(key) {
                problems.add(format!("labels.{key}"), "keys are lowercase letters, digits and ._/-");
            }
            if key.starts_with(RESERVED_LABEL_PREFIX) {
                problems.add(format!("labels.{key}"), format!("the {RESERVED_LABEL_PREFIX} prefix is blocklyd's own"));
            }
            if value.len() > 256 || !no_nul(value) {
                problems.add(format!("labels.{key}"), "values are at most 256 bytes, without NUL");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{blockly_like, spec};
    use super::*;
    use crate::protocol::{Proto, RestartSpec};

    pub(crate) fn policy() -> SpecPolicy {
        SpecPolicy {
            allowed_images: vec!["itzg/minecraft-server:".into(), "alpine:".into()],
            min_memory_mb: 128,
            max_memory_mb: 16_384,
            max_cpu_millis: 8000,
        }
    }

    #[test]
    fn a_blockly_runtime_spec_is_valid() {
        let s = spec(blockly_like());
        assert!(s.validate(&policy()).is_ok());
        assert_eq!(s.ports[0].protocol, Proto::Tcp, "tcp is the default");
        assert_eq!(s.restart, RestartSpec::default());
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let mut v = blockly_like();
        v["image"] = "evil/image:latest".into();
        v["storage"]["mountPath"] = "/data/../etc".into();
        v["resources"]["memoryMb"] = 64.into();
        v["labels"]["blocklyd.workload"] = "someone-else".into();
        v["env"]["BAD-NAME"] = "x".into();
        let errors = spec(v).validate(&policy()).unwrap_err();
        let fields: Vec<_> = errors.iter().map(|e| e.field.as_str()).collect();
        for expected in ["image", "storage.mountPath", "resources.memoryMb", "labels.blocklyd.workload", "env.BAD-NAME"]
        {
            assert!(fields.contains(&expected), "{expected} in {fields:?}");
        }
    }

    #[test]
    fn mount_paths_are_plain() {
        for ok in ["/data", "/srv/mc-data", "/a/b.c"] {
            assert!(mount_path_ok(ok), "{ok}");
        }
        for bad in ["", "/", "data", "/data/", "/a//b", "/a/./b", "/a/../b", "/a b", "/a\0b", "/$(x)"] {
            assert!(!mount_path_ok(bad), "{bad:?}");
        }
    }

    #[test]
    fn secrets_are_held_to_the_env_rules_and_limits() {
        let mut v = blockly_like();
        v["secrets"]["bad-name"] = "x".into();
        v["secrets"]["EULA"] = "TRUE".into();
        v["secrets"]["BIG"] = "x".repeat(MAX_ENV_VALUE).into();
        v["env"]["ALSO_BIG"] = "x".repeat(MAX_ENV_TOTAL - MAX_ENV_VALUE).into();
        let errors = spec(v).validate(&policy()).unwrap_err();
        let problems: Vec<_> = errors.iter().map(|e| (e.field.as_str(), e.problem.as_str())).collect();
        assert_eq!(
            problems,
            [
                ("env.ALSO_BIG", "values are at most 65536 bytes, without NUL"),
                ("secrets.EULA", "is also in env; a name is one or the other"),
                ("secrets.bad-name", "names are letters, digits and _, not starting with a digit"),
                ("env", "all variables together must stay under 524288 bytes"),
            ]
        );
    }

    #[test]
    fn ports_must_be_distinct_and_audienced() {
        let mut v = blockly_like();
        v["ports"] = serde_json::json!([
            { "name": "game", "containerPort": 25565, "audience": ["edge"] },
            { "name": "game", "containerPort": 25565, "audience": [] }
        ]);
        let errors = spec(v).validate(&policy()).unwrap_err();
        assert!(errors.iter().any(|e| e.problem.contains("used twice")));
        assert!(errors.iter().any(|e| e.field == "ports[1].audience"));
    }
}
