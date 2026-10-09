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

impl WorkloadSpec {
    /// Every problem at once, so a client fixes a request in one round.
    pub(crate) fn validate(&self, policy: &SpecPolicy) -> Result<(), Vec<FieldError>> {
        let mut errors = Vec::new();
        let mut err = |field: &str, problem: String| errors.push(FieldError { field: field.into(), problem });

        if !image_ok(&self.image) {
            err("image", "must be an image reference of letters, digits and ._-/:@".into());
        } else if !policy.allowed_images.iter().any(|prefix| self.image.starts_with(prefix.as_str())) {
            err("image", format!("is not allowed on this host (allowed: {})", policy.allowed_images.join(", ")));
        }

        if let Some(entrypoint) = &self.entrypoint {
            if entrypoint.is_empty() || entrypoint.len() > MAX_ENTRYPOINT_ARGS {
                err("entrypoint", format!("must hold 1 to {MAX_ENTRYPOINT_ARGS} arguments"));
            }
            if entrypoint.iter().any(|a| a.len() > MAX_ARG || !no_nul(a)) {
                err("entrypoint", format!("arguments must be at most {MAX_ARG} bytes, without NUL"));
            }
        }

        let mut total = 0usize;
        if self.env.len() + self.secrets.len() > MAX_ENV {
            err("env", format!("at most {MAX_ENV} variables, secrets included"));
        }
        for (key, value) in &self.env {
            total += key.len() + value.len();
            if !env_key_ok(key) {
                err(&format!("env.{key}"), "names are letters, digits and _, not starting with a digit".into());
            }
            if value.len() > MAX_ENV_VALUE || !no_nul(value) {
                err(&format!("env.{key}"), format!("values are at most {MAX_ENV_VALUE} bytes, without NUL"));
            }
        }
        for (key, value) in &self.secrets {
            total += key.len() + value.0.len();
            if !env_key_ok(key) {
                err(&format!("secrets.{key}"), "names are letters, digits and _, not starting with a digit".into());
            }
            if value.0.len() > MAX_ENV_VALUE || !no_nul(&value.0) {
                err(&format!("secrets.{key}"), format!("values are at most {MAX_ENV_VALUE} bytes, without NUL"));
            }
            if self.env.contains_key(key) {
                err(&format!("secrets.{key}"), "is also in env; a name is one or the other".into());
            }
        }
        if total > MAX_ENV_TOTAL {
            err("env", format!("all variables together must stay under {MAX_ENV_TOTAL} bytes"));
        }

        let r = &self.resources;
        if r.memory_mb < policy.min_memory_mb || r.memory_mb > policy.max_memory_mb {
            err(
                "resources.memoryMb",
                format!("must be between {} and {} on this host", policy.min_memory_mb, policy.max_memory_mb),
            );
        }
        if let Some(cpu) = r.cpu_millis
            && (cpu < 100 || cpu > policy.max_cpu_millis)
        {
            err("resources.cpuMillis", format!("must be between 100 and {} on this host", policy.max_cpu_millis));
        }
        if let Some(weight) = r.cpu_weight
            && !(2..=262_144).contains(&weight)
        {
            err("resources.cpuWeight", "must be between 2 and 262144".into());
        }
        if let Some(pids) = r.pids_limit
            && !(64..=65_536).contains(&pids)
        {
            err("resources.pidsLimit", "must be between 64 and 65536".into());
        }

        if !mount_path_ok(&self.storage.mount_path) {
            err("storage.mountPath", "must be an absolute path of plain components, not /".into());
        }
        if !(1..=4096).contains(&self.storage.size_gb) {
            err("storage.sizeGb", "must be between 1 and 4096".into());
        }

        if self.ports.len() > MAX_PORTS {
            err("ports", format!("at most {MAX_PORTS}"));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut inside = std::collections::BTreeSet::new();
        for (i, port) in self.ports.iter().enumerate() {
            if !port_name_ok(&port.name) {
                err(&format!("ports[{i}].name"), "is 1-16 of a-z, 0-9 and '-', starting with a letter".into());
            }
            if !names.insert(port.name.as_str()) {
                err(&format!("ports[{i}].name"), "is used twice".into());
            }
            if port.container_port == 0 {
                err(&format!("ports[{i}].containerPort"), "must be 1-65535".into());
            }
            if !inside.insert((port.protocol, port.container_port)) {
                err(&format!("ports[{i}].containerPort"), "is used twice for the same protocol".into());
            }
            if port.audience.is_empty() {
                err(&format!("ports[{i}].audience"), "names at least one of edge, control".into());
            }
        }

        if !(1..=600).contains(&self.stop.timeout_seconds) {
            err("stop.timeoutSeconds", "must be between 1 and 600".into());
        }
        if self.restart.max_retries > 10 {
            err("restart.maxRetries", "must be at most 10".into());
        }

        if self.labels.len() > MAX_LABELS {
            err("labels", format!("at most {MAX_LABELS}"));
        }
        for (key, value) in &self.labels {
            if !label_key_ok(key) {
                err(&format!("labels.{key}"), "keys are lowercase letters, digits and ._/-".into());
            }
            if key.starts_with(RESERVED_LABEL_PREFIX) {
                err(&format!("labels.{key}"), format!("the {RESERVED_LABEL_PREFIX} prefix is blocklyd's own"));
            }
            if value.len() > 256 || !no_nul(value) {
                err(&format!("labels.{key}"), "values are at most 256 bytes, without NUL".into());
            }
        }

        if errors.is_empty() { Ok(()) } else { Err(errors) }
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
