//! What blocklyd leaves on disk and on its containers, pinned as an older build wrote it. A node
//! that upgrades reads these with the new build, and one put back reads what the new build wrote
//! with the old one. Each fixture in `fixtures/` must read as the values it was written from, and
//! those values must still be written as the same bytes. See fixtures/README.md.

use std::path::PathBuf;

use blocklyd::cli::upgrade::Trial;
use blocklyd::fleet::identity::IdentityFile;
use blocklyd::fleet::wire::UpgradeFailure;
use blocklyd::ids::{SnapshotId, WorkloadId};
use blocklyd::manager::LabelRecord;
use blocklyd::protocol::{Proto, SnapshotView, SpecRecord, WorkloadSpec};
use blocklyd::store::{AllocatedPort, Phase, RestingPort, WorkloadRecord};
use blocklyd::tree::Method;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// How blocklyd writes each: a file pretty, a label on one line.
enum Form {
    File,
    Label,
}

/// `name` reads as `then`, and `then` is written as `name`'s bytes. With BLOCKLYD_WRITE_FIXTURES
/// set, a fixture that doesn't exist yet is written first; one that does is never touched.
fn pinned<T>(name: &str, form: Form, then: &T)
where
    T: Serialize + DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name);
    let now = match form {
        Form::File => serde_json::to_string_pretty(then),
        Form::Label => serde_json::to_string(then),
    }
    .unwrap();
    if std::env::var_os("BLOCKLYD_WRITE_FIXTURES").is_some() && !path.exists() {
        std::fs::write(&path, &now).unwrap();
    }
    let old = std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()));
    let read: T = serde_json::from_str(&old).unwrap_or_else(|e| panic!("{name} no longer reads: {e}"));
    assert_eq!(read, *then, "{name} reads as other values");
    assert!(now == old, "this build writes {name} differently, which an older one may not read:\n{now}");
}

fn spec(value: serde_json::Value) -> SpecRecord {
    serde_json::from_value::<WorkloadSpec>(value).unwrap().record()
}

/// Every field a request may leave out set, none to its default.
fn full_spec() -> SpecRecord {
    spec(serde_json::json!({
        "image": "itzg/minecraft-server:2026.9.1-java21",
        "entrypoint": ["/start", "--nogui"],
        "env": { "EULA": "TRUE", "TYPE": "PAPER", "VERSION": "1.21.8", "MEMORY": "4608M" },
        "secrets": { "RCON_PASSWORD": "hunter2hunter2" },
        "resources": { "memoryMb": 6144, "cpuMillis": 3000, "cpuWeight": 200, "pidsLimit": 2048 },
        "storage": { "mountPath": "/data", "sizeGb": 20 },
        "ports": [
            { "name": "game", "containerPort": 25565, "protocol": "tcp", "audience": ["edge", "control"] },
            { "name": "voice", "containerPort": 24454, "protocol": "udp", "audience": ["edge"] }
        ],
        "stop": { "signal": "SIGINT", "timeoutSeconds": 30 },
        "restart": { "policy": "no", "maxRetries": 0 },
        "labels": { "blockly.server": "0b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10" }
    }))
}

const FULL_DIGEST: &str = "sha256:254b6f6c3ff0511614e5d07d04385cfb70f94dd5f05864f59b18f7c3228136a2";

/// Only what a request must say.
fn minimal_spec() -> SpecRecord {
    spec(serde_json::json!({
        "image": "itzg/minecraft-server:2026.9.1-java21",
        "resources": { "memoryMb": 3072 },
        "storage": { "mountPath": "/data", "sizeGb": 5 }
    }))
}

const MINIMAL_DIGEST: &str = "sha256:431a570aeec3f8f524675d78c950fe9d389976be51319d54a8b131aad0a1415b";

fn full_ports() -> Vec<AllocatedPort> {
    vec![
        AllocatedPort { name: "game".into(), protocol: Proto::Tcp, container_port: 25565, host_port: 42017 },
        AllocatedPort { name: "voice".into(), protocol: Proto::Udp, container_port: 24454, host_port: 42018 },
    ]
}

fn survival() -> WorkloadId {
    WorkloadId::parse("0b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10").unwrap()
}

#[test]
fn a_workload_record_with_every_field_set() {
    let record = WorkloadRecord {
        version: 1,
        id: survival(),
        generation: 3,
        phase: Phase::Active,
        spec: full_spec(),
        spec_digest: FULL_DIGEST.into(),
        ports: full_ports(),
        container_name: "blockly-staging-0b6f1f2e-6a47-4c9a-9a39-2f4ac7e51f10".into(),
        container_id: Some("9f2c4be1d8a07e35c6b1f04a92d7e8c3b5a1f6e2d9c84b07a3e5f1c2d6b8a9e4".into()),
        epoch: Some(7),
        superseded_by: Some(8),
        stop_requested_at: Some("2026-10-06T09:12:44.512305117Z".into()),
        running_boot: Some("3c5e1a7d-92b4-4f0e-8d6a-1b2c3d4e5f60".into()),
        restart_count: 2,
        created_at: "2026-10-01T08:00:03.104772391Z".into(),
        updated_at: "2026-10-06T09:12:44.512305117Z".into(),
    };
    pinned("workload-full.json", Form::File, &record);
    assert_eq!(record.spec.digest(), record.spec_digest, "the digest stored is the one this build computes");
}

#[test]
fn a_workload_record_with_nothing_optional() {
    let record = WorkloadRecord {
        version: 1,
        id: WorkloadId::parse("w1").unwrap(),
        generation: 1,
        phase: Phase::Creating,
        spec: minimal_spec(),
        spec_digest: MINIMAL_DIGEST.into(),
        ports: vec![],
        container_name: "blockly-staging-w1".into(),
        container_id: None,
        epoch: None,
        superseded_by: None,
        stop_requested_at: None,
        running_boot: None,
        restart_count: 0,
        created_at: "2026-10-01T08:00:03.104772391Z".into(),
        updated_at: "2026-10-01T08:00:03.104772391Z".into(),
    };
    pinned("workload-minimal.json", Form::File, &record);
    assert_eq!(record.spec.digest(), record.spec_digest, "the digest stored is the one this build computes");
}

#[test]
fn a_snapshot_description() {
    let snapshot = SnapshotView {
        id: SnapshotId::parse("5d0e7c1a-3b9f-4e62-a8d4-7f1b2c3e4d5a").unwrap(),
        workload: survival(),
        epoch: Some(7),
        created_at: "2026-10-05T22:00:01.287441903Z".into(),
        size_bytes: 734_003_200,
        files: 4211,
        method: Method::Reflink,
        quiesced: true,
        spec_digest: FULL_DIGEST.into(),
        duration_ms: 312,
    };
    pinned("snapshot.json", Form::File, &snapshot);
}

#[test]
fn ports_resting_in_quarantine() {
    let ports = vec![
        RestingPort { protocol: Proto::Tcp, port: 42003, released_at_unix: 1_791_277_964 },
        RestingPort { protocol: Proto::Udp, port: 42004, released_at_unix: 1_791_277_964 },
    ];
    pinned("ports.json", Form::File, &ports);
}

#[test]
fn a_fleet_identity_after_renewals() {
    let identity = IdentityFile {
        node_id: "6f1c2d3e-4a5b-4c6d-8e7f-9a0b1c2d3e4f".into(),
        deployment_id: "staging".into(),
        control_plane: "https://fleet.internal.example:8443".into(),
        allowed_clients: vec!["control-plane.staging.fleet".into()],
        enrolled_at: "2026-09-12T14:03:27.665190042Z".into(),
        generation: 2,
    };
    pinned("node.json", Form::File, &identity);
}

#[test]
fn an_upgrade_on_trial_and_one_given_up_on() {
    let trial = Trial {
        from: "0.2.0".into(),
        to: "0.3.0".into(),
        sha256: "c0ffee5e7a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5".into(),
    };
    pinned("trial.json", Form::File, &trial);
    let failure = UpgradeFailure {
        version: "0.3.0".into(),
        reason: "blocklyd 0.3.0 didn't reconcile and get a heartbeat accepted within 120 s".into(),
    };
    pinned("failed.json", Form::File, &failure);
}

#[test]
fn the_record_a_container_carries_in_its_label() {
    let label = LabelRecord {
        version: 1,
        generation: 3,
        epoch: Some(7),
        spec_digest: FULL_DIGEST.into(),
        ports: full_ports(),
        spec: full_spec(),
        created_at: "2026-10-01T08:00:03.104772391Z".into(),
    };
    pinned("label-record.json", Form::Label, &label);
    assert_eq!(label.spec.digest(), label.spec_digest, "the digest stored is the one this build computes");
}
