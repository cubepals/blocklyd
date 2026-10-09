//! The wire types as OpenAPI 3.1 documents, in openapi/. Each release carries them, and the control
//! plane (cubepals/cubepals) pins a release's and generates its TypeScript types from them: the
//! protocol is written down once, here, and neither side keeps a copy of the other's by hand.
//!
//! Two documents, because serde reads a type differently from how it writes it: a field with a
//! default may be left out of what the node reads but is always in what it writes, so `Resources`
//! requires different fields in each direction.
//! - `node-reads.openapi.json`, as the node deserializes: its API's requests, and the control
//!   plane's answers to enrollment, heartbeats and renewal.
//! - `node-writes.openapi.json`, as the node serializes: its views and answers, log lines and
//!   errors, and its own requests to the control plane.
//!
//! Only tests derive the schemas, so none of this reaches the binary. A test fails when a committed
//! document is not what the types say; `BLOCKLYD_WRITE_SCHEMA=1 cargo test --lib protocol::schema`
//! writes them again. The documents don't describe the API's paths: those are in docs/protocol.md.

use std::path::Path;

use schemars::SchemaGenerator;
use schemars::generate::{Contract, SchemaSettings};
use serde_json::{Value, json};

use super::*;
use crate::fleet::wire::{
    EnrollRequest, EnrollResponse, HeartbeatRequest, HeartbeatResponse, RenewRequest, RenewResponse,
};

/// Where the documents are committed, and where a release takes them from.
const OPENAPI: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/openapi");

/// Adds each type's schema, and the schemas of the types it holds, to the generator's components.
macro_rules! schemas {
    ($generator:expr; $($ty:ty),+ $(,)?) => { $( $generator.subschema_for::<$ty>(); )+ };
}

fn node_reads() -> Value {
    document("blocklyd: what a node reads", Contract::Deserialize, |g| {
        schemas!(g;
            WorkloadSpec, StopRequest, FenceRequest, ExecRequest, ExportRequest, RestoreRequest, SnapshotRequest,
            UploadRequest, EnrollResponse, HeartbeatResponse, RenewResponse,
        );
    })
}

fn node_writes() -> Value {
    document("blocklyd: what a node writes", Contract::Serialize, |g| {
        schemas!(g;
            WorkloadView, ListResponse, StatsView, LogRecord, EnsureResponse, PowerResponse, FenceResponse,
            DeleteResponse, ExecResponse, ExportResponse, RestoreResponse, SnapshotResponse, SnapshotList,
            SnapshotDeleteResponse, NodeHealth, NodeStatus, ErrorBody, EnrollRequest, HeartbeatRequest, RenewRequest,
        );
    })
}

/// An OpenAPI document with no paths: the schemas of the types `add` names, under
/// `components/schemas`, keys sorted as the control plane keeps every spec it pins.
fn document(title: &str, contract: Contract, add: impl FnOnce(&mut SchemaGenerator)) -> Value {
    let mut generator = SchemaSettings::draft2020_12()
        .with(|s| {
            s.definitions_path = "/components/schemas".into();
            s.meta_schema = None;
            s.contract = contract;
        })
        .into_generator();
    add(&mut generator);
    let mut document = json!({
        "openapi": "3.1.0",
        "info": { "title": title, "version": PROTOCOL_VERSION.to_string() },
        "paths": {},
        "components": { "schemas": generator.take_definitions(true) },
    });
    document.sort_all_objects();
    document
}

/// Two-space indent and a newline at the end, as `JSON.stringify(spec, null, 2)` writes it.
fn canonical(document: &Value) -> String {
    format!("{}\n", serde_json::to_string_pretty(document).expect("a document serializes"))
}

fn required<'a>(document: &'a Value, schema: &str) -> Vec<&'a str> {
    let required = document["components"]["schemas"][schema]["required"].as_array();
    required.into_iter().flatten().filter_map(Value::as_str).collect()
}

#[test]
fn the_committed_documents_are_what_the_wire_types_say() {
    for (name, document) in [("node-reads.openapi.json", node_reads()), ("node-writes.openapi.json", node_writes())] {
        let path = Path::new(OPENAPI).join(name);
        let generated = canonical(&document);
        if std::env::var_os("BLOCKLYD_WRITE_SCHEMA").is_some() {
            std::fs::write(&path, &generated).expect("writes the document");
            continue;
        }
        let committed = std::fs::read_to_string(&path).unwrap_or_default();
        assert!(
            committed == generated,
            "{} is not what the wire types say: run `BLOCKLYD_WRITE_SCHEMA=1 cargo test --lib protocol::schema` \
             and commit openapi/",
            path.display()
        );
    }
}

/// What the control plane's types stand on: a spec with a field the node doesn't know is refused, a
/// default may be left out of a request but is always in an answer, and an exit code may be null.
#[test]
fn the_documents_follow_serde_in_each_direction() {
    let (reads, writes) = (node_reads(), node_writes());
    assert_eq!(reads["components"]["schemas"]["WorkloadSpec"]["additionalProperties"], json!(false));
    assert_eq!(required(&reads, "Resources"), ["memoryMb"]);
    assert_eq!(required(&writes, "Resources"), ["memoryMb", "cpuMillis", "cpuWeight", "pidsLimit"]);
    let exit_code = &writes["components"]["schemas"]["ExecResponse"]["properties"]["exitCode"];
    assert_eq!(exit_code["type"], json!(["integer", "null"]));
    assert!(required(&writes, "ExecResponse").contains(&"exitCode"), "written as null, never left out");
}
