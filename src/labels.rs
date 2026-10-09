//! The labels blocklyd puts on every container it makes, under `protocol::RESERVED_LABEL_PREFIX`:
//! whose the container is, and the record it carries.

pub const LABEL_MANAGED: &str = "blocklyd.managed";
pub const LABEL_DEPLOYMENT: &str = "blocklyd.deployment";
pub const LABEL_NODE: &str = "blocklyd.node";
pub const LABEL_WORKLOAD: &str = "blocklyd.workload";
pub const LABEL_GENERATION: &str = "blocklyd.generation";
pub const LABEL_DIGEST: &str = "blocklyd.spec-digest";
pub const LABEL_EPOCH: &str = "blocklyd.epoch";
/// The whole record (minus secrets) as JSON: enough to rebuild blocklyd's state from the
/// runtime alone if the state directory is lost.
pub const LABEL_RECORD: &str = "blocklyd.record";
