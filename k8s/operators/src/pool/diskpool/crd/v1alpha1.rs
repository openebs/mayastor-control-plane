use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    CustomResource, Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone, JsonSchema,
)]
#[kube(
group = "openebs.io",
version = "v1alpha1",
kind = "DiskPool",
plural = "diskpools",
// The name of the struct that gets created that represents a resource
namespaced,
status = "DiskPoolStatus",
derive = "PartialEq",
derive = "Default",
shortname = "dsp",
printcolumn = r#"{ "name":"node", "type":"string", "description":"node the pool is on", "jsonPath":".spec.node"}"#,
printcolumn = r#"{ "name":"state", "type":"string", "description":"dsp cr state", "jsonPath":".status.state"}"#,
printcolumn = r#"{ "name":"pool_status", "type":"string", "description":"Control plane pool status", "jsonPath":".status.pool_status"}"#,
printcolumn = r#"{ "name":"capacity", "type":"integer", "format": "int64", "minimum" : "0", "description":"total bytes", "jsonPath":".status.capacity"}"#,
printcolumn = r#"{ "name":"used", "type":"integer", "format": "int64", "minimum" : "0", "description":"used bytes", "jsonPath":".status.used"}"#,
printcolumn = r#"{ "name":"available", "type":"integer", "format": "int64", "minimum" : "0", "description":"available bytes", "jsonPath":".status.available"}"#
)]

/// The pool spec which contains the parameters we use when creating the pool
pub struct DiskPoolSpec {
    /// The node the pool is placed on
    node: String,
    /// The disk device the pool is located on
    disks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
pub enum PoolState {
    /// The pool is a new OR missing resource, and it has not been created or
    /// imported yet by the operator. The pool spec MAY be but DOES
    /// NOT have a status field.
    Creating,
    /// The resource spec has been created, and the pool is getting created by
    /// the control plane.
    Created,
    /// The resource is present, and the pool has been created. The schema MUST
    /// have a status and spec field.
    Online,
    /// The resource is present but the control plane did not return the pool state.
    Unknown,
    /// Trying to converge to the next state has exceeded the maximum retry
    /// counts. The retry counts are implemented using an exponential back-off,
    /// which by default is set to 10. Once the error state is entered,
    /// reconciliation stops. Only external events (a new resource version)
    /// will trigger a new attempt.
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema, Default)]
/// PoolState represents operator specific states for DSP CR.
pub enum CrPoolState {
    /// The pool is a new OR missing resource, and it has not been created or
    /// imported yet by the operator. The pool spec MAY be but DOES
    /// NOT have a status field.
    #[default]
    Creating,
    /// The resource spec has been created, and the pool is getting created by
    /// the control plane.
    Created,
    /// This state is set when we receive delete event on the dsp cr.
    Terminating,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
/// PoolStatus is Control plane status of a given DSP CR.
pub enum PoolStatus {
    /// State is Unknown.
    Unknown,
    /// The pool is in normal working order.
    Online,
    /// The pool has experienced a failure but can still function.
    Degraded,
    /// The pool is completely inaccessible.
    Faulted,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
/// Status of the pool which is driven and changed by the controller loop.
pub struct DiskPoolStatus {
    pub state: PoolState,
    /// The state of the pool.
    #[serde(default)]
    pub cr_state: CrPoolState,
    /// Pool status from respective control plane object.
    pub pool_status: Option<PoolStatus>,
    /// Capacity as number of bytes.
    capacity: u64,
    /// Used number of bytes.
    used: u64,
    /// Available number of bytes.
    available: u64,
}
