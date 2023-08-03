use kube::CustomResource;
use openapi::models::{pool_status::PoolStatus as RestPoolStatus, Pool};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(
    CustomResource, Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone, JsonSchema,
)]
#[kube(
group = "openebs.io",
version = "v1beta1",
kind = "DiskPool",
plural = "diskpools",
// The name of the struct that gets created that represents a resource
namespaced,
status = "DiskPoolStatus",
derive = "PartialEq",
derive = "Default",
shortname = "dsp",
printcolumn = r#"{ "name":"node", "type":"string", "description":"node the pool is on", "jsonPath":".spec.node"}"#,
printcolumn = r#"{ "name":"state", "type":"string", "description":"dsp cr state", "jsonPath":".status.cr_state"}"#,
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

impl DiskPoolSpec {
    /// Create a new DiskPoolSpec from the node and the disks.
    pub fn new(node: String, disks: Vec<String>) -> Self {
        Self { node, disks }
    }
    /// The node the pool is placed on.
    pub fn node(&self) -> String {
        self.node.clone()
    }
    /// The disk devices that compose the pool.
    pub fn disks(&self) -> Vec<String> {
        self.disks.clone()
    }
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

impl Default for DiskPoolStatus {
    fn default() -> Self {
        Self {
            cr_state: CrPoolState::Creating,
            pool_status: None,
            capacity: 0,
            used: 0,
            available: 0,
        }
    }
}

impl DiskPoolStatus {
    /// Set when Pool is not found for some reason.
    pub fn not_found() -> Self {
        Self {
            pool_status: None,
            ..Default::default()
        }
    }

    /// Set when operator is attempting delete on pool.
    pub fn terminating(p: Pool) -> Self {
        let state = p.state.unwrap_or_default();
        let free = if state.capacity > state.used {
            state.capacity - state.used
        } else {
            0
        };
        Self {
            cr_state: CrPoolState::Terminating,
            pool_status: Some(state.status.into()),
            capacity: state.capacity,
            used: state.used,
            available: free,
        }
    }

    /// Set when deleting a Pool which is not accessible.
    pub fn terminating_when_unknown() -> Self {
        Self {
            cr_state: CrPoolState::Terminating,
            pool_status: Some(PoolStatus::Unknown),
            ..Default::default()
        }
    }

    pub fn mark_unknown() -> Self {
        Self {
            cr_state: CrPoolState::Created,
            pool_status: Some(PoolStatus::Unknown),
            ..Default::default()
        }
    }
}

impl From<RestPoolStatus> for PoolStatus {
    fn from(p: RestPoolStatus) -> Self {
        match p {
            RestPoolStatus::Unknown => Self::Unknown,
            RestPoolStatus::Online => Self::Online,
            RestPoolStatus::Degraded => Self::Degraded,
            RestPoolStatus::Faulted => Self::Faulted,
        }
    }
}

/// Returns DiskPoolStatus from Control plane pool object.
impl From<Pool> for DiskPoolStatus {
    fn from(p: Pool) -> Self {
        if let Some(state) = p.state {
            let free = if state.capacity > state.used {
                state.capacity - state.used
            } else {
                0
            };
            Self {
                cr_state: CrPoolState::Created,
                pool_status: Some(state.status.into()),
                capacity: state.capacity,
                used: state.used,
                available: free,
            }
        } else {
            Self {
                cr_state: CrPoolState::Created,
                ..Default::default()
            }
        }
    }
}
