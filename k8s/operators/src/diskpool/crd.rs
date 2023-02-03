use kube::CustomResource;
use openapi::models::{pool_status::PoolStatus as RestPoolStatus, Pool};
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
printcolumn = r#"{ "name":"status", "type":"string", "description":"pool status", "jsonPath":".status.state"}"#,
printcolumn = r#"{ "name":"poolstatus", "type":"string", "description":"control plane pool status", "jsonPath":".status.status"}"#,
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
    /// The node the pool is placed on
    pub fn node(&self) -> String {
        self.node.clone()
    }
    /// The disk device the pool is located on
    pub fn disks(&self) -> Vec<String> {
        self.disks.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
#[non_exhaustive]
/// PoolState represents operator specific states for DSP CR.
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
    /// This state is set when we receive delete event on the dsp cr.
    Terminating,
    /// The resource is present but the control plane did not return the pool state.
    Unknown,
    /// Trying to converge to the next state has exceeded the maximum retry
    /// counts. The retry counts are implemented using an exponential back-off,
    /// which by default is set to 10. Once the error state is entered,
    /// reconciliation stops. Only external events (a new resource version)
    /// will trigger a new attempt.
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, JsonSchema)]
#[non_exhaustive]
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
    /// The state of the pool.
    pub state: PoolState,
    /// Pool status from respective control plane object.
    pub status: Option<PoolStatus>,
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
            state: PoolState::Creating,
            status: None,
            capacity: 0,
            used: 0,
            available: 0,
        }
    }
}

impl DiskPoolStatus {
    /// Set when Pool creation fails for some reason.
    pub fn error() -> Self {
        Self {
            state: PoolState::Error,
            ..Default::default()
        }
    }
    /// Set when create pool api call is successful.
    pub fn created() -> Self {
        Self {
            state: PoolState::Created,
            ..Default::default()
        }
    }
    /// Set when we cant get the Pool from control plane.
    pub fn unknown() -> Self {
        Self {
            state: PoolState::Unknown,
            status: Some(PoolStatus::Unknown),
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
            state: PoolState::Terminating,
            status: Some(state.status.into()),
            capacity: state.capacity,
            used: state.used,
            available: free,
        }
    }

    // /// Set when deleting a Pool which is not accessible.
    // pub fn terminating_when_unknown() -> Self {
    //     Self {
    //         state: PoolState::Terminating,
    //         status: Some(PoolStatus::Unknown),
    //         ..Default::default()
    //     }
    // }
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
        let state = p.state.expect("pool does not have state");
        // todo: Should we set the pool to some sort of error state?
        let free = if state.capacity > state.used {
            state.capacity - state.used
        } else {
            0
        };
        Self {
            state: PoolState::Online,
            status: Some(state.status.into()),
            capacity: state.capacity,
            used: state.used,
            available: free,
        }
    }
}

/// converts the pool state into a string
impl ToString for PoolState {
    fn to_string(&self) -> String {
        match self {
            PoolState::Creating => "Creating",
            PoolState::Created => "Created",
            PoolState::Online => "Online",
            PoolState::Unknown => "Unknown",
            PoolState::Error => "Error",
            PoolState::Terminating => "Terminating",
        }
        .to_string()
    }
}
/// Pool state into a string
impl From<PoolState> for String {
    fn from(p: PoolState) -> Self {
        p.to_string()
    }
}
