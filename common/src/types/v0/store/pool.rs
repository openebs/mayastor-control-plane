//! Definition of pool types that can be saved to the persistent store.

use crate::types::v0::{
    message_bus::{
        mbus,
        mbus::{CreatePool, NodeId, PoolDeviceUri, PoolId},
    },
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        SpecState, SpecTransaction,
    },
};
use paperclip::actix::Apiv2Schema;
use serde::{Deserialize, Serialize};

type PoolLabel = String;

/// Pool data structure used by the persistent store.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Pool {
    /// Current state of the pool.
    pub state: Option<PoolState>,
    /// Desired pool specification.
    pub spec: Option<PoolSpec>,
}

/// Runtime state of the pool.
/// This should eventually satisfy the PoolSpec.
#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct PoolState {
    /// Pool information returned by Mayastor.
    pub pool: mbus::Pool,
    /// Pool labels.
    pub labels: Vec<PoolLabel>,
}

/// State of the Pool Spec
pub type PoolSpecState = SpecState<mbus::PoolState>;
impl From<&CreatePool> for PoolSpec {
    fn from(request: &CreatePool) -> Self {
        Self {
            node: request.node.clone(),
            id: request.id.clone(),
            disks: request.disks.clone(),
            state: PoolSpecState::Creating,
            labels: vec![],
            updating: false,
            operation: None,
        }
    }
}
impl PartialEq<CreatePool> for PoolSpec {
    fn eq(&self, other: &CreatePool) -> bool {
        let mut other = PoolSpec::from(other);
        other.state = self.state.clone();
        &other == self
    }
}

/// User specification of a pool.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Apiv2Schema)]
pub struct PoolSpec {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
    /// absolute disk paths claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
    /// state of the pool
    pub state: PoolSpecState,
    /// Pool labels.
    pub labels: Vec<PoolLabel>,
    /// Update in progress
    #[serde(skip)]
    pub updating: bool,
    /// Record of the operation in progress
    pub operation: Option<PoolOperationState>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Apiv2Schema)]
pub struct PoolOperationState {
    /// Record of the operation
    pub operation: PoolOperation,
    /// Result of the operation
    pub result: Option<bool>,
}

impl SpecTransaction<PoolOperation> for PoolSpec {
    fn pending_op(&self) -> bool {
        self.operation.is_some()
    }

    fn commit_op(&mut self) {
        if let Some(op) = self.operation.clone() {
            match op.operation {
                PoolOperation::Unknown => unreachable!(),
                PoolOperation::Destroy => {
                    self.state = SpecState::Deleted;
                }
                PoolOperation::Create => {
                    self.state = SpecState::Created(mbus::PoolState::Online);
                }
            }
        }
        self.clear_op();
    }

    fn clear_op(&mut self) {
        self.operation = None;
        self.updating = false;
    }

    fn start_op(&mut self, operation: PoolOperation) {
        self.updating = true;
        self.operation = Some(PoolOperationState {
            operation,
            result: None,
        })
    }

    fn set_op_result(&mut self, result: bool) {
        if let Some(op) = &mut self.operation {
            op.result = Some(result);
        }
        self.updating = false;
    }
}

/// Available Pool Operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Apiv2Schema)]
pub enum PoolOperation {
    Unknown,
    Create,
    Destroy,
}

impl Default for PoolOperation {
    fn default() -> Self {
        Self::Unknown
    }
}

impl PartialEq<mbus::Pool> for PoolSpec {
    fn eq(&self, other: &mbus::Pool) -> bool {
        self.node == other.node
    }
}

/// Key used by the store to uniquely identify a PoolSpec structure.
pub struct PoolSpecKey(PoolId);

impl From<&PoolId> for PoolSpecKey {
    fn from(id: &PoolId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for PoolSpecKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::PoolSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for PoolSpec {
    type Key = PoolSpecKey;

    fn key(&self) -> Self::Key {
        PoolSpecKey(self.id.clone())
    }
}

impl From<&PoolSpec> for mbus::Pool {
    fn from(pool: &PoolSpec) -> Self {
        Self {
            node: pool.node.clone(),
            id: pool.id.clone(),
            disks: pool.disks.clone(),
            state: mbus::PoolState::Unknown,
            capacity: 0,
            used: 0,
        }
    }
}
