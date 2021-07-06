//! Definition of pool types that can be saved to the persistent store.

use crate::types::v0::{
    message_bus::{self, CreatePool, NodeId, Pool as MbusPool, PoolDeviceUri, PoolId},
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        SpecState, SpecTransaction,
    },
};

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
#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
pub struct PoolState {
    /// Pool information returned by Mayastor.
    pub pool: message_bus::Pool,
    /// Pool labels.
    pub labels: Vec<PoolLabel>,
}

impl From<MbusPool> for PoolState {
    fn from(pool: MbusPool) -> Self {
        Self {
            pool,
            labels: vec![],
        }
    }
}

/// State of the Pool Spec
pub type PoolSpecState = SpecState<message_bus::PoolState>;
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
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
                PoolOperation::Destroy => {
                    self.state = SpecState::Deleted;
                }
                PoolOperation::Create => {
                    self.state = SpecState::Created(message_bus::PoolState::Online);
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PoolOperation {
    Create,
    Destroy,
}

impl PartialEq<message_bus::Pool> for PoolSpec {
    fn eq(&self, other: &message_bus::Pool) -> bool {
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

impl From<&PoolSpec> for message_bus::Pool {
    fn from(pool: &PoolSpec) -> Self {
        Self {
            node: pool.node.clone(),
            id: pool.id.clone(),
            disks: pool.disks.clone(),
            state: message_bus::PoolState::Unknown,
            capacity: 0,
            used: 0,
        }
    }
}
