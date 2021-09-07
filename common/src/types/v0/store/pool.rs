//! Definition of pool types that can be saved to the persistent store.

use crate::{
    types::v0::{
        message_bus::{self, CreatePool, NodeId, PoolDeviceUri, PoolId},
        openapi::models,
        store::{
            definitions::{ObjectKey, StorableObject, StorableObjectType},
            OperationSequence, OperationSequencer, SpecStatus, SpecTransaction, UuidString,
        },
    },
    IntoVec,
};

use serde::{Deserialize, Serialize};
use std::convert::From;

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
    pub pool: message_bus::PoolState,
}

impl From<message_bus::PoolState> for PoolState {
    fn from(pool: message_bus::PoolState) -> Self {
        Self { pool }
    }
}

impl UuidString for PoolState {
    fn uuid_as_string(&self) -> String {
        self.pool.id.clone().into()
    }
}

/// Status of the Pool Spec
pub type PoolSpecStatus = SpecStatus<message_bus::PoolStatus>;
impl From<models::SpecStatus> for PoolSpecStatus {
    fn from(spec_state: models::SpecStatus) -> Self {
        match spec_state {
            models::SpecStatus::Creating => Self::Creating,
            models::SpecStatus::Created => Self::Created(message_bus::PoolStatus::Unknown),
            models::SpecStatus::Deleting => Self::Deleting,
            models::SpecStatus::Deleted => Self::Deleted,
        }
    }
}

impl From<&CreatePool> for PoolSpec {
    fn from(request: &CreatePool) -> Self {
        Self {
            node: request.node.clone(),
            id: request.id.clone(),
            disks: request.disks.clone(),
            status: PoolSpecStatus::Creating,
            labels: vec![],
            sequencer: OperationSequence::new(request.id.clone()),
            operation: None,
        }
    }
}
impl PartialEq<CreatePool> for PoolSpec {
    fn eq(&self, other: &CreatePool) -> bool {
        let mut other = PoolSpec::from(other);
        other.status = self.status.clone();
        other.sequencer = self.sequencer.clone();
        &other == self
    }
}

/// User specification of a pool.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct PoolSpec {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
    /// absolute disk paths claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
    /// status of the pool
    pub status: PoolSpecStatus,
    /// Pool labels.
    pub labels: Vec<PoolLabel>,
    /// Update in progress
    #[serde(skip)]
    pub sequencer: OperationSequence,
    /// Record of the operation in progress
    pub operation: Option<PoolOperationState>,
}

macro_rules! pool_span {
    ($Self:tt, $Level:expr, $func:expr) => {
        match tracing::Span::current().field("pool.uuid") {
            None => {
                let _span = tracing::span!($Level, "log_event", pool.uuid = %$Self.id).entered();
                $func();
            }
            Some(_) => {
                $func();
            }
        }
    };
}
crate::impl_trace_span!(pool_span, PoolSpec);

impl OperationSequencer for PoolSpec {
    fn as_ref(&self) -> &OperationSequence {
        &self.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.sequencer
    }
}

impl UuidString for PoolSpec {
    fn uuid_as_string(&self) -> String {
        self.id.clone().into()
    }
}

impl From<PoolSpec> for models::PoolSpec {
    fn from(src: PoolSpec) -> Self {
        Self::new(src.disks, src.id, src.labels, src.node, src.status)
    }
}
impl From<models::PoolSpec> for PoolSpec {
    fn from(src: models::PoolSpec) -> Self {
        Self {
            node: src.node.into(),
            id: src.id.into(),
            disks: src.disks.into_vec(),
            status: src.status.into(),
            labels: src.labels,
            sequencer: Default::default(),
            operation: None,
        }
    }
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
                    self.status = SpecStatus::Deleted;
                }
                PoolOperation::Create => {
                    self.status = SpecStatus::Created(message_bus::PoolStatus::Online);
                }
            }
        }
        self.clear_op();
    }

    fn clear_op(&mut self) {
        self.operation = None;
    }

    fn start_op(&mut self, operation: PoolOperation) {
        self.operation = Some(PoolOperationState {
            operation,
            result: None,
        })
    }

    fn set_op_result(&mut self, result: bool) {
        if let Some(op) = &mut self.operation {
            op.result = Some(result);
        }
    }
}

/// Available Pool Operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum PoolOperation {
    Create,
    Destroy,
}

impl PartialEq<message_bus::PoolState> for PoolSpec {
    fn eq(&self, other: &message_bus::PoolState) -> bool {
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

impl From<&PoolSpec> for message_bus::PoolState {
    fn from(pool: &PoolSpec) -> Self {
        Self {
            node: pool.node.clone(),
            id: pool.id.clone(),
            disks: pool.disks.clone(),
            status: message_bus::PoolStatus::Unknown,
            capacity: 0,
            used: 0,
        }
    }
}
