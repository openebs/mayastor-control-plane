//! Definition of pool types that can be saved to the persistent store.

use crate::v0::{
    message_bus::{
        mbus,
        mbus::{CreatePool, NodeId, PoolDeviceUri, PoolId},
    },
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        SpecState,
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
            updating: true,
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
