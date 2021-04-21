//! Definition of replica types that can be saved to the persistent store.

use crate::{
    store::{ObjectKey, StorableObject, StorableObjectType},
    types::SpecState,
};
use mbus_api::{v0, v0::ReplicaId};
use serde::{Deserialize, Serialize};

/// Replica information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Replica {
    /// Current state of the replica.
    pub state: Option<ReplicaState>,
    /// Desired replica specification.
    pub spec: ReplicaSpec,
}

/// Runtime state of a replica.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ReplicaState {
    /// Replica information.
    pub replica: v0::Replica,
    /// State of the replica.
    pub state: v0::ReplicaState,
}

/// Key used by the store to uniquely identify a ReplicaState structure.
pub struct ReplicaStateKey(ReplicaId);

impl ObjectKey for ReplicaStateKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::ReplicaState
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for ReplicaState {
    type Key = ReplicaStateKey;

    fn key(&self) -> Self::Key {
        ReplicaStateKey(self.replica.uuid.clone())
    }
}

/// User specification of a replica.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ReplicaSpec {
    /// uuid of the replica
    pub uuid: v0::ReplicaId,
    /// The size that the replica should be.
    pub size: u64,
    /// The pool that the replica should live on.
    pub pool: v0::PoolId,
    /// Protocol used for exposing the replica.
    pub share: v0::Protocol,
    /// Thin provisioning.
    pub thin: bool,
    /// The state that the replica should eventually achieve.
    pub state: ReplicaSpecState,
    /// Managed by our control plane
    pub managed: bool,
    /// Owner Resource
    pub owners: v0::ReplicaOwners,
    /// Update in progress
    #[serde(skip)]
    pub updating: bool,
}

/// Key used by the store to uniquely identify a ReplicaSpec structure.
pub struct ReplicaSpecKey(ReplicaId);

impl ObjectKey for ReplicaSpecKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::ReplicaSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl From<&ReplicaId> for ReplicaSpecKey {
    fn from(id: &ReplicaId) -> Self {
        ReplicaSpecKey(id.clone())
    }
}

impl StorableObject for ReplicaSpec {
    type Key = ReplicaSpecKey;

    fn key(&self) -> Self::Key {
        ReplicaSpecKey(self.uuid.clone())
    }
}

impl From<&ReplicaSpec> for v0::Replica {
    fn from(replica: &ReplicaSpec) -> Self {
        Self {
            node: v0::NodeId::default(),
            uuid: replica.uuid.clone(),
            pool: replica.pool.clone(),
            thin: replica.thin,
            size: replica.size,
            share: replica.share.clone(),
            uri: "".to_string(),
            state: v0::ReplicaState::Unknown,
        }
    }
}

/// State of the Replica Spec
pub type ReplicaSpecState = SpecState<v0::ReplicaState>;

impl From<&v0::CreateReplica> for ReplicaSpec {
    fn from(request: &v0::CreateReplica) -> Self {
        Self {
            uuid: request.uuid.clone(),
            size: request.size,
            pool: request.pool.clone(),
            share: request.share.clone(),
            thin: request.thin,
            state: ReplicaSpecState::Creating,
            managed: request.managed,
            owners: request.owners.clone(),
            updating: true,
        }
    }
}
impl PartialEq<v0::CreateReplica> for ReplicaSpec {
    fn eq(&self, other: &v0::CreateReplica) -> bool {
        let mut other = ReplicaSpec::from(other);
        other.state = self.state.clone();
        other.updating = self.updating;
        &other == self
    }
}
