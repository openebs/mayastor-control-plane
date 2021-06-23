//! Definition of child types that can be saved to the persistent store.

use crate::v0::{
    message_bus::{mbus, mbus::ReplicaId},
    store::definitions::{ObjectKey, StorableObject, StorableObjectType},
};
use serde::{Deserialize, Serialize};

/// Child information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Child {
    /// Current state of the child.
    pub state: Option<ChildState>,
    /// Desired child specification.
    pub spec: ChildSpec,
}

/// Runtime state of a child.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ChildState {
    pub child: mbus::Child,
    /// Size of the child.
    pub size: u64,
    /// UUID of the replica that the child connects to.
    pub replica_uuid: ReplicaId,
}

/// Key used by the store to uniquely identify a ChildState structure.
/// The child is identified through the replica ID.
pub struct ChildStateKey(ReplicaId);

impl From<&ReplicaId> for ChildStateKey {
    fn from(id: &ReplicaId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for ChildStateKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::ChildState
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for ChildState {
    type Key = ChildStateKey;

    fn key(&self) -> Self::Key {
        ChildStateKey(self.replica_uuid.clone())
    }
}

/// User specification of a child.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ChildSpec {
    /// The size the child should be.
    pub size: u64,
    /// The UUID of the replica the child should be associated with.
    pub replica_uuid: ReplicaId,
    /// The state the child should eventually reach.
    pub state: mbus::ChildState,
}

/// Key used by the store to uniquely identify a ChildSpec structure.
/// The child is identified through the replica ID.
pub struct ChildSpecKey(ReplicaId);

impl From<&ReplicaId> for ChildSpecKey {
    fn from(id: &ReplicaId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for ChildSpecKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::ChildSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for ChildSpec {
    type Key = ChildSpecKey;

    fn key(&self) -> Self::Key {
        ChildSpecKey(self.replica_uuid.clone())
    }
}
