use crate::types::v0::{
    message_bus::{NexusId, ReplicaId},
    store::definitions::{ObjectKey, StorableObject, StorableObjectType},
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Definition of the nexus information that gets saved in the persistent
/// store.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct NexusInfo {
    #[serde(skip)]
    /// uuid of the Nexus
    pub uuid: NexusId,
    /// Nexus destroyed successfully.
    pub clean_shutdown: bool,
    /// Information about children.
    pub children: Vec<ChildInfo>,
}

impl NexusInfo {
    /// Check if the provided replica is healthy or not
    pub fn is_replica_healthy(&self, replica: &ReplicaId) -> bool {
        match self.children.iter().find(|c| c.uuid == replica.as_str()) {
            Some(info) => info.healthy,
            None => false,
        }
    }
    /// Check if no replica is healthy
    pub fn no_healthy_replicas(&self) -> bool {
        self.children.iter().all(|c| !c.healthy) || self.children.is_empty()
    }
}

/// Definition of the child information that gets saved in the persistent
/// store.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ChildInfo {
    /// UUID of the child.
    pub uuid: String,
    /// Child's state of health.
    pub healthy: bool,
}

/// Key used by the store to uniquely identify a NexusInfo structure.
pub struct NexusInfoKey(NexusId);

impl From<&NexusId> for NexusInfoKey {
    fn from(id: &NexusId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for NexusInfoKey {
    fn key(&self) -> String {
        // no key prefix (as it's written by mayastor)
        self.key_uuid()
    }

    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::NexusInfo
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for NexusInfo {
    type Key = NexusInfoKey;

    fn key(&self) -> Self::Key {
        NexusInfoKey(self.uuid.clone())
    }
}
