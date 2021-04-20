//! Definition of nexus types that can be saved to the persistent store.

use crate::{
    store::{ObjectKey, StorableObject, StorableObjectType},
    types::SpecState,
};
use mbus_api::{v0, v0::NexusId};
use serde::{Deserialize, Serialize};

/// Nexus information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Nexus {
    /// Current state of the nexus.
    pub state: Option<NexusState>,
    /// Desired nexus specification.
    pub spec: NexusSpec,
}

/// Runtime state of the nexus.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct NexusState {
    /// Nexus information.
    pub nexus: v0::Nexus,
}

/// Key used by the store to uniquely identify a NexusState structure.
pub struct NexusStateKey(NexusId);

impl From<&NexusId> for NexusStateKey {
    fn from(id: &NexusId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for NexusStateKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::NexusState
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for NexusState {
    type Key = NexusStateKey;

    fn key(&self) -> Self::Key {
        NexusStateKey(self.nexus.uuid.clone())
    }
}

/// State of the Nexus Spec
pub type NexusSpecState = SpecState<v0::NexusState>;

/// User specification of a nexus.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NexusSpec {
    /// Nexus Id
    pub uuid: v0::NexusId,
    /// Node where the nexus should live.
    pub node: v0::NodeId,
    /// List of children.
    pub children: Vec<v0::ChildUri>,
    /// Size of the nexus.
    pub size: u64,
    /// The state the nexus should eventually reach.
    pub state: NexusSpecState,
    /// Share Protocol
    pub share: v0::Protocol,
    /// Managed by our control plane
    pub managed: bool,
    /// Volume which owns this nexus, if any
    pub owner: Option<v0::VolumeId>,
    /// Update of the state in progress
    #[serde(skip)]
    pub updating: bool,
}

/// Key used by the store to uniquely identify a NexusSpec structure.
pub struct NexusSpecKey(NexusId);

impl From<&NexusId> for NexusSpecKey {
    fn from(id: &NexusId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for NexusSpecKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::NexusSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for NexusSpec {
    type Key = NexusSpecKey;

    fn key(&self) -> Self::Key {
        NexusSpecKey(self.uuid.clone())
    }
}

impl From<&v0::CreateNexus> for NexusSpec {
    fn from(request: &v0::CreateNexus) -> Self {
        Self {
            uuid: request.uuid.clone(),
            node: request.node.clone(),
            children: request.children.clone(),
            size: request.size,
            state: NexusSpecState::Creating,
            share: v0::Protocol::Off,
            managed: request.managed,
            owner: request.owner.clone(),
            updating: true,
        }
    }
}

impl PartialEq<v0::CreateNexus> for NexusSpec {
    fn eq(&self, other: &v0::CreateNexus) -> bool {
        let mut other = NexusSpec::from(other);
        other.state = self.state.clone();
        other.updating = self.updating;
        &other == self
    }
}

impl From<&NexusSpec> for v0::Nexus {
    fn from(nexus: &NexusSpec) -> Self {
        Self {
            node: nexus.node.clone(),
            uuid: nexus.uuid.clone(),
            size: nexus.size,
            state: v0::NexusState::Unknown,
            children: nexus
                .children
                .iter()
                .map(|uri| v0::Child {
                    uri: uri.clone(),
                    state: v0::ChildState::Unknown,
                    rebuild_progress: None,
                })
                .collect(),
            device_uri: "".to_string(),
            rebuilds: 0,
        }
    }
}

impl From<NexusSpec> for v0::DestroyNexus {
    fn from(nexus: NexusSpec) -> Self {
        Self {
            node: nexus.node,
            uuid: nexus.uuid,
        }
    }
}
