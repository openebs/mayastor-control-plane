//! Definition of nexus types that can be saved to the persistent store.

use crate::types::v0::{
    message_bus::{
        self, ChildState, ChildUri, CreateNexus, DestroyNexus, Nexus as MbusNexus, NexusId,
        NexusShareProtocol, NodeId, Protocol, VolumeId,
    },
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        SpecState, SpecTransaction,
    },
};

use crate::types::v0::{openapi::models, store::UuidString};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// Nexus information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Nexus {
    /// Current state of the nexus.
    pub state: Option<message_bus::NexusState>,
    /// Desired nexus specification.
    pub spec: NexusSpec,
}

/// Runtime state of the nexus.
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct NexusState {
    /// Nexus information.
    pub nexus: message_bus::Nexus,
}

impl From<MbusNexus> for NexusState {
    fn from(nexus: MbusNexus) -> Self {
        Self { nexus }
    }
}

impl UuidString for NexusState {
    fn uuid_as_string(&self) -> String {
        self.nexus.uuid.clone().into()
    }
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
pub type NexusSpecState = SpecState<message_bus::NexusState>;

/// User specification of a nexus.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct NexusSpec {
    /// Nexus Id
    pub uuid: NexusId,
    /// Node where the nexus should live.
    pub node: NodeId,
    /// List of children.
    pub children: Vec<ChildUri>,
    /// Size of the nexus.
    pub size: u64,
    /// The state the nexus should eventually reach.
    pub state: NexusSpecState,
    /// Share Protocol
    pub share: Protocol,
    /// Managed by our control plane
    pub managed: bool,
    /// Volume which owns this nexus, if any
    pub owner: Option<VolumeId>,
    /// Update of the state in progress
    #[serde(skip)]
    pub updating: bool,
    /// Record of the operation in progress
    pub operation: Option<NexusOperationState>,
}

impl UuidString for NexusSpec {
    fn uuid_as_string(&self) -> String {
        self.uuid.clone().into()
    }
}

impl From<NexusSpec> for models::NexusSpec {
    fn from(src: NexusSpec) -> Self {
        Self::new(
            src.children,
            src.managed,
            src.node,
            src.share,
            src.size as i64,
            src.state,
            openapi::apis::Uuid::try_from(src.uuid).unwrap(),
        )
    }
}

/// Operation State for a Nexus spec resource
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NexusOperationState {
    /// Record of the operation
    pub operation: NexusOperation,
    /// Result of the operation
    pub result: Option<bool>,
}

impl SpecTransaction<NexusOperation> for NexusSpec {
    fn pending_op(&self) -> bool {
        self.operation.is_some()
    }

    fn commit_op(&mut self) {
        if let Some(op) = self.operation.clone() {
            match op.operation {
                NexusOperation::Destroy => {
                    self.state = SpecState::Deleted;
                }
                NexusOperation::Create => {
                    self.state = SpecState::Created(message_bus::NexusState::Online);
                }
                NexusOperation::Share(share) => {
                    self.share = share.into();
                }
                NexusOperation::Unshare => {
                    self.share = Protocol::None;
                }
                NexusOperation::AddChild(uri) => self.children.push(uri),
                NexusOperation::RemoveChild(uri) => self.children.retain(|c| c != &uri),
            }
        }
        self.clear_op();
    }

    fn clear_op(&mut self) {
        self.operation = None;
        self.updating = false;
    }

    fn start_op(&mut self, operation: NexusOperation) {
        self.updating = true;
        self.operation = Some(NexusOperationState {
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

/// Available Nexus Operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum NexusOperation {
    Create,
    Destroy,
    Share(NexusShareProtocol),
    Unshare,
    AddChild(ChildUri),
    RemoveChild(ChildUri),
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

impl From<&CreateNexus> for NexusSpec {
    fn from(request: &CreateNexus) -> Self {
        Self {
            uuid: request.uuid.clone(),
            node: request.node.clone(),
            children: request.children.clone(),
            size: request.size,
            state: NexusSpecState::Creating,
            share: Protocol::None,
            managed: request.managed,
            owner: request.owner.clone(),
            updating: false,
            operation: None,
        }
    }
}

impl PartialEq<CreateNexus> for NexusSpec {
    fn eq(&self, other: &CreateNexus) -> bool {
        let mut other = NexusSpec::from(other);
        other.state = self.state.clone();
        other.updating = self.updating;
        &other == self
    }
}
impl PartialEq<message_bus::Nexus> for NexusSpec {
    fn eq(&self, other: &message_bus::Nexus) -> bool {
        self.share == other.share && self.children == other.children && self.node == other.node
    }
}

impl From<&NexusSpec> for message_bus::Nexus {
    fn from(nexus: &NexusSpec) -> Self {
        Self {
            node: nexus.node.clone(),
            uuid: nexus.uuid.clone(),
            size: nexus.size,
            state: message_bus::NexusState::Unknown,
            children: nexus
                .children
                .iter()
                .map(|uri| message_bus::Child {
                    uri: uri.clone(),
                    state: ChildState::Unknown,
                    rebuild_progress: None,
                })
                .collect(),
            device_uri: "".to_string(),
            rebuilds: 0,
            share: nexus.share.clone(),
        }
    }
}

impl From<NexusSpec> for DestroyNexus {
    fn from(nexus: NexusSpec) -> Self {
        Self {
            node: nexus.node,
            uuid: nexus.uuid,
        }
    }
}
