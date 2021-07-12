//! Definition of volume types that can be saved to the persistent store.

use crate::types::v0::{
    message_bus::{self, CreateVolume, NexusId, NodeId, Protocol, VolumeId, VolumeShareProtocol},
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        SpecState, SpecTransaction,
    },
};

use crate::types::v0::openapi::models;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

type VolumeLabel = String;

/// Volume information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Volume {
    /// Current state of the volume.
    pub state: Option<VolumeState>,
    /// Desired volume specification.
    pub spec: VolumeSpec,
}

/// Runtime state of the volume.
/// This should eventually satisfy the VolumeSpec.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct VolumeState {
    /// Volume Id
    pub uuid: VolumeId,
    /// Volume size.
    pub size: u64,
    /// Volume labels.
    pub labels: Vec<VolumeLabel>,
    /// Number of replicas.
    pub num_replicas: u8,
    /// Protocol that the volume is shared over.
    pub protocol: Protocol,
    /// Nexuses that make up the volume.
    pub nexuses: Vec<NexusId>,
    /// Number of front-end paths.
    pub num_paths: u8,
    /// State of the volume.
    pub state: message_bus::VolumeState,
}

/// Key used by the store to uniquely identify a VolumeState structure.
pub struct VolumeStateKey(VolumeId);

impl From<&VolumeId> for VolumeStateKey {
    fn from(id: &VolumeId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for VolumeStateKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::VolumeState
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for VolumeState {
    type Key = VolumeStateKey;

    fn key(&self) -> Self::Key {
        VolumeStateKey(self.uuid.clone())
    }
}

/// User specification of a volume.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VolumeSpec {
    /// Volume Id
    pub uuid: VolumeId,
    /// Size that the volume should be.
    pub size: u64,
    /// Volume labels.
    pub labels: Vec<VolumeLabel>,
    /// Number of children the volume should have.
    pub num_replicas: u8,
    /// Protocol that the volume should be shared over.
    pub protocol: Protocol,
    /// Number of front-end paths.
    pub num_paths: u8,
    /// State that the volume should eventually achieve.
    pub state: VolumeSpecState,
    /// The node where front-end IO will be sent to
    pub target_node: Option<NodeId>,
    /// Update of the state in progress
    #[serde(skip)]
    pub updating: bool,
    /// Record of the operation in progress
    pub operation: Option<VolumeOperationState>,
}

/// Operation State for a Nexus spec resource
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VolumeOperationState {
    /// Record of the operation
    pub operation: VolumeOperation,
    /// Result of the operation
    pub result: Option<bool>,
}

impl SpecTransaction<VolumeOperation> for VolumeSpec {
    fn pending_op(&self) -> bool {
        self.operation.is_some()
    }

    fn commit_op(&mut self) {
        if let Some(op) = self.operation.clone() {
            match op.operation {
                VolumeOperation::Destroy => {
                    self.state = SpecState::Deleted;
                }
                VolumeOperation::Create => {
                    self.state = SpecState::Created(message_bus::VolumeState::Online);
                }
                VolumeOperation::Share(share) => {
                    self.protocol = share.into();
                }
                VolumeOperation::Unshare => {
                    self.protocol = Protocol::None;
                }
                VolumeOperation::AddReplica => self.num_replicas += 1,
                VolumeOperation::RemoveReplica => self.num_replicas -= 1,
                VolumeOperation::Publish((node, share)) => {
                    self.target_node = Some(node);
                    self.protocol = share.map_or(Protocol::None, Protocol::from);
                }
                VolumeOperation::Unpublish => {
                    self.target_node = None;
                    self.protocol = Protocol::None;
                }
            }
        }
        self.clear_op();
    }

    fn clear_op(&mut self) {
        self.operation = None;
        self.updating = false;
    }

    fn start_op(&mut self, operation: VolumeOperation) {
        self.updating = true;
        self.operation = Some(VolumeOperationState {
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

/// Available Volume Operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum VolumeOperation {
    Create,
    Destroy,
    Share(VolumeShareProtocol),
    Unshare,
    AddReplica,
    RemoveReplica,
    Publish((NodeId, Option<VolumeShareProtocol>)),
    Unpublish,
}

/// Key used by the store to uniquely identify a VolumeSpec structure.
pub struct VolumeSpecKey(VolumeId);

impl From<&VolumeId> for VolumeSpecKey {
    fn from(id: &VolumeId) -> Self {
        Self(id.clone())
    }
}

impl ObjectKey for VolumeSpecKey {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::VolumeSpec
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

impl StorableObject for VolumeSpec {
    type Key = VolumeSpecKey;

    fn key(&self) -> Self::Key {
        VolumeSpecKey(self.uuid.clone())
    }
}

/// State of the Volume Spec
pub type VolumeSpecState = SpecState<message_bus::VolumeState>;

impl From<&CreateVolume> for VolumeSpec {
    fn from(request: &CreateVolume) -> Self {
        Self {
            uuid: request.uuid.clone(),
            size: request.size,
            labels: vec![],
            num_replicas: request.replicas as u8,
            protocol: Protocol::None,
            num_paths: 1,
            state: VolumeSpecState::Creating,
            target_node: None,
            updating: false,
            operation: None,
        }
    }
}
impl PartialEq<CreateVolume> for VolumeSpec {
    fn eq(&self, other: &CreateVolume) -> bool {
        let mut other = VolumeSpec::from(other);
        other.state = self.state.clone();
        other.updating = self.updating;
        &other == self
    }
}
impl From<&VolumeSpec> for message_bus::Volume {
    fn from(spec: &VolumeSpec) -> Self {
        Self {
            uuid: spec.uuid.clone(),
            size: spec.size,
            state: message_bus::VolumeState::Unknown,
            protocol: spec.protocol.clone(),
            children: vec![],
        }
    }
}
impl PartialEq<message_bus::Volume> for VolumeSpec {
    fn eq(&self, other: &message_bus::Volume) -> bool {
        self.protocol == other.protocol
            && match &self.target_node {
                None => other.target_node().flatten().is_none(),
                Some(node) => {
                    self.num_paths as usize == other.children.len()
                        && Some(node) == other.target_node().flatten().as_ref()
                }
            }
    }
}

impl From<VolumeSpec> for models::VolumeSpec {
    fn from(src: VolumeSpec) -> Self {
        Self::new(
            src.labels,
            src.num_paths as i32,
            src.num_replicas as i32,
            src.protocol,
            src.size as i64,
            src.state,
            openapi::apis::Uuid::try_from(src.uuid).unwrap(),
        )
    }
}
