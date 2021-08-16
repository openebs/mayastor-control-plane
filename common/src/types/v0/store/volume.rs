//! Definition of volume types that can be saved to the persistent store.

use crate::types::v0::{
    message_bus::{self, CreateVolume, NexusId, NodeId, Protocol, VolumeId, VolumeShareProtocol},
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        SpecStatus, SpecTransaction,
    },
};

use crate::types::v0::{
    message_bus::{ReplicaId, Topology, VolumeHealPolicy, VolumeStatus},
    openapi::models,
    store::{OperationSequence, OperationSequencer, UuidString},
};
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
    /// Status of the volume.
    pub status: message_bus::VolumeStatus,
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
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
    /// Status that the volume should eventually achieve.
    pub status: VolumeSpecStatus,
    /// The node where front-end IO will be sent to
    pub target_node: Option<NodeId>,
    /// volume healing policy
    pub policy: VolumeHealPolicy,
    /// replica placement topology
    pub topology: Topology,
    /// Update of the state in progress
    #[serde(skip)]
    pub sequencer: OperationSequence,
    /// Id of the last Nexus used by the volume
    pub last_nexus_id: Option<NexusId>,
    /// Record of the operation in progress
    pub operation: Option<VolumeOperationState>,
}

impl OperationSequencer for VolumeSpec {
    fn as_ref(&self) -> &OperationSequence {
        &self.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.sequencer
    }
}

impl VolumeSpec {
    /// explicitly selected allowed_nodes
    pub fn allowed_nodes(&self) -> Vec<NodeId> {
        self.topology
            .explicit
            .clone()
            .unwrap_or_default()
            .allowed_nodes
    }
    /// target volume replica count if during `SetReplica` operation
    /// or otherwise the current num_replicas
    pub fn target_num_replicas(&self) -> u8 {
        match &self.operation {
            Some(operation) => match operation.operation {
                VolumeOperation::SetReplica(count) => count,
                _ => self.num_replicas,
            },
            _ => self.num_replicas,
        }
    }
}

impl UuidString for VolumeSpec {
    fn uuid_as_string(&self) -> String {
        self.uuid.clone().into()
    }
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
                    self.status = SpecStatus::Deleted;
                }
                VolumeOperation::Create => {
                    self.status = SpecStatus::Created(message_bus::VolumeStatus::Online);
                }
                VolumeOperation::Share(share) => {
                    self.protocol = share.into();
                }
                VolumeOperation::Unshare => {
                    self.protocol = Protocol::None;
                }
                VolumeOperation::SetReplica(count) => self.num_replicas = count,
                VolumeOperation::RemoveUnusedReplica(_) => {}
                VolumeOperation::Publish((node, nexus, share)) => {
                    self.target_node = Some(node);
                    self.last_nexus_id = Some(nexus);
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
    }

    fn start_op(&mut self, operation: VolumeOperation) {
        self.operation = Some(VolumeOperationState {
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

/// Available Volume Operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum VolumeOperation {
    Create,
    Destroy,
    Share(VolumeShareProtocol),
    Unshare,
    SetReplica(u8),
    Publish((NodeId, NexusId, Option<VolumeShareProtocol>)),
    Unpublish,
    RemoveUnusedReplica(ReplicaId),
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
pub type VolumeSpecStatus = SpecStatus<message_bus::VolumeStatus>;

impl From<models::SpecState> for VolumeSpecStatus {
    fn from(spec_state: models::SpecState) -> Self {
        match spec_state {
            models::SpecState::Creating => Self::Creating,
            models::SpecState::Created => Self::Created(message_bus::VolumeStatus::Unknown),
            models::SpecState::Deleting => Self::Deleting,
            models::SpecState::Deleted => Self::Deleted,
        }
    }
}

impl From<&CreateVolume> for VolumeSpec {
    fn from(request: &CreateVolume) -> Self {
        Self {
            uuid: request.uuid.clone(),
            size: request.size,
            labels: vec![],
            num_replicas: request.replicas as u8,
            protocol: Protocol::None,
            num_paths: 1,
            status: VolumeSpecStatus::Creating,
            target_node: None,
            policy: request.policy.clone(),
            topology: request.topology.clone(),
            sequencer: OperationSequence::new(request.uuid.clone()),
            last_nexus_id: None,
            operation: None,
        }
    }
}
impl PartialEq<CreateVolume> for VolumeSpec {
    fn eq(&self, other: &CreateVolume) -> bool {
        let mut other = VolumeSpec::from(other);
        other.status = self.status.clone();
        other.sequencer = self.sequencer.clone();
        &other == self
    }
}
impl From<&VolumeSpec> for message_bus::VolumeState {
    fn from(spec: &VolumeSpec) -> Self {
        Self {
            uuid: spec.uuid.clone(),
            size: spec.size,
            status: message_bus::VolumeStatus::Unknown,
            protocol: spec.protocol.clone(),
            children: vec![],
        }
    }
}
impl PartialEq<message_bus::VolumeState> for VolumeSpec {
    fn eq(&self, other: &message_bus::VolumeState) -> bool {
        self.protocol == other.protocol
            && match &self.target_node {
                None => other.target_node().flatten().is_none(),
                Some(node) => {
                    self.num_paths as usize == other.children.len()
                        && Some(node) == other.target_node().flatten().as_ref()
                        && other.status == VolumeStatus::Online
                }
            }
    }
}

impl From<VolumeSpec> for models::VolumeSpec {
    fn from(src: VolumeSpec) -> Self {
        Self::new(
            src.labels,
            src.num_paths,
            src.num_replicas,
            src.protocol,
            src.size,
            src.status,
            openapi::apis::Uuid::try_from(src.uuid).unwrap(),
        )
    }
}

impl From<models::VolumeSpec> for VolumeSpec {
    fn from(spec: models::VolumeSpec) -> Self {
        Self {
            uuid: spec.uuid.to_string().into(),
            size: spec.size,
            labels: spec.labels,
            num_replicas: spec.num_replicas,
            protocol: spec.protocol.into(),
            num_paths: spec.num_paths,
            status: spec.state.into(),
            target_node: spec.target_node.map(From::from),
            policy: Default::default(),
            topology: Default::default(),
            sequencer: OperationSequence::new(spec.uuid.to_string()),
            last_nexus_id: None,
            operation: None,
        }
    }
}
