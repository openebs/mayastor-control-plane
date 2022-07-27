//! Definition of volume types that can be saved to the persistent store.

use crate::types::v0::{
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        SpecStatus, SpecTransaction,
    },
    transport::{self, CreateVolume, NexusId, NodeId, VolumeId, VolumeShareProtocol},
};

use crate::{
    types::v0::{
        openapi::models,
        store::{OperationSequence, OperationSequencer, ResourceUuid},
        transport::{ReplicaId, Topology, VolumeLabels, VolumePolicy, VolumeStatus},
    },
    IntoOption,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

/// Volume Target (node and nexus)
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeTarget {
    /// The node where front-end IO will be sent to
    node: NodeId,
    /// The identification of the nexus where the frontend-IO will be sent to
    nexus: NexusId,
    /// The protocol to use on the target
    protocol: Option<VolumeShareProtocol>,
}
impl VolumeTarget {
    /// Create a new `Self` based on the given parameters
    pub fn new(node: NodeId, nexus: NexusId, protocol: Option<VolumeShareProtocol>) -> Self {
        Self {
            node,
            nexus,
            protocol,
        }
    }
    /// Get a reference to the node identification
    pub fn node(&self) -> &NodeId {
        &self.node
    }
    /// Get a reference to the nexus identification
    pub fn nexus(&self) -> &NexusId {
        &self.nexus
    }
    /// Get a reference to the volume protocol
    pub fn protocol(&self) -> Option<&VolumeShareProtocol> {
        self.protocol.as_ref()
    }
}
impl From<VolumeTarget> for models::VolumeTarget {
    fn from(src: VolumeTarget) -> Self {
        Self::new_all(src.node, src.protocol.into_opt())
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
    pub labels: Option<VolumeLabels>,
    /// Number of children the volume should have.
    pub num_replicas: u8,
    /// Status that the volume should eventually achieve.
    pub status: VolumeSpecStatus,
    /// The target where front-end IO will be sent to
    pub target: Option<VolumeTarget>,
    /// volume policy
    pub policy: VolumePolicy,
    /// replica placement topology for the volume creation only
    pub topology: Option<Topology>,
    /// Update of the state in progress
    #[serde(skip)]
    pub sequencer: OperationSequence,
    /// Id of the last Nexus used by the volume
    pub last_nexus_id: Option<NexusId>,
    /// Record of the operation in progress
    pub operation: Option<VolumeOperationState>,
    /// Flag indicating whether the volume should be thin provisioned
    pub thin: bool,
}

macro_rules! volume_log {
    ($Self:tt, $Level:expr, $Message:tt) => {
        match tracing::Span::current().field("volume.uuid") {
            None => {
                let _span = tracing::span!($Level, "log_event", volume.uuid = %$Self.uuid).entered();
                tracing::event!($Level, volume.uuid = %$Self.uuid, $Message);
            }
            Some(_) => {
                tracing::event!($Level, volume.uuid = %$Self.uuid, $Message);
            }
        }
    };
}
crate::impl_trace_str_log!(volume_log, VolumeSpec);

macro_rules! volume_span {
    ($Self:tt, $Level:expr, $func:expr) => {
        match tracing::Span::current().field("volume.uuid") {
            None => {
                let _span = tracing::span!($Level, "log_event", volume.uuid = %$Self.uuid).entered();
                $func();
            }
            Some(_) => {
                $func();
            }
        }
    };
}
crate::impl_trace_span!(volume_span, VolumeSpec);

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
        match &self.topology {
            None => vec![],
            Some(t) => t
                .explicit()
                .map(|t| t.allowed_nodes.clone())
                .unwrap_or_default(),
        }
    }
    /// desired volume replica count if during `SetReplica` operation
    /// or otherwise the current num_replicas
    pub fn desired_num_replicas(&self) -> u8 {
        match &self.operation {
            Some(operation) => match operation.operation {
                VolumeOperation::SetReplica(count) => count,
                _ => self.num_replicas,
            },
            _ => self.num_replicas,
        }
    }
}

impl ResourceUuid for VolumeSpec {
    type Id = VolumeId;
    fn uuid(&self) -> Self::Id {
        self.uuid.clone()
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

impl From<VolumeOperationState> for models::VolumeSpecOperation {
    fn from(src: VolumeOperationState) -> Self {
        models::VolumeSpecOperation::new_all(src.operation, src.result)
    }
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
                    self.status = SpecStatus::Created(transport::VolumeStatus::Online);
                }
                VolumeOperation::Share(share) => {
                    if let Some(target) = &mut self.target {
                        target.protocol = share.into();
                    }
                }
                VolumeOperation::Unshare => {
                    if let Some(target) = self.target.as_mut() {
                        target.protocol = None
                    }
                }
                VolumeOperation::SetReplica(count) => self.num_replicas = count,
                VolumeOperation::RemoveUnusedReplica(_) => {}
                VolumeOperation::Publish((node, nexus, protocol)) => {
                    self.target = Some(VolumeTarget::new(node, nexus.clone(), protocol));
                    self.last_nexus_id = Some(nexus);
                }
                VolumeOperation::Unpublish => {
                    self.target = None;
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

impl From<VolumeOperation> for models::volume_spec_operation::Operation {
    fn from(src: VolumeOperation) -> Self {
        match src {
            VolumeOperation::Create => models::volume_spec_operation::Operation::Create,
            VolumeOperation::Destroy => models::volume_spec_operation::Operation::Destroy,
            VolumeOperation::Share(_) => models::volume_spec_operation::Operation::Share,
            VolumeOperation::Unshare => models::volume_spec_operation::Operation::Unshare,
            VolumeOperation::SetReplica(_) => models::volume_spec_operation::Operation::SetReplica,
            VolumeOperation::Publish(_) => models::volume_spec_operation::Operation::Publish,
            VolumeOperation::Unpublish => models::volume_spec_operation::Operation::Unpublish,
            VolumeOperation::RemoveUnusedReplica(_) => {
                models::volume_spec_operation::Operation::RemoveUnusedReplica
            }
        }
    }
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
pub type VolumeSpecStatus = SpecStatus<transport::VolumeStatus>;

impl From<&CreateVolume> for VolumeSpec {
    fn from(request: &CreateVolume) -> Self {
        Self {
            uuid: request.uuid.clone(),
            size: request.size,
            labels: request.labels.clone(),
            num_replicas: request.replicas as u8,
            status: VolumeSpecStatus::Creating,
            target: None,
            policy: request.policy.clone(),
            topology: request.topology.clone(),
            sequencer: OperationSequence::new(request.uuid.clone()),
            last_nexus_id: None,
            operation: None,
            thin: request.thin,
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
impl From<&VolumeSpec> for transport::VolumeState {
    fn from(spec: &VolumeSpec) -> Self {
        Self {
            uuid: spec.uuid.clone(),
            size: spec.size,
            status: transport::VolumeStatus::Unknown,
            target: None,
            replica_topology: HashMap::new(),
        }
    }
}
impl PartialEq<transport::VolumeState> for VolumeSpec {
    fn eq(&self, other: &transport::VolumeState) -> bool {
        match &self.target {
            None => other.target_node().flatten().is_none(),
            Some(target) => {
                target.protocol == other.target_protocol()
                    && Some(&target.node) == other.target_node().flatten().as_ref()
                    && other.status == VolumeStatus::Online
            }
        }
    }
}

impl From<VolumeSpec> for models::VolumeSpec {
    fn from(src: VolumeSpec) -> Self {
        Self::new_all(
            src.labels,
            src.num_replicas,
            src.operation.into_opt(),
            src.size,
            src.status,
            src.target.into_opt(),
            src.uuid,
            src.topology.into_opt(),
            src.policy,
            src.thin,
        )
    }
}
