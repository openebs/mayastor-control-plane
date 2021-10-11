//! Definition of nexus types that can be saved to the persistent store.

use crate::types::v0::{
    message_bus::{
        self, ChildState, ChildUri, CreateNexus, DestroyNexus, Nexus as MbusNexus, NexusId,
        NexusShareProtocol, NodeId, Protocol, ReplicaId, VolumeId,
    },
    openapi::models,
    store::{
        definitions::{ObjectKey, StorableObject, StorableObjectType},
        nexus_child::NexusChild,
        ResourceUuid, SpecStatus, SpecTransaction,
    },
};

use crate::types::v0::store::{OperationSequence, OperationSequencer};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

/// Nexus information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Nexus {
    /// Current state of the nexus.
    pub status: Option<message_bus::NexusStatus>,
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

impl ResourceUuid for NexusState {
    type Id = NexusId;
    fn uuid(&self) -> Self::Id {
        self.nexus.uuid.clone()
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

/// Status of the Nexus Spec
pub type NexusSpecStatus = SpecStatus<message_bus::NexusStatus>;

/// User specification of a nexus.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct NexusSpec {
    /// Nexus Id
    pub uuid: NexusId,
    /// Node where the nexus should live.
    pub node: NodeId,
    /// List of children.
    pub children: Vec<NexusChild>,
    /// Size of the nexus.
    pub size: u64,
    /// The status the nexus spec.
    pub spec_status: NexusSpecStatus,
    /// Share Protocol
    pub share: Protocol,
    /// Managed by our control plane
    pub managed: bool,
    /// Volume which owns this nexus, if any
    pub owner: Option<VolumeId>,
    /// Update of the state in progress
    #[serde(skip)]
    pub sequencer: OperationSequence,
    /// Record of the operation in progress
    pub operation: Option<NexusOperationState>,
}
impl NexusSpec {
    /// Check if the spec contains the provided replica by it's `ReplicaId`
    pub fn contains_replica(&self, uuid: &ReplicaId) -> bool {
        self.children.iter().any(|child| match child {
            NexusChild::Replica(replica) => &replica.uuid == uuid,
            NexusChild::Uri(_) => false,
        })
    }
    /// Disown nexus by its volume owner
    pub fn disowned_by_volume(&mut self) {
        let _ = self.owner.take();
    }
}

macro_rules! nexus_log {
    ($Self:tt, $Level:expr, $Message:tt) => {
        match tracing::Span::current().field("nexus.uuid") {
            None => {
                if let Some(volume_uuid) = &$Self.owner {
                    let _span = tracing::span!($Level, "log_event", volume.uuid = %volume_uuid, nexus.uuid = %$Self.uuid).entered();
                    tracing::event!($Level, volume.uuid = %volume_uuid, nexus.uuid = %$Self.uuid, $Message);
                } else {
                    let _span = tracing::span!($Level, "log_event", nexus.uuid = %$Self.uuid).entered();
                    tracing::event!($Level, nexus.uuid = %$Self.uuid, $Message);
                }
            }
            Some(_) => {
                if let Some(volume_uuid) = &$Self.owner {
                    tracing::event!($Level, volume.uuid = %volume_uuid, nexus.uuid = %$Self.uuid, $Message);
                } else {
                    tracing::event!($Level, nexus.uuid = %$Self.uuid, $Message);
                }
            }
        }
    };
}
crate::impl_trace_str_log!(nexus_log, NexusSpec);

macro_rules! nexus_span {
    ($Self:tt, $Level:expr, $func:expr) => {
        match tracing::Span::current().field("nexus.uuid") {
            None => {
                if let Some(volume_uuid) = &$Self.owner {
                    let _span = tracing::span!($Level, "log_event", volume.uuid = %volume_uuid, nexus.uuid = %$Self.uuid, nexus.node.uuid = %$Self.node).entered();
                    $func();
                } else {
                    let _span = tracing::span!($Level, "log_event", nexus.uuid = %$Self.uuid, nexus.node.uuid = %$Self.node).entered();
                    $func();
                }
            }
            Some(_) => {
                $func();
            }
        }
    };
}
crate::impl_trace_span!(nexus_span, NexusSpec);

impl From<&NexusSpec> for CreateNexus {
    fn from(spec: &NexusSpec) -> Self {
        CreateNexus::new(
            &spec.node,
            &spec.uuid,
            spec.size,
            &spec.children,
            spec.managed,
            spec.owner.as_ref(),
        )
    }
}

impl OperationSequencer for NexusSpec {
    fn as_ref(&self) -> &OperationSequence {
        &self.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.sequencer
    }
}

impl ResourceUuid for NexusSpec {
    type Id = NexusId;
    fn uuid(&self) -> Self::Id {
        self.uuid.clone()
    }
}

impl From<NexusSpec> for models::NexusSpec {
    fn from(src: NexusSpec) -> Self {
        Self::new(
            src.children,
            src.managed,
            src.node,
            src.share,
            src.size,
            src.spec_status,
            openapi::apis::Uuid::try_from(src.uuid).unwrap(),
        )
    }
}

/// ReplicaUri used by managed nexus creation
/// Includes the ReplicaId which is unique and allows us to pinpoint the exact replica
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct ReplicaUri {
    uuid: ReplicaId,
    share_uri: ChildUri,
}

impl ReplicaUri {
    /// Create a new ReplicaUri from a replicaId and a share nvmf URI
    pub fn new(uuid: &ReplicaId, share_uri: &ChildUri) -> Self {
        Self {
            uuid: uuid.clone(),
            share_uri: share_uri.clone(),
        }
    }
    /// Get the replica uuid
    pub fn uuid(&self) -> &ReplicaId {
        &self.uuid
    }
    /// Get the replica uri
    pub fn uri(&self) -> &ChildUri {
        &self.share_uri
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
                    self.spec_status = SpecStatus::Deleted;
                }
                NexusOperation::Create => {
                    self.spec_status = SpecStatus::Created(message_bus::NexusStatus::Online);
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
    }

    fn start_op(&mut self, operation: NexusOperation) {
        self.operation = Some(NexusOperationState {
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

/// Available Nexus Operations
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum NexusOperation {
    Create,
    Destroy,
    Share(NexusShareProtocol),
    Unshare,
    AddChild(NexusChild),
    RemoveChild(NexusChild),
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
            spec_status: NexusSpecStatus::Creating,
            share: Protocol::None,
            managed: request.managed,
            owner: request.owner.clone(),
            sequencer: OperationSequence::new(request.uuid.clone()),
            operation: None,
        }
    }
}

impl PartialEq<CreateNexus> for NexusSpec {
    fn eq(&self, other: &CreateNexus) -> bool {
        let mut other = NexusSpec::from(other);
        other.spec_status = self.spec_status.clone();
        other.sequencer = self.sequencer.clone();
        &other == self
    }
}
impl PartialEq<message_bus::Nexus> for NexusSpec {
    fn eq(&self, status: &message_bus::Nexus) -> bool {
        self.share == status.share && self.children == status.children && self.node == status.node
    }
}

impl From<&NexusSpec> for message_bus::Nexus {
    fn from(nexus: &NexusSpec) -> Self {
        Self {
            node: nexus.node.clone(),
            uuid: nexus.uuid.clone(),
            size: nexus.size,
            status: message_bus::NexusStatus::Unknown,
            children: nexus
                .children
                .iter()
                .map(|child| message_bus::Child {
                    uri: child.uri(),
                    state: ChildState::Unknown,
                    rebuild_progress: None,
                })
                .collect(),
            device_uri: "".to_string(),
            rebuilds: 0,
            share: nexus.share,
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
