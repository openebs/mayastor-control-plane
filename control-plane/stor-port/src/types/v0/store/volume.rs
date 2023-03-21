//! Definition of volume types that can be saved to the persistent store.

use crate::{
    types::v0::{
        openapi::models,
        store::{
            definitions::{ObjectKey, StorableObject, StorableObjectType},
            AsOperationSequencer, OperationSequence, SpecStatus, SpecTransaction,
        },
        transport::{
            self, CreateVolume, HostNqn, NexusId, NexusNvmfConfig, NodeId, ReplicaId, Topology,
            VolumeGroup, VolumeId, VolumeLabels, VolumePolicy, VolumeShareProtocol, VolumeStatus,
        },
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
    type Kind = StorableObjectType;

    fn version(&self) -> u64 {
        0
    }
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::VolumeState
    }

    fn key_uuid(&self) -> String {
        self.0.to_string()
    }
}

/// Frontend nodes configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct FrontendConfig {
    host_acl: Vec<InitiatorAC>,
}
impl FrontendConfig {
    /// Create new `Self` based on the host access list.
    pub fn from_acls(host_acl: Vec<InitiatorAC>) -> Self {
        Self { host_acl }
    }
    /// Check if the nodename is allowed.
    pub fn nodename_allowed(&self, nodename: &str) -> bool {
        self.host_acl.is_empty() || self.host_acl.iter().any(|n| n.node_name() == nodename)
    }
    /// Get the node names of the acl.
    pub fn node_names(&self) -> Vec<String> {
        self.host_acl.iter().map(|h| h.node_name.clone()).collect()
    }
    /// Get the first node name.
    pub fn node_name(&self) -> Option<String> {
        self.host_acl.first().map(|h| h.node_name.clone())
    }
    /// Get the hostnqn's for the acl.
    pub fn node_nqns(&self) -> Vec<HostNqn> {
        self.host_acl.iter().map(|h| h.node_nqn.clone()).collect()
    }
}

/// Volume Frontend
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct InitiatorAC {
    /// The nodename where front-end IO will be sent from.
    node_name: String,
    /// The nvme nqn of the front-end node.
    node_nqn: HostNqn,
}
impl InitiatorAC {
    /// Get a new `Self` with the given parameters.
    pub fn new(node_name: String, node_nqn: HostNqn) -> Self {
        Self {
            node_name,
            node_nqn,
        }
    }
    /// Get the nodename of the front-end initiator.
    pub fn node_name(&self) -> &str {
        &self.node_name
    }
    /// Get the hostnqn of the front-end initiator.
    pub fn node_nqn(&self) -> &HostNqn {
        &self.node_nqn
    }
}

/// Volume Target (node and nexus)
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeTarget {
    /// The node where front-end IO will be sent to.
    node: NodeId,
    /// The identification of the nexus where the frontend-IO will be sent to.
    nexus: NexusId,
    /// The protocol to use on the target.
    protocol: Option<VolumeShareProtocol>,
}
impl VolumeTarget {
    /// Create a new `Self` based on the given parameters.
    pub fn new(node: NodeId, nexus: NexusId, protocol: Option<VolumeShareProtocol>) -> Self {
        Self {
            node,
            nexus,
            protocol,
        }
    }
    /// Get a reference to the node identification.
    pub fn node(&self) -> &NodeId {
        &self.node
    }
    /// Get a reference to the nexus identification.
    pub fn nexus(&self) -> &NexusId {
        &self.nexus
    }
    /// Get a reference to the volume protocol.
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
    /// Volume Id.
    pub uuid: VolumeId,
    /// Size that the volume should be.
    pub size: u64,
    /// Volume labels.
    pub labels: Option<VolumeLabels>,
    /// Number of children the volume should have.
    pub num_replicas: u8,
    /// Status that the volume should eventually achieve.
    pub status: VolumeSpecStatus,
    /// Volume policy.
    pub policy: VolumePolicy,
    /// Replica placement topology for the volume creation only.
    pub topology: Option<Topology>,
    /// Update of the state in progress.
    #[serde(skip)]
    pub sequencer: OperationSequence,
    /// Id of the last Nexus used by the volume.
    pub last_nexus_id: Option<NexusId>,
    /// Record of the operation in progress
    pub operation: Option<VolumeOperationState>,
    /// Flag indicating whether the volume should be thin provisioned.
    #[serde(default)]
    pub thin: bool,
    /// Last used Target or current Configuration.
    #[serde(default, rename = "target")]
    pub target_config: Option<TargetConfig>,
    /// The publish context of the volume.
    #[serde(default)]
    pub publish_context: Option<HashMap<String, String>>,
    /// Volume Group related information.
    #[serde(default)]
    pub volume_group: Option<VolumeGroup>,
}

/// The volume's Nvmf Configuration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct TargetConfig {
    /// Back-compat target parameters.
    #[serde(flatten)]
    target: VolumeTarget,
    /// Indicates whether the target is active.
    /// We keep this config even if the target is no longer active so we can inspect
    /// what the previous config was like.
    #[serde(default = "default_active")]
    active: bool,
    /// The nexus nvmf configuration.
    #[serde(default)]
    config: NexusNvmfConfig,
    /// Config of frontend-nodes where IO will be sent from.
    #[serde(default)]
    frontend: FrontendConfig,
}

/// Default value for the active field in TargetConfig.
fn default_active() -> bool {
    true
}

impl TargetConfig {
    /// Get the uuid of the target.
    pub fn new(target: VolumeTarget, config: NexusNvmfConfig, frontend: FrontendConfig) -> Self {
        Self {
            target,
            active: true,
            config,
            frontend,
        }
    }

    /// Get the last target configuration.
    /// # Note: It may or may not the the current active target.
    pub fn target(&self) -> &VolumeTarget {
        &self.target
    }
    /// Get the active target.
    pub fn active_target(&self) -> Option<&VolumeTarget> {
        match self.active {
            true => Some(&self.target),
            false => None,
        }
    }
    /// Get the initiators.
    pub fn frontend(&self) -> &FrontendConfig {
        &self.frontend
    }
    /// Get the uuid of the target.
    pub fn uuid(&self) -> &NexusId {
        &self.target.nexus
    }
    /// Get the target's nvmf configuration.
    pub fn config(&self) -> &NexusNvmfConfig {
        &self.config
    }
}

impl AsOperationSequencer for VolumeSpec {
    fn as_ref(&self) -> &OperationSequence {
        &self.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.sequencer
    }
}

impl AsOperationSequencer for VolumeGroupSpec {
    fn as_ref(&self) -> &OperationSequence {
        &self.sequencer
    }

    fn as_mut(&mut self) -> &mut OperationSequence {
        &mut self.sequencer
    }
}

impl VolumeSpec {
    /// Get the currently active target.
    pub fn target(&self) -> Option<&VolumeTarget> {
        self.target_config
            .as_ref()
            .filter(|t| t.active)
            .map(|t| &t.target)
    }
    /// Get the target.
    pub fn target_mut(&mut self) -> Option<&mut VolumeTarget> {
        self.target_config.as_mut().map(|t| &mut t.target)
    }
    /// Explicitly selected allowed_nodes.
    pub fn allowed_nodes(&self) -> Vec<NodeId> {
        match &self.topology {
            None => vec![],
            Some(t) => t
                .explicit()
                .map(|t| t.allowed_nodes.clone())
                .unwrap_or_default(),
        }
    }
    /// Desired volume replica count if during `SetReplica` operation
    /// or otherwise the current num_replicas.
    pub fn desired_num_replicas(&self) -> u8 {
        match &self.operation {
            Some(operation) => match operation.operation {
                VolumeOperation::SetReplica(count) => count,
                _ => self.num_replicas,
            },
            _ => self.num_replicas,
        }
    }
    /// Get the last known target configuration.
    /// # Warning: the target may no longer be active.
    /// To get the current active target configuration use `VolumeSpec::active_config`.
    pub fn config(&self) -> &Option<TargetConfig> {
        &self.target_config
    }
    /// Get the current target configuration.
    pub fn active_config(&self) -> Option<&TargetConfig> {
        self.target_config.as_ref().filter(|t| t.active)
    }
    /// Deactivate the current target but keep it's information around.
    pub fn deactivate_target(&mut self) {
        if let Some(cfg) = self.target_config.as_mut() {
            cfg.active = false;
        }
    }
    /// Get the health info key which is used to retrieve the volume's replica health information.
    pub fn health_info_id(&self) -> Option<&NexusId> {
        if let Some(id) = &self.last_nexus_id {
            return Some(id);
        }
        self.target_config.as_ref().map(|c| &c.target.nexus)
    }
}

/// Operation State for a Nexus spec resource.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VolumeOperationState {
    /// Record of the operation.
    pub operation: VolumeOperation,
    /// Result of the operation.
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
                    if let Some(target) = self.target_mut() {
                        target.protocol = share.into();
                    }
                }
                VolumeOperation::Unshare => {
                    if let Some(target) = self.target_mut() {
                        target.protocol = None
                    }
                }
                VolumeOperation::SetReplica(count) => self.num_replicas = count,
                VolumeOperation::RemoveUnusedReplica(_) => {}
                VolumeOperation::PublishOld(args) => {
                    let (node, nexus, protocol) = (args.node, args.nexus, args.protocol);
                    let target = VolumeTarget::new(node, nexus, protocol);
                    self.last_nexus_id = None;
                    self.target_config = Some(TargetConfig::new(
                        target,
                        NexusNvmfConfig::default().with_no_resv(),
                        Default::default(),
                    ));
                }
                VolumeOperation::Publish(args) => {
                    self.last_nexus_id = None;
                    self.target_config = Some(args.config);
                    self.publish_context = Some(args.publish_context);
                }
                VolumeOperation::Republish(args) => {
                    self.last_nexus_id = None;
                    self.target_config = Some(args.config);
                }
                VolumeOperation::Unpublish => {
                    self.deactivate_target();
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

/// Available Volume Operations.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum VolumeOperation {
    Create,
    Destroy,
    Share(VolumeShareProtocol),
    Unshare,
    SetReplica(u8),
    #[serde(rename = "Publish")]
    PublishOld(OldPublishOperation),
    #[serde(rename = "Publish2")]
    Publish(PublishOperation),
    Republish(RepublishOperation),
    Unpublish,
    RemoveUnusedReplica(ReplicaId),
}

#[test]
fn volume_op_deserializer() {
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct TestSpec {
        op: VolumeOperation,
    }
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct Test<'a> {
        input: &'a str,
        expected: VolumeOperation,
    }
    let tests: Vec<Test> = vec![Test {
        input: r#"{"op":{"Publish":["4ffe7e43-46dd-4912-9d0f-6c9844fa7c6e","4ffe7e43-46dd-4912-9d0f-6c9844fa7c6e",null]}}"#,
        expected: VolumeOperation::PublishOld(OldPublishOperation {
            node: "4ffe7e43-46dd-4912-9d0f-6c9844fa7c6e".try_into().unwrap(),
            nexus: "4ffe7e43-46dd-4912-9d0f-6c9844fa7c6e".try_into().unwrap(),
            protocol: None,
        }),
    }];

    for test in &tests {
        println!("{}", serde_json::to_string(&test.expected).unwrap());
        let spec: TestSpec = serde_json::from_str(test.input).unwrap();
        assert_eq!(test.expected, spec.op);
    }
}

/// The `PublishOperation` which is easier to manage and update.
#[derive(serde_tuple::Serialize_tuple, serde_tuple::Deserialize_tuple, Debug, Clone, PartialEq)]
pub struct OldPublishOperation {
    node: NodeId,
    nexus: NexusId,
    protocol: Option<VolumeShareProtocol>,
}

/// The `PublishOperation` which is easier to manage and update.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PublishOperation {
    config: TargetConfig,
    publish_context: HashMap<String, String>,
}
impl PublishOperation {
    /// Return new `Self` from the given parameters.
    pub fn new(config: TargetConfig, publish_context: HashMap<String, String>) -> Self {
        Self {
            config,
            publish_context,
        }
    }
    /// Get the share protocol.
    pub fn protocol(&self) -> Option<VolumeShareProtocol> {
        self.config.target.protocol
    }
    /// Get the publish context.
    pub fn publish_context(&self) -> HashMap<String, String> {
        self.publish_context.clone()
    }
}

/// Volume Republish Operation parameters.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct RepublishOperation {
    config: TargetConfig,
}
impl RepublishOperation {
    /// Return new `Self` from the given parameters.
    pub fn new(config: TargetConfig) -> Self {
        Self { config }
    }
    /// Get the share protocol.
    pub fn protocol(&self) -> VolumeShareProtocol {
        self.config.target.protocol.unwrap_or_default()
    }
}

impl From<VolumeOperation> for models::volume_spec_operation::Operation {
    fn from(src: VolumeOperation) -> Self {
        match src {
            VolumeOperation::Create => models::volume_spec_operation::Operation::Create,
            VolumeOperation::Destroy => models::volume_spec_operation::Operation::Destroy,
            VolumeOperation::Share(_) => models::volume_spec_operation::Operation::Share,
            VolumeOperation::Unshare => models::volume_spec_operation::Operation::Unshare,
            VolumeOperation::SetReplica(_) => models::volume_spec_operation::Operation::SetReplica,
            VolumeOperation::PublishOld(_) => models::volume_spec_operation::Operation::Publish,
            VolumeOperation::Publish(_) => models::volume_spec_operation::Operation::Publish,
            VolumeOperation::Republish(_) => models::volume_spec_operation::Operation::Republish,
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
    type Kind = StorableObjectType;

    fn version(&self) -> u64 {
        0
    }
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

/// State of the Volume Spec.
pub type VolumeSpecStatus = SpecStatus<transport::VolumeStatus>;

impl From<&CreateVolume> for VolumeSpec {
    fn from(request: &CreateVolume) -> Self {
        Self {
            uuid: request.uuid.clone(),
            size: request.size,
            labels: request.labels.clone(),
            num_replicas: request.replicas as u8,
            status: VolumeSpecStatus::Creating,
            policy: request.policy.clone(),
            topology: request.topology.clone(),
            sequencer: OperationSequence::new(request.uuid.clone()),
            last_nexus_id: None,
            operation: None,
            thin: request.thin,
            target_config: None,
            publish_context: None,
            volume_group: request.volume_group.clone(),
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
            usage: None,
            status: transport::VolumeStatus::Unknown,
            target: None,
            replica_topology: HashMap::new(),
        }
    }
}
impl PartialEq<transport::VolumeState> for VolumeSpec {
    fn eq(&self, other: &transport::VolumeState) -> bool {
        match self.target() {
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
        let target = src.target().cloned();
        Self::new_all(
            src.labels,
            src.num_replicas,
            src.operation.into_opt(),
            src.size,
            src.status,
            target.into_opt(),
            src.uuid,
            src.topology.into_opt(),
            src.policy,
            src.thin,
        )
    }
}

/// VolumeGroupId type.
pub type VolumeGroupId = String;

/// In memory representation of volume group.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Default)]
pub struct VolumeGroupSpec {
    /// Name of the volume group.
    id: VolumeGroupId,
    /// List of ids of volumes that belong to the volume group.
    volumes: Vec<VolumeId>,
    /// The operation sequence resource is in.
    #[serde(skip)]
    sequencer: OperationSequence,
}

impl VolumeGroupSpec {
    /// Create a new VolumeGroupSpec.
    pub fn new(id: VolumeGroupId, volumes: Vec<VolumeId>) -> Self {
        Self {
            id: id.clone(),
            volumes,
            sequencer: OperationSequence::new(id),
        }
    }
    /// Name of the volume group.
    pub fn id(&self) -> &VolumeGroupId {
        &self.id
    }
    /// List of volumes in volume group.
    pub fn volumes(&self) -> &Vec<VolumeId> {
        &self.volumes
    }
    /// Add a new volume to the list of volumes.
    pub fn append(&mut self, id: VolumeId) {
        self.volumes.push(id)
    }
    /// Add a new volume to the list of volumes.
    pub fn remove(&mut self, id: &VolumeId) {
        self.volumes.retain(|vol| vol != id)
    }
    /// Check if the volume group has any more volumes.
    pub fn is_empty(&self) -> bool {
        self.volumes.is_empty()
    }
}
