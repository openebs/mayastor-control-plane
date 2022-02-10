use super::*;

use crate::{types::v0::store::volume::VolumeSpec, IntoOption};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, convert::TryFrom, fmt::Debug};

bus_impl_string_uuid!(VolumeId, "UUID of a mayastor volume");

/// Volumes
///
/// Volume information
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    /// Desired specification of the volume.
    spec: VolumeSpec,
    /// Runtime state of the volume.
    state: VolumeState,
}

impl Volume {
    /// Construct a new volume.
    pub fn new(spec: VolumeSpec, state: VolumeState) -> Self {
        Self { spec, state }
    }

    /// Get the volume spec.
    pub fn spec(&self) -> VolumeSpec {
        self.spec.clone()
    }

    /// Get the volume's uuid.
    pub fn uuid(&self) -> &VolumeId {
        &self.spec.uuid
    }

    /// Get the volume state.
    pub fn state(&self) -> VolumeState {
        self.state.clone()
    }

    /// Get the volume status, if any.
    pub fn status(&self) -> Option<VolumeStatus> {
        Some(self.state.status.clone())
    }
}

impl From<Volume> for models::Volume {
    fn from(volume: Volume) -> Self {
        models::Volume::new_all(volume.spec(), volume.state())
    }
}

/// Runtime volume state information.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct VolumeState {
    /// name of the volume
    pub uuid: VolumeId,
    /// size of the volume in bytes
    pub size: u64,
    /// current status of the volume
    pub status: VolumeStatus,
    /// target nexus that connects to the children
    pub target: Option<Nexus>,
    /// replica topology information
    pub replica_topology: HashMap<ReplicaId, ReplicaTopology>,
}

impl From<VolumeState> for models::VolumeState {
    fn from(volume: VolumeState) -> Self {
        Self {
            uuid: volume.uuid.into(),
            size: volume.size,
            status: volume.status.into(),
            target: volume.target.into_opt(),
            replica_topology: volume
                .replica_topology
                .iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }
}

impl VolumeState {
    /// Get the target node if the volume is published
    pub fn target_node(&self) -> Option<Option<NodeId>> {
        self.target.as_ref()?;
        Some(self.target.clone().map(|n| n.node))
    }
    /// Get the target protocol if the volume is published
    pub fn target_protocol(&self) -> Option<VolumeShareProtocol> {
        match &self.target {
            None => None,
            Some(target) => VolumeShareProtocol::try_from(target.share)
                .map(Some)
                .unwrap_or_default(),
        }
    }
}

impl From<(&VolumeId, &Nexus)> for VolumeState {
    fn from(src: (&VolumeId, &Nexus)) -> Self {
        let uuid = src.0.clone();
        let nexus = src.1;
        Self {
            uuid,
            size: nexus.size,
            status: nexus.status.clone(),
            target: Some(nexus.clone()),
            replica_topology: HashMap::new(),
        }
    }
}

/// The protocol used to share the volume
/// Currently it's the same as the nexus
pub type VolumeShareProtocol = NexusShareProtocol;
impl From<NexusShareProtocol> for models::VolumeShareProtocol {
    fn from(src: NexusShareProtocol) -> Self {
        match src {
            NexusShareProtocol::Nvmf => Self::Nvmf,
            NexusShareProtocol::Iscsi => Self::Iscsi,
        }
    }
}
impl From<models::VolumeShareProtocol> for NexusShareProtocol {
    fn from(src: models::VolumeShareProtocol) -> Self {
        match src {
            models::VolumeShareProtocol::Nvmf => Self::Nvmf,
            models::VolumeShareProtocol::Iscsi => Self::Iscsi,
        }
    }
}

/// Volume State information
/// Currently it's the same as the nexus
pub type VolumeStatus = NexusStatus;

impl From<VolumeStatus> for models::VolumeStatus {
    fn from(src: VolumeStatus) -> Self {
        match src {
            VolumeStatus::Unknown => models::VolumeStatus::Unknown,
            VolumeStatus::Online => models::VolumeStatus::Online,
            VolumeStatus::Degraded => models::VolumeStatus::Degraded,
            VolumeStatus::Faulted => models::VolumeStatus::Faulted,
        }
    }
}

/// Volume placement topology using resource labels
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct LabelledTopology {
    /// exclusive labels
    #[serde(default)]
    pub exclusion: ::std::collections::HashMap<String, String>,
    /// inclusive labels
    #[serde(default)]
    pub inclusion: ::std::collections::HashMap<String, String>,
}

impl From<models::LabelledTopology> for LabelledTopology {
    fn from(src: models::LabelledTopology) -> Self {
        Self {
            exclusion: src.exclusion,
            inclusion: src.inclusion,
        }
    }
}
impl From<LabelledTopology> for models::LabelledTopology {
    fn from(src: LabelledTopology) -> Self {
        Self::new(src.exclusion, src.inclusion)
    }
}

/// Volume topology used to determine how to place/distribute the data
/// If no topology is used then the control plane will select from all available resources.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct Topology {
    pub node: Option<NodeTopology>,
    pub pool: Option<PoolTopology>,
}
impl Topology {
    /// Get a reference to the explicit topology
    pub fn explicit(&self) -> Option<&ExplicitNodeTopology> {
        self.node.as_ref().and_then(|n| n.explicit())
    }
}
impl From<Topology> for models::Topology {
    fn from(src: Topology) -> Self {
        Self::new_all(src.node.into_opt(), src.pool.into_opt())
    }
}
impl From<models::Topology> for Topology {
    fn from(src: models::Topology) -> Self {
        Self {
            node: src.node_topology.into_opt(),
            pool: src.pool_topology.into_opt(),
        }
    }
}

/// Excludes resources with the same $label name, eg:
/// "Zone" would not allow for resources with the same "Zone" value
/// to be used for a certain operation, eg:
/// A node with "Zone: A" would not be paired up with a node with "Zone: A",
/// but it could be paired up with a node with "Zone: B"
/// exclusive label NAME in the form "NAME", and not "NAME: VALUE"
pub type ExclusiveLabel = String;

/// Includes resources with the same $label or $label:$value eg:
/// if label is "Zone: A":
/// A resource with "Zone: A" would be paired up with a resource with "Zone: A",
/// but not with a resource with "Zone: B"
/// if label is "Zone":
/// A resource with "Zone: A" would be paired up with a resource with "Zone: B",
/// but not with a resource with "OtherLabel: B"
/// inclusive label key value in the form "NAME: VALUE"
pub type InclusiveLabel = String;

/// Node topology for volumes
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum NodeTopology {
    /// using topology labels
    Labelled(LabelledTopology),
    /// explicitly selected
    Explicit(ExplicitNodeTopology),
}

impl NodeTopology {
    /// Get a reference to the explicit topology
    pub fn explicit(&self) -> Option<&ExplicitNodeTopology> {
        match self {
            Self::Labelled(_) => None,
            Self::Explicit(topology) => Some(topology),
        }
    }
}

impl From<NodeTopology> for models::NodeTopology {
    fn from(src: NodeTopology) -> Self {
        match src {
            NodeTopology::Explicit(topology) => Self::explicit(topology.into()),
            NodeTopology::Labelled(topology) => Self::labelled(topology.into()),
        }
    }
}
impl From<models::NodeTopology> for NodeTopology {
    fn from(src: models::NodeTopology) -> Self {
        match src {
            models::NodeTopology::explicit(topology) => Self::Explicit(topology.into()),
            models::NodeTopology::labelled(topology) => Self::Labelled(topology.into()),
        }
    }
}

/// Placement pool topology used by volume operations
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub enum PoolTopology {
    Labelled(LabelledTopology),
}
impl From<models::PoolTopology> for PoolTopology {
    fn from(src: models::PoolTopology) -> Self {
        match src {
            models::PoolTopology::labelled(topology) => Self::Labelled(topology.into()),
        }
    }
}
impl From<PoolTopology> for models::PoolTopology {
    fn from(src: PoolTopology) -> Self {
        match src {
            PoolTopology::Labelled(topology) => Self::labelled(topology.into()),
        }
    }
}

/// Explicit node placement Selection for a volume
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct ExplicitNodeTopology {
    /// replicas can only be placed on these nodes
    #[serde(default)]
    pub allowed_nodes: Vec<NodeId>,
    /// preferred nodes to place the replicas
    #[serde(default)]
    pub preferred_nodes: Vec<NodeId>,
}

impl From<models::ExplicitNodeTopology> for ExplicitNodeTopology {
    fn from(src: models::ExplicitNodeTopology) -> Self {
        Self {
            allowed_nodes: src.allowed_nodes.into_iter().map(From::from).collect(),
            preferred_nodes: src.preferred_nodes.into_iter().map(From::from).collect(),
        }
    }
}
impl From<ExplicitNodeTopology> for models::ExplicitNodeTopology {
    fn from(src: ExplicitNodeTopology) -> Self {
        Self::new(src.allowed_nodes, src.preferred_nodes)
    }
}

/// Volume policy used to determine if and how to replace a replica
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct VolumePolicy {
    /// the server will attempt to heal the volume by itself
    /// the client should not attempt to do the same if this is enabled
    pub self_heal: bool,
}

impl Default for VolumePolicy {
    fn default() -> Self {
        Self { self_heal: true }
    }
}

impl From<models::VolumePolicy> for VolumePolicy {
    fn from(src: models::VolumePolicy) -> Self {
        Self {
            self_heal: src.self_heal,
        }
    }
}
impl From<VolumePolicy> for models::VolumePolicy {
    fn from(src: VolumePolicy) -> Self {
        Self::new_all(src.self_heal)
    }
}

/// Get volumes
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetVolumes {
    /// filter volumes
    pub filter: Filter,
}
impl GetVolumes {
    /// Return new `Self` to retrieve the specified volume
    pub fn new(volume: &VolumeId) -> Self {
        Self {
            filter: Filter::Volume(volume.clone()),
        }
    }
}

/// Create volume
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
    /// size of the volume in bytes
    pub size: u64,
    /// number of storage replicas
    pub replicas: u64,
    /// volume policy
    pub policy: VolumePolicy,
    /// initial replica placement topology
    pub topology: Option<Topology>,
    /// volume labels
    pub labels: Option<VolumeLabels>,
}

/// Volume label information
pub type VolumeLabels = HashMap<String, String>;

impl CreateVolume {
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
}

/// Add ANA Nexus to volume
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct AddVolumeNexus {
    /// uuid of the volume
    pub uuid: VolumeId,
    /// preferred node id for the nexus
    pub preferred_node: Option<NodeId>,
}

/// Add ANA Nexus to volume
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RemoveVolumeNexus {
    /// uuid of the volume
    pub uuid: VolumeId,
    /// id of the node where the nexus lives
    pub node: Option<NodeId>,
}

/// Publish a volume on a node
/// Unpublishes the nexus if it's published somewhere else and creates a nexus on the given node.
/// Then, share the nexus via the provided share protocol.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct PublishVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
    /// the node where front-end IO will be sent to
    pub target_node: Option<NodeId>,
    /// share protocol
    pub share: Option<VolumeShareProtocol>,
}
impl PublishVolume {
    /// Create new `PublishVolume` based on the provided arguments
    pub fn new(
        uuid: VolumeId,
        target_node: Option<NodeId>,
        share: Option<VolumeShareProtocol>,
    ) -> Self {
        Self {
            uuid,
            target_node,
            share,
        }
    }
}

/// Unpublish a volume from any node where it may be published
/// Unshares the children nexuses from the volume and destroys them.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnpublishVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
    /// if the node where the nexus lives is offline then we can force unpublish, forgetting about
    /// the nexus. Note: this option should be used only when we know the node will not become
    /// accessible again and it is safe to do so.
    force: bool,
}
impl UnpublishVolume {
    /// Create a new `UnpublishVolume` for the given uuid
    pub fn new(uuid: &VolumeId, force: bool) -> Self {
        Self {
            uuid: uuid.clone(),
            force,
        }
    }
    /// It's a force `Self`
    pub fn force(&self) -> bool {
        self.force
    }
}

/// Share Volume request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ShareVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
    /// share protocol
    pub protocol: VolumeShareProtocol,
}
impl ShareVolume {
    /// Create a new `ShareVolume` request
    pub(crate) fn new(uuid: VolumeId, protocol: VolumeShareProtocol) -> Self {
        Self { uuid, protocol }
    }
}

/// Unshare Volume request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnshareVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
}
impl UnshareVolume {
    /// Create a new `UnshareVolume` request
    pub(crate) fn new(uuid: VolumeId) -> Self {
        Self { uuid }
    }
}
/// Set the volume replica count
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SetVolumeReplica {
    /// uuid of the volume
    pub uuid: VolumeId,
    /// replica count
    pub replicas: u8,
}
impl SetVolumeReplica {
    /// Create new `Self` based on the provided arguments
    pub fn new(uuid: VolumeId, replicas: u8) -> Self {
        Self { uuid, replicas }
    }
}

/// Delete volume
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
}
impl DestroyVolume {
    /// Create new `Self` to destroy the specified volume
    pub fn new(volume: &VolumeId) -> Self {
        Self {
            uuid: volume.clone(),
        }
    }
    /// Get the volume's identification
    pub fn uuid(&self) -> &VolumeId {
        &self.uuid
    }
}

/// Replica topology information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaTopology {
    /// id of the mayastor instance
    node: Option<NodeId>,
    /// id of the pool
    pool: Option<PoolId>,
    /// status of the replica
    status: ReplicaStatus,
}

impl ReplicaTopology {
    /// Create a new instance of ReplicaTopology.
    pub fn new(node: Option<NodeId>, pool: Option<PoolId>, status: ReplicaStatus) -> Self {
        Self { node, pool, status }
    }

    /// Get the ReplicaTopology node ID.
    pub fn node(&self) -> &Option<NodeId> {
        &self.node
    }

    /// Get the ReplicaTopology pool ID.
    pub fn pool(&self) -> &Option<PoolId> {
        &self.pool
    }

    /// Get the status of the replica
    pub fn status(&self) -> &ReplicaStatus {
        &self.status
    }
}

impl From<&ReplicaTopology> for models::ReplicaTopology {
    fn from(replica_topology: &ReplicaTopology) -> Self {
        models::ReplicaTopology::new_all(
            replica_topology.node.clone().map(Into::into),
            replica_topology.pool.clone().map(Into::into),
            replica_topology.status.clone(),
        )
    }
}
