use super::*;

use crate::{types::v0::store::volume::VolumeSpec, IntoOption};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

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
    state: Option<VolumeState>,
}

impl Volume {
    /// Construct a new volume.
    pub fn new(spec: &VolumeSpec, state: &Option<VolumeState>) -> Self {
        Self {
            spec: spec.clone(),
            state: state.clone(),
        }
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
    pub fn state(&self) -> Option<VolumeState> {
        self.state.clone()
    }
}

impl From<Volume> for models::Volume {
    fn from(volume: Volume) -> Self {
        models::Volume::new_all(volume.spec(), volume.state().into_opt())
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
    /// current share protocol
    pub protocol: Protocol,
    /// child nexus
    pub child: Option<Nexus>,
}

impl From<VolumeState> for models::VolumeState {
    fn from(volume: VolumeState) -> Self {
        Self {
            child: volume.child.into_opt(),
            protocol: volume.protocol.into(),
            size: volume.size,
            status: volume.status.into(),
            uuid: volume.uuid.into(),
        }
    }
}

impl VolumeState {
    /// Get the target node if the volume is published
    pub fn target_node(&self) -> Option<Option<NodeId>> {
        self.child.as_ref()?;
        Some(self.child.clone().map(|n| n.node))
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
            protocol: nexus.share,
            child: Some(nexus.clone()),
        }
    }
}

/// The protocol used to share the volume
/// Currently it's the same as the nexus
pub type VolumeShareProtocol = NexusShareProtocol;

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

impl From<models::VolumeShareProtocol> for VolumeShareProtocol {
    fn from(src: models::VolumeShareProtocol) -> Self {
        match src {
            models::VolumeShareProtocol::Nvmf => Self::Nvmf,
            models::VolumeShareProtocol::Iscsi => Self::Iscsi,
        }
    }
}

/// Volume topology using labels to determine how to place/distribute the data
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct LabelledTopology {
    /// node topology
    node_topology: NodeTopology,
    /// pool topology
    pool_topology: PoolTopology,
}

impl From<models::LabelledTopology> for LabelledTopology {
    fn from(src: models::LabelledTopology) -> Self {
        Self {
            node_topology: src.node_topology.into(),
            pool_topology: src.pool_topology.into(),
        }
    }
}

/// Volume topology used to determine how to place/distribute the data
/// Should either be labelled or explicit, not both.
/// If neither is used then the control plane will select from all available resources.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct Topology {
    /// volume topology using labels
    pub labelled: Option<LabelledTopology>,
    /// volume topology, explicitly selected
    pub explicit: Option<ExplicitTopology>,
}

impl From<models::Topology> for Topology {
    fn from(src: models::Topology) -> Self {
        Self {
            labelled: src.labelled.map(From::from),
            explicit: src.explicit.map(From::from),
        }
    }
}

/// Excludes resources with the same $label name, eg:
/// "Zone" would not allow for resources with the same "Zone" value
/// to be used for a certain operation, eg:
/// A node with "Zone: A" would not be paired up with a node with "Zone: A",
/// but it could be paired up with a node with "Zone: B"
/// exclusive label NAME in the form "NAME", and not "NAME: VALUE"
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct ExclusiveLabel(
    /// inner label
    pub String,
);

impl From<String> for ExclusiveLabel {
    fn from(src: String) -> Self {
        Self(src)
    }
}

/// Includes resources with the same $label or $label:$value eg:
/// if label is "Zone: A":
/// A resource with "Zone: A" would be paired up with a resource with "Zone: A",
/// but not with a resource with "Zone: B"
/// if label is "Zone":
/// A resource with "Zone: A" would be paired up with a resource with "Zone: B",
/// but not with a resource with "OtherLabel: B"
/// inclusive label key value in the form "NAME: VALUE"
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct InclusiveLabel(
    /// inner label
    pub String,
);

impl From<String> for InclusiveLabel {
    fn from(src: String) -> Self {
        Self(src)
    }
}

/// Placement node topology used by volume operations
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct NodeTopology {
    /// exclusive labels
    #[serde(default)]
    pub exclusion: Vec<ExclusiveLabel>,
    /// inclusive labels
    #[serde(default)]
    pub inclusion: Vec<InclusiveLabel>,
}

impl From<models::NodeTopology> for NodeTopology {
    fn from(src: models::NodeTopology) -> Self {
        Self {
            exclusion: src.exclusion.into_iter().map(From::from).collect(),
            inclusion: src.inclusion.into_iter().map(From::from).collect(),
        }
    }
}

/// Placement pool topology used by volume operations
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct PoolTopology {
    /// inclusive labels
    #[serde(default)]
    pub inclusion: Vec<InclusiveLabel>,
}

impl From<models::PoolTopology> for PoolTopology {
    fn from(src: models::PoolTopology) -> Self {
        Self {
            inclusion: src.inclusion.into_iter().map(From::from).collect(),
        }
    }
}

/// Explicit node placement Selection for a volume
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct ExplicitTopology {
    /// replicas can only be placed on these nodes
    #[serde(default)]
    pub allowed_nodes: Vec<NodeId>,
    /// preferred nodes to place the replicas
    #[serde(default)]
    pub preferred_nodes: Vec<NodeId>,
}

impl From<models::ExplicitTopology> for ExplicitTopology {
    fn from(src: models::ExplicitTopology) -> Self {
        Self {
            allowed_nodes: src.allowed_nodes.into_iter().map(From::from).collect(),
            preferred_nodes: src.preferred_nodes.into_iter().map(From::from).collect(),
        }
    }
}

/// Volume Healing policy used to determine if and how to replace a replica
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct VolumeHealPolicy {
    /// the server will attempt to heal the volume by itself
    /// the client should not attempt to do the same if this is enabled
    pub self_heal: bool,
    /// topology to choose a replacement replica for self healing
    /// (overrides the initial creation topology)
    pub topology: Option<Topology>,
}

impl Default for VolumeHealPolicy {
    fn default() -> Self {
        Self {
            self_heal: true,
            topology: None,
        }
    }
}

impl From<models::VolumeHealPolicy> for VolumeHealPolicy {
    fn from(src: models::VolumeHealPolicy) -> Self {
        Self {
            self_heal: src.self_heal,
            topology: src.topology.map(From::from),
        }
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
    /// volume healing policy
    pub policy: VolumeHealPolicy,
    /// initial replica placement topology
    pub topology: Topology,
}

impl CreateVolume {
    /// explicitly selected allowed_nodes
    pub fn allowed_nodes(&self) -> Vec<NodeId> {
        self.topology
            .explicit
            .clone()
            .unwrap_or_default()
            .allowed_nodes
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
}
impl UnpublishVolume {
    /// Create a new `UnpublishVolume` for the given uuid
    pub fn new(uuid: VolumeId) -> Self {
        Self { uuid }
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
