use super::*;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

bus_impl_string_uuid!(VolumeId, "UUID of a mayastor volume");

/// Volumes
///
/// Volume information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Volume {
    /// name of the volume
    pub uuid: VolumeId,
    /// size of the volume in bytes
    pub size: u64,
    /// current state of the volume
    pub state: VolumeState,
    /// current share protocol
    pub protocol: Protocol,
    /// array of children nexuses
    pub children: Vec<Nexus>,
}

impl Volume {
    /// Get the target node if the volume is published
    pub fn target_node(&self) -> Option<Option<NodeId>> {
        if self.children.len() > 1 {
            return None;
        }
        Some(self.children.get(0).map(|n| n.node.clone()))
    }
}

/// ANA not supported at the moment, so derive volume state from the
/// single Nexus instance
impl From<(&VolumeId, &Nexus)> for Volume {
    fn from(src: (&VolumeId, &Nexus)) -> Self {
        let uuid = src.0.clone();
        let nexus = src.1;
        Self {
            uuid,
            size: nexus.size,
            state: nexus.state.clone(),
            protocol: nexus.share.clone(),
            children: vec![nexus.clone()],
        }
    }
}

/// The protocol used to share the volume
/// Currently it's the same as the nexus
pub type VolumeShareProtocol = NexusShareProtocol;

/// Volume State information
/// Currently it's the same as the nexus
pub type VolumeState = NexusState;

/// Volume topology using labels to determine how to place/distribute the data
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct LabelTopology {
    /// node topology
    node_topology: NodeTopology,
    /// pool topology
    pool_topology: PoolTopology,
}

/// Volume topology used to determine how to place/distribute the data
/// Should either be labelled or explicit, not both.
/// If neither is used then the control plane will select from all available resources.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct Topology {
    /// volume topology using labels
    pub labelled: Option<LabelTopology>,
    /// volume topology, explicitly selected
    pub explicit: Option<ExplicitTopology>,
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

/// Placement pool topology used by volume operations
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct PoolTopology {
    /// inclusive labels
    #[serde(default)]
    pub inclusion: Vec<InclusiveLabel>,
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

/// Volume Healing policy used to determine if and how to replace a replica
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct VolumeHealPolicy {
    /// the server will attempt to heal the volume by itself
    /// the client should not attempt to do the same if this is enabled
    pub self_heal: bool,
    /// topology to choose a replacement replica for self healing
    /// (overrides the initial creation topology)
    pub topology: Option<Topology>,
}

/// Get volumes
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetVolumes {
    /// filter volumes
    pub filter: Filter,
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

/// Unpublish a volume from any node where it may be published
/// Unshares the children nexuses from the volume and destroys them.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnpublishVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
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

/// Unshare Volume request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnshareVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
}

/// Delete volume
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyVolume {
    /// uuid of the volume
    pub uuid: VolumeId,
}
