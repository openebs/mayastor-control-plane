//! This module contains definitions of data structures that can be saved to the
//! persistent store.

use crate::store::{ObjectKey, StorableObjectType};
use mbus_api::v0;
use serde::{Deserialize, Serialize};

type NodeLabel = String;
type VolumeLabel = String;
type PoolLabel = String;

/// Node data structure used by the persistent store.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    // Node information
    node: v0::Node,
    /// Node labels.
    labels: Vec<NodeLabel>,
}

/// Pool data structure used by the persistent store.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Pool {
    /// Current state of the pool.
    pub state: Option<PoolState>,
    /// Desired pool specification.
    pub spec: PoolSpec,
}

/// Runtime state of the pool.
/// This should eventually satisfy the PoolSpec.
#[derive(Serialize, Deserialize, Debug, PartialEq, Default)]
pub struct PoolState {
    /// Pool information returned by Mayastor.
    pub pool: v0::Pool,
    /// Pool labels.
    pub labels: Vec<PoolLabel>,
}

/// User specification of a pool.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct PoolSpec {
    /// id of the mayastor instance
    pub node: v0::NodeId,
    /// id of the pool
    pub id: v0::PoolId,
    /// absolute disk paths claimed by the pool
    pub disks: Vec<String>,
    /// state of the pool
    pub state: v0::PoolState,
    /// Pool labels.
    pub labels: Vec<PoolLabel>,
}

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
    /// Volume size.
    pub size: u64,
    /// Volume labels.
    pub labels: Vec<VolumeLabel>,
    /// Number of replicas.
    pub num_replicas: u8,
    /// Protocol that the volume is shared over.
    pub protocol: v0::Protocol,
    /// Nexuses that make up the volume.
    pub nexuses: Vec<v0::NexusId>,
    /// Number of front-end paths.
    pub num_paths: u8,
    /// State of the volume.
    pub state: v0::VolumeState,
}

/// User specification of a volume.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct VolumeSpec {
    /// Size that the volume should be.
    pub size: u64,
    /// Volume labels.
    pub labels: Vec<VolumeLabel>,
    /// Number of children the volume should have.
    pub num_replicas: u8,
    /// Protocol that the volume should be shared over.
    pub protocol: v0::Protocol,
    /// Number of front-end paths.
    pub num_paths: u8,
    /// State that the volume should eventually achieve.
    pub state: v0::VolumeState,
}

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

/// User specification of a nexus.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct NexusSpec {
    /// Node where the nexus should live.
    pub node: v0::NodeId,
    /// List of children.
    pub children: Vec<Child>,
    /// Size of the nexus.
    pub size: u64,
    /// The state the nexus should eventually reach.
    pub state: v0::NexusState,
}

/// Child information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Child {
    /// Current state of the child.
    pub state: Option<ChildState>,
    /// Desired child specification.
    pub spec: ChildSpec,
}

/// Runtime state of a child.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ChildState {
    pub child: v0::Child,
    /// Size of the child.
    pub size: u64,
    /// UUID of the replica that the child connects to.
    pub replica_uuid: String,
}

/// User specification of a child.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ChildSpec {
    /// The size the child should be.
    pub size: u64,
    /// The UUID of the replica the child should be associated with.
    pub replica_uuid: String,
    /// The state the child should eventually reach.
    pub state: v0::ChildState,
}

/// Replica information
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Replica {
    /// Current state of the replica.
    pub state: Option<ReplicaState>,
    /// Desired replica specification.
    pub spec: ReplicaSpec,
}

/// Runtime state of a replica.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ReplicaState {
    /// Replica information.
    pub replica: v0::Replica,
    /// State of the replica.
    pub state: v0::ReplicaState,
}

/// User specification of a replica.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct ReplicaSpec {
    /// The size that the replica should be.
    pub size: u64,
    /// The pool that the replica should live on.
    pub pool: v0::PoolId,
    /// Protocol used for exposing the replica.
    pub share: v0::Protocol,
    /// Thin provisioning.
    pub thin: bool,
    /// The state that the replica should eventually achieve.
    pub state: v0::ReplicaState,
}

impl ObjectKey for v0::VolumeId {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::Volume
    }
    fn key_uuid(&self) -> String {
        self.to_string()
    }
}

impl ObjectKey for v0::NexusId {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::Nexus
    }
    fn key_uuid(&self) -> String {
        self.to_string()
    }
}

impl ObjectKey for v0::WatchResourceId {
    fn key_type(&self) -> StorableObjectType {
        match &self {
            v0::WatchResourceId::Node(_) => StorableObjectType::Node,
            v0::WatchResourceId::Pool(_) => StorableObjectType::Pool,
            v0::WatchResourceId::Replica(_) => StorableObjectType::Replica,
            v0::WatchResourceId::Nexus(_) => StorableObjectType::Nexus,
            v0::WatchResourceId::Volume(_) => StorableObjectType::Volume,
        }
    }
    fn key_uuid(&self) -> String {
        match &self {
            v0::WatchResourceId::Node(i) => i.to_string(),
            v0::WatchResourceId::Pool(i) => i.to_string(),
            v0::WatchResourceId::Replica(i) => i.to_string(),
            v0::WatchResourceId::Nexus(i) => i.to_string(),
            v0::WatchResourceId::Volume(i) => i.to_string(),
        }
    }
}
