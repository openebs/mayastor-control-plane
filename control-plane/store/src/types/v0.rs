//! This module contains definitions of data structures that can be saved to the
//! persistent store.

use crate::store::{ObjectKey, StorableObject, StorableObjectType};
use mbus_api::v0;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

type NodeLabels = HashMap<String, String>;
type VolumeLabel = String;
type PoolLabel = String;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum SpecState<T> {
    Creating,
    Created(T),
    Deleting,
    Deleted,
}

impl<T: std::cmp::PartialEq> SpecState<T> {
    pub fn creating(&self) -> bool {
        self == &Self::Creating
    }
    pub fn created(&self) -> bool {
        matches!(self, &Self::Created(_))
    }
    pub fn deleting(&self) -> bool {
        self == &Self::Deleting
    }
    pub fn deleted(&self) -> bool {
        self == &Self::Deleted
    }
}

/// Node data structure used by the persistent store.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Node {
    /// Node information.
    node: v0::Node,
    /// Node labels.
    labels: NodeLabels,
}

/// Node data structure used by the persistent store.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct NodeSpec {
    /// Node identification.
    id: v0::NodeId,
    /// Node labels.
    labels: NodeLabels,
}

/// Pool data structure used by the persistent store.
#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct Pool {
    /// Current state of the pool.
    pub state: Option<PoolState>,
    /// Desired pool specification.
    pub spec: Option<PoolSpec>,
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

/// State of the Pool Spec
pub type PoolSpecState = SpecState<v0::PoolState>;
impl From<&v0::CreatePool> for PoolSpec {
    fn from(request: &v0::CreatePool) -> Self {
        Self {
            node: request.node.clone(),
            id: request.id.clone(),
            disks: request.disks.clone(),
            state: PoolSpecState::Creating,
            labels: vec![],
            updating: true,
        }
    }
}
impl PartialEq<v0::CreatePool> for PoolSpec {
    fn eq(&self, other: &v0::CreatePool) -> bool {
        let mut other = PoolSpec::from(other);
        other.state = self.state.clone();
        &other == self
    }
}

/// User specification of a pool.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct PoolSpec {
    /// id of the mayastor instance
    pub node: v0::NodeId,
    /// id of the pool
    pub id: v0::PoolId,
    /// absolute disk paths claimed by the pool
    pub disks: Vec<String>,
    /// state of the pool
    pub state: PoolSpecState,
    /// Pool labels.
    pub labels: Vec<PoolLabel>,
    /// Update in progress
    #[serde(skip)]
    pub updating: bool,
}
impl From<&PoolSpec> for v0::Pool {
    fn from(pool: &PoolSpec) -> Self {
        Self {
            node: pool.node.clone(),
            id: pool.id.clone(),
            disks: pool.disks.clone(),
            state: v0::PoolState::Unknown,
            capacity: 0,
            used: 0,
        }
    }
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
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct VolumeSpec {
    /// Volume Id
    pub uuid: v0::VolumeId,
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
    pub state: VolumeSpecState,
    /// Update of the state in progress
    #[serde(skip)]
    pub updating: bool,
}

/// State of the Volume Spec
pub type VolumeSpecState = SpecState<v0::VolumeState>;

impl From<&v0::CreateVolume> for VolumeSpec {
    fn from(request: &v0::CreateVolume) -> Self {
        Self {
            uuid: request.uuid.clone(),
            size: request.size,
            labels: vec![],
            num_replicas: request.replicas as u8,
            protocol: v0::Protocol::Off,
            num_paths: request.nexuses as u8,
            state: VolumeSpecState::Creating,
            updating: true,
        }
    }
}
impl PartialEq<v0::CreateVolume> for VolumeSpec {
    fn eq(&self, other: &v0::CreateVolume) -> bool {
        let mut other = VolumeSpec::from(other);
        other.state = self.state.clone();
        other.updating = self.updating;
        &other == self
    }
}
impl From<&VolumeSpec> for v0::Volume {
    fn from(spec: &VolumeSpec) -> Self {
        Self {
            uuid: spec.uuid.clone(),
            size: spec.size,
            state: v0::VolumeState::Unknown,
            children: vec![],
        }
    }
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

/// State of the Pool Spec
pub type NexusSpecState = SpecState<v0::NexusState>;

impl From<&v0::CreateNexus> for NexusSpec {
    fn from(request: &v0::CreateNexus) -> Self {
        Self {
            uuid: request.uuid.clone(),
            node: request.node.clone(),
            children: request.children.clone(),
            size: request.size,
            state: NexusSpecState::Creating,
            share: v0::Protocol::Off,
            managed: request.managed,
            owner: request.owner.clone(),
            updating: true,
        }
    }
}
impl PartialEq<v0::CreateNexus> for NexusSpec {
    fn eq(&self, other: &v0::CreateNexus) -> bool {
        let mut other = NexusSpec::from(other);
        other.state = self.state.clone();
        other.updating = self.updating;
        &other == self
    }
}

/// User specification of a nexus.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct NexusSpec {
    /// Nexus Id
    pub uuid: v0::NexusId,
    /// Node where the nexus should live.
    pub node: v0::NodeId,
    /// List of children.
    pub children: Vec<v0::ChildUri>,
    /// Size of the nexus.
    pub size: u64,
    /// The state the nexus should eventually reach.
    pub state: NexusSpecState,
    /// Share Protocol
    pub share: v0::Protocol,
    /// Managed by our control plane
    pub managed: bool,
    /// Volume which owns this nexus, if any
    pub owner: Option<v0::VolumeId>,
    /// Update of the state in progress
    #[serde(skip)]
    pub updating: bool,
}
impl From<&NexusSpec> for v0::Nexus {
    fn from(nexus: &NexusSpec) -> Self {
        Self {
            node: nexus.node.clone(),
            uuid: nexus.uuid.clone(),
            size: nexus.size,
            state: v0::NexusState::Unknown,
            children: nexus
                .children
                .iter()
                .map(|uri| v0::Child {
                    uri: uri.clone(),
                    state: v0::ChildState::Unknown,
                    rebuild_progress: None,
                })
                .collect(),
            device_uri: "".to_string(),
            rebuilds: 0,
        }
    }
}
impl From<NexusSpec> for v0::DestroyNexus {
    fn from(nexus: NexusSpec) -> Self {
        Self {
            node: nexus.node,
            uuid: nexus.uuid,
        }
    }
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

/// State of the Replica Spec
pub type ReplicaSpecState = SpecState<v0::ReplicaState>;

impl From<&v0::CreateReplica> for ReplicaSpec {
    fn from(request: &v0::CreateReplica) -> Self {
        Self {
            uuid: request.uuid.clone(),
            size: request.size,
            pool: request.pool.clone(),
            share: request.share.clone(),
            thin: request.thin,
            state: ReplicaSpecState::Creating,
            managed: request.managed,
            owners: request.owners.clone(),
            updating: true,
        }
    }
}
impl PartialEq<v0::CreateReplica> for ReplicaSpec {
    fn eq(&self, other: &v0::CreateReplica) -> bool {
        let mut other = ReplicaSpec::from(other);
        other.state = self.state.clone();
        other.updating = self.updating;
        &other == self
    }
}

/// User specification of a replica.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ReplicaSpec {
    /// uuid of the replica
    pub uuid: v0::ReplicaId,
    /// The size that the replica should be.
    pub size: u64,
    /// The pool that the replica should live on.
    pub pool: v0::PoolId,
    /// Protocol used for exposing the replica.
    pub share: v0::Protocol,
    /// Thin provisioning.
    pub thin: bool,
    /// The state that the replica should eventually achieve.
    pub state: ReplicaSpecState,
    /// Managed by our control plane
    pub managed: bool,
    /// Owner Resource
    pub owners: v0::ReplicaOwners,
    /// Update in progress
    #[serde(skip)]
    pub updating: bool,
}

impl From<&ReplicaSpec> for v0::Replica {
    fn from(replica: &ReplicaSpec) -> Self {
        Self {
            node: v0::NodeId::default(),
            uuid: replica.uuid.clone(),
            pool: replica.pool.clone(),
            thin: replica.thin,
            size: replica.size,
            share: replica.share.clone(),
            uri: "".to_string(),
            state: v0::ReplicaState::Unknown,
        }
    }
}

impl StorableObject for ReplicaSpec {
    type Key = v0::ReplicaId;
    fn key(&self) -> Self::Key {
        self.uuid.clone()
    }
}

impl StorableObject for NexusSpec {
    type Key = v0::NexusId;
    fn key(&self) -> Self::Key {
        self.uuid.clone()
    }
}

impl ObjectKey for v0::NexusId {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::NexusSpec
    }
    fn key_uuid(&self) -> String {
        self.to_string()
    }
}

impl StorableObject for VolumeSpec {
    type Key = v0::VolumeId;
    fn key(&self) -> Self::Key {
        self.uuid.clone()
    }
}

impl ObjectKey for v0::VolumeId {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::VolumeSpec
    }
    fn key_uuid(&self) -> String {
        self.to_string()
    }
}

impl ObjectKey for v0::ReplicaId {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::ReplicaSpec
    }
    fn key_uuid(&self) -> String {
        self.to_string()
    }
}

impl StorableObject for PoolSpec {
    type Key = v0::PoolId;

    fn key(&self) -> Self::Key {
        self.id.clone()
    }
}

impl ObjectKey for v0::PoolId {
    fn key_type(&self) -> StorableObjectType {
        StorableObjectType::PoolSpec
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
