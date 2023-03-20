use super::*;

use crate::{types::v0::store::nexus::ReplicaUri, IntoOption};
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, fmt::Debug, ops::Deref};
use strum_macros::{Display, EnumString};

/// Get all the replicas from specific node and pool
/// or None for all nodes or all pools.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetReplicas {
    /// Filter request.
    pub filter: Filter,
}
impl GetReplicas {
    /// Return new `Self` to fetch a replica by its `ReplicaId`.
    pub fn new(uuid: &ReplicaId) -> Self {
        Self {
            filter: Filter::Replica(uuid.clone()),
        }
    }
}

/// Replica Space Usage information.
/// Useful for capacity management, eg: figure out how much of a thin-provisioned replica is
/// really allocated.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
pub struct ReplicaSpaceUsage {
    /// Replica capacity in bytes.
    pub capacity_bytes: u64,
    /// Amount of actually allocated disk space for this replica in bytes.
    pub allocated_bytes: u64,
    /// Cluster size in bytes.
    pub cluster_size: u64,
    /// Total number of clusters.
    pub clusters: u64,
    /// Number of actually used clusters.
    pub allocated_clusters: u64,
}

/// Replica information.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Replica {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Name of the replica.
    pub name: ReplicaName,
    /// UUID of the replica.
    pub uuid: ReplicaId,
    /// Id of the pool.
    pub pool_id: PoolId,
    /// UUID of the pool.
    pub pool_uuid: Option<PoolUuid>,
    /// Thin provisioning.
    pub thin: bool,
    /// Size of the replica in bytes.
    pub size: u64,
    /// Space information.
    pub space: Option<ReplicaSpaceUsage>,
    /// Protocol used for exposing the replica.
    pub share: Protocol,
    /// Uri usable by nexus to access it.
    pub uri: String,
    /// Status of the replica.
    pub status: ReplicaStatus,
    /// Host nqn's allowed to connect to the target.
    pub allowed_hosts: Vec<HostNqn>,
}
impl Replica {
    /// Check if the replica is online.
    pub fn online(&self) -> bool {
        self.status.online()
    }
}

/// Name of a Replica.
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ReplicaName(String);

impl ReplicaName {
    /// Derive Self from option or the replica_uuid, todo: needed until we fix CAS-1107.
    pub fn from_opt_uuid(opt: Option<&Self>, replica_uuid: &ReplicaId) -> Self {
        opt.unwrap_or(&Self::from_uuid(replica_uuid)).clone()
    }
    /// Derive Self from a replica_uuid, todo: needed until we fix CAS-1107.
    pub fn from_uuid(replica_uuid: &ReplicaId) -> Self {
        // CAS-1107 -> replica uuid are not checked to be unique, so until that is fixed
        // use the name as uuid (since the name is guaranteed to be unique)
        ReplicaName(replica_uuid.to_string())
    }
    /// Create new `Self` derived from the replica and volume id's.
    pub fn new(replica_uuid: &ReplicaId, _volume_uuid: Option<&VolumeId>) -> Self {
        // CAS-1107 -> replica uuid are not checked to be unique, so until that is fixed
        // use the name as uuid (since the name is guaranteed to be unique)
        ReplicaName(replica_uuid.to_string())
    }
    /// Create new `Self` derived from the replica name string.
    pub fn from_string(replica_name: String) -> Self {
        ReplicaName(replica_name)
    }
}
impl Default for ReplicaName {
    fn default() -> Self {
        ReplicaName::new(&ReplicaId::new(), None)
    }
}
impl Deref for ReplicaName {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<String> for ReplicaName {
    fn from(src: String) -> Self {
        Self(src)
    }
}
impl From<&str> for ReplicaName {
    fn from(src: &str) -> Self {
        Self(src.to_string())
    }
}
impl From<ReplicaName> for String {
    fn from(src: ReplicaName) -> Self {
        src.0
    }
}

impl From<Replica> for models::Replica {
    fn from(src: Replica) -> Self {
        Self::new_all(
            src.node,
            src.pool_id,
            src.pool_uuid.into_opt(),
            src.share,
            src.size,
            src.space.into_opt(),
            src.status,
            src.thin,
            src.uri,
            *src.uuid.uuid(),
            None::<Vec<String>>,
        )
    }
}
impl From<ReplicaSpaceUsage> for models::ReplicaSpaceUsage {
    fn from(src: ReplicaSpaceUsage) -> Self {
        Self::new_all(
            src.capacity_bytes,
            src.allocated_bytes,
            src.cluster_size,
            src.clusters,
            src.allocated_clusters,
        )
    }
}

rpc_impl_string_uuid!(ReplicaId, "UUID of a pool replica");

impl From<Replica> for DestroyReplica {
    fn from(replica: Replica) -> Self {
        Self {
            node: replica.node,
            pool_id: replica.pool_id,
            pool_uuid: replica.pool_uuid,
            uuid: replica.uuid,
            name: replica.name.into(),
            disowners: Default::default(),
        }
    }
}

/// Create Replica Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateReplica {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Name of the replica.
    pub name: Option<ReplicaName>,
    /// UUID of the replica.
    pub uuid: ReplicaId,
    /// Id of the pool.
    pub pool_id: PoolId,
    /// UUID of the pool.
    pub pool_uuid: Option<PoolUuid>,
    /// Size of the replica in bytes.
    pub size: u64,
    /// Thin provisioning.
    pub thin: bool,
    /// Protocol to expose the replica over.
    pub share: Protocol,
    /// Managed by our control plane.
    pub managed: bool,
    /// Owners of the resource.
    pub owners: ReplicaOwners,
    /// Host nqn's allowed to connect to the target.
    pub allowed_hosts: Vec<HostNqn>,
}

/// Replica owners which is a volume or none and a list of nexuses.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub struct ReplicaOwners {
    volume: Option<VolumeId>,
    #[serde(skip)]
    nexuses: Vec<NexusId>,
    #[serde(default)]
    disown_all: bool,
}
impl ReplicaOwners {
    /// Create new owners from the given volume and nexus id's.
    pub fn new(volume: Option<VolumeId>, nexuses: Vec<NexusId>) -> Self {
        Self {
            volume,
            nexuses,
            disown_all: false,
        }
    }
    /// Create a special `Self` that will disown all owners.
    pub fn new_disown_all() -> Self {
        Self {
            disown_all: true,
            ..Default::default()
        }
    }
    /// Disown all owners.
    pub fn with_disown_all(mut self) -> Self {
        self.disown_all = true;
        self
    }
    /// Return the volume owner, if any.
    pub fn volume(&self) -> Option<&VolumeId> {
        self.volume.as_ref()
    }
    /// Return the nexuses that own the replica.
    pub fn nexuses(&self) -> &Vec<NexusId> {
        self.nexuses.as_ref()
    }
    /// Check if this replica is owned by any nexuses or a volume.
    pub fn is_owned(&self) -> bool {
        self.volume.is_some() || !self.nexuses.is_empty()
    }
    /// Check if this replica is owned by the volume.
    pub fn owned_by(&self, id: &VolumeId) -> bool {
        self.volume.as_ref() == Some(id)
    }
    /// Check if this replica is owned by the nexus.
    pub fn owned_by_nexus(&self, id: &NexusId) -> bool {
        self.nexuses.iter().any(|n| n == id)
    }
    /// Check if this replica is owned by a nexus.
    pub fn owned_by_a_nexus(&self) -> bool {
        !self.nexuses.is_empty()
    }
    /// Create new owners from the volume Id.
    pub fn from_volume(volume: &VolumeId) -> Self {
        Self {
            volume: Some(volume.clone()),
            nexuses: vec![],
            disown_all: false,
        }
    }
    /// The replica is no longer part of the volume.
    pub fn disowned_by_volume(&mut self) {
        let _ = self.volume.take();
    }
    /// The replica is no longer part of the nexus.
    pub fn disowned_by_nexus(&mut self, nexus: &NexusId) {
        self.nexuses.retain(|n| n != nexus)
    }
    /// The replica is no longer part of the provided owners.
    pub fn disown(&mut self, disowner: &Self) {
        if disowner.disown_all {
            self.disown_all();
            return;
        }
        if self.volume == disowner.volume {
            self.volume = None;
        }
        self.nexuses.retain(|n| !disowner.owned_by_nexus(n));
    }
    /// The replica is no longer part of the provided owners.
    pub fn merge(&mut self, owners: Self) {
        if self.volume.is_none() {
            self.volume = owners.volume;
        }
        self.nexuses.extend(owners.nexuses);
    }
    pub fn disown_all(&mut self) {
        self.volume.take();
        self.nexuses.clear();
    }
    /// Add new volume owner.
    pub fn add_volume(&mut self, new: VolumeId) {
        self.volume = Some(new);
    }
    /// Add new nexus owner.
    pub fn add_owner(&mut self, new: &NexusId) {
        match self.nexuses.iter().find(|nexus| nexus == &new) {
            None => self.nexuses.push(new.clone()),
            Some(_) => {}
        }
    }
}

impl From<ReplicaOwners> for models::ReplicaSpecOwners {
    fn from(src: ReplicaOwners) -> Self {
        Self {
            nexuses: src
                .nexuses
                .iter()
                .map(|n| apis::Uuid::try_from(n).unwrap())
                .collect(),
            volume: src.volume.into_opt(),
        }
    }
}

/// Destroy Replica Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyReplica {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Id of the pool.
    pub pool_id: PoolId,
    /// UUID of the pool.
    pub pool_uuid: Option<PoolUuid>,
    /// UUID of the replica.
    pub uuid: ReplicaId,
    /// Name of the replica.
    pub name: Option<ReplicaName>,
    /// Delete by owners.
    pub disowners: ReplicaOwners,
}
impl DestroyReplica {
    /// Return a new `Self` from the provided arguments.
    pub fn new(
        node: &NodeId,
        pool_id: &PoolId,
        pool_uuid: &Option<PoolUuid>,
        name: &ReplicaName,
        uuid: &ReplicaId,
        disowners: &ReplicaOwners,
    ) -> Self {
        Self {
            node: node.clone(),
            pool_id: pool_id.clone(),
            pool_uuid: pool_uuid.clone(),
            uuid: uuid.clone(),
            name: name.clone().into(),
            disowners: disowners.clone(),
        }
    }
    /// Disown all owners.
    pub fn with_disown_all(mut self) -> Self {
        self.disowners.disown_all = true;
        self
    }
}

/// Share Replica Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ShareReplica {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Id of the pool.
    pub pool_id: PoolId,
    /// UUID of the pool.
    pub pool_uuid: Option<PoolUuid>,
    /// UUID of the replica.
    pub uuid: ReplicaId,
    /// Name of the replica.
    pub name: Option<ReplicaName>,
    /// Protocol used for exposing the replica.
    pub protocol: ReplicaShareProtocol,
    /// Nqn of hosts allowed to connect to the replica.
    pub allowed_hosts: Vec<HostNqn>,
}

impl ShareReplica {
    /// Get `Self` with the provided allowed_hosts.
    pub fn with_hosts(mut self, hosts: Vec<HostNqn>) -> Self {
        self.allowed_hosts = hosts;
        self
    }
}

impl From<ShareReplica> for UnshareReplica {
    fn from(share: ShareReplica) -> Self {
        Self {
            node: share.node,
            pool_id: share.pool_id,
            pool_uuid: share.pool_uuid,
            uuid: share.uuid,
            name: share.name,
        }
    }
}
impl From<&Replica> for ShareReplica {
    fn from(from: &Replica) -> Self {
        Self {
            node: from.node.clone(),
            pool_id: from.pool_id.clone(),
            pool_uuid: from.pool_uuid.clone(),
            uuid: from.uuid.clone(),
            name: from.name.clone().into(),
            protocol: ReplicaShareProtocol::Nvmf,
            allowed_hosts: vec![],
        }
    }
}
impl From<&Replica> for UnshareReplica {
    fn from(from: &Replica) -> Self {
        let from = from.clone();
        Self {
            node: from.node,
            pool_id: from.pool_id,
            pool_uuid: from.pool_uuid,
            uuid: from.uuid,
            name: from.name.into(),
        }
    }
}
impl From<UnshareReplica> for ShareReplica {
    fn from(share: UnshareReplica) -> Self {
        Self {
            node: share.node,
            pool_id: share.pool_id,
            pool_uuid: share.pool_uuid,
            uuid: share.uuid,
            name: share.name,
            protocol: ReplicaShareProtocol::Nvmf,
            allowed_hosts: vec![],
        }
    }
}

/// Unshare Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnshareReplica {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// Id of the pool.
    pub pool_id: PoolId,
    /// UUID of the pool.
    pub pool_uuid: Option<PoolUuid>,
    /// UUID of the replica.
    pub uuid: ReplicaId,
    /// Name of the replica.
    pub name: Option<ReplicaName>,
}

/// The protocol used to share the replica.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, EnumString, Display, Eq, PartialEq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReplicaShareProtocol {
    /// Shared as NVMe-oF TCP.
    Nvmf = 1,
}

impl std::cmp::PartialEq<Protocol> for ReplicaShareProtocol {
    fn eq(&self, other: &Protocol) -> bool {
        &Protocol::from(*self) == other
    }
}
impl Default for ReplicaShareProtocol {
    fn default() -> Self {
        Self::Nvmf
    }
}
impl From<i32> for ReplicaShareProtocol {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Nvmf,
            _ => panic!("Invalid replica share protocol {src}"),
        }
    }
}
impl From<ReplicaShareProtocol> for Protocol {
    fn from(src: ReplicaShareProtocol) -> Self {
        match src {
            ReplicaShareProtocol::Nvmf => Self::Nvmf,
        }
    }
}

/// State of the Replica.
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, Display, Eq, PartialEq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReplicaStatus {
    /// Unknown state.
    Unknown = 0,
    /// The replica is in normal working order.
    Online = 1,
    /// The replica has experienced a failure but can still function.
    Degraded = 2,
    /// The replica is completely inaccessible.
    Faulted = 3,
}
impl ReplicaStatus {
    /// Check if the state is online.
    pub fn online(&self) -> bool {
        self == &Self::Online
    }
}

impl Default for ReplicaStatus {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for ReplicaStatus {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}
impl From<ReplicaStatus> for models::ReplicaState {
    fn from(src: ReplicaStatus) -> Self {
        match src {
            ReplicaStatus::Unknown => Self::Unknown,
            ReplicaStatus::Online => Self::Online,
            ReplicaStatus::Degraded => Self::Degraded,
            ReplicaStatus::Faulted => Self::Faulted,
        }
    }
}

/// Add Replica to Nexus Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AddNexusReplica {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// UUID of the nexus.
    pub nexus: NexusId,
    /// UUID and URI of the replica to be added.
    pub replica: ReplicaUri,
    /// Auto start rebuilding.
    pub auto_rebuild: bool,
}
impl AddNexusReplica {
    /// Return new `Self` from it's properties.
    pub fn new(node: &NodeId, nexus: &NexusId, replica: &ReplicaUri, auto_rebuild: bool) -> Self {
        Self {
            node: node.clone(),
            nexus: nexus.clone(),
            replica: replica.clone(),
            auto_rebuild,
        }
    }
}

impl From<&AddNexusReplica> for AddNexusChild {
    fn from(add: &AddNexusReplica) -> Self {
        let add = add.clone();
        Self {
            node: add.node,
            nexus: add.nexus,
            uri: add.replica.uri().clone(),
            auto_rebuild: add.auto_rebuild,
        }
    }
}

/// Remove Replica from Nexus Request.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoveNexusReplica {
    /// Id of the io-engine instance.
    pub node: NodeId,
    /// UUID of the nexus.
    pub nexus: NexusId,
    /// UUID and URI of the replica to be added.
    pub replica: ReplicaUri,
}
impl RemoveNexusReplica {
    /// Return new `Self`.
    pub fn new(node: &NodeId, nexus: &NexusId, replica: &ReplicaUri) -> Self {
        Self {
            node: node.clone(),
            nexus: nexus.clone(),
            replica: replica.clone(),
        }
    }
}
impl From<&RemoveNexusReplica> for RemoveNexusChild {
    fn from(rm: &RemoveNexusReplica) -> Self {
        Self {
            node: rm.node.clone(),
            nexus: rm.nexus.clone(),
            uri: rm.replica.uri().clone(),
        }
    }
}
