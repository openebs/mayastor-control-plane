use super::*;

use crate::{types::v0::store::nexus::ReplicaUri, IntoOption};
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, fmt::Debug, ops::Deref};
use strum_macros::{EnumString, ToString};

/// Get all the replicas from specific node and pool
/// or None for all nodes or all pools
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetReplicas {
    /// Filter request
    pub filter: Filter,
}
impl GetReplicas {
    /// Return new `Self` to fetch a replica by its `ReplicaId`
    pub fn new(uuid: &ReplicaId) -> Self {
        Self {
            filter: Filter::Replica(uuid.clone()),
        }
    }
}

/// Replica information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Replica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// name of the replica
    pub name: ReplicaName,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// id of the pool
    pub pool: PoolId,
    /// thin provisioning
    pub thin: bool,
    /// size of the replica in bytes
    pub size: u64,
    /// protocol used for exposing the replica
    pub share: Protocol,
    /// uri usable by nexus to access it
    pub uri: String,
    /// status of the replica
    pub status: ReplicaStatus,
}
impl Replica {
    /// check if the replica is online
    pub fn online(&self) -> bool {
        self.status.online()
    }
}

/// Name of a Replica
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
pub struct ReplicaName(String);

impl ReplicaName {
    /// Derive Self from option or the replica_uuid, todo: needed until we fix CAS-1107
    pub fn from_opt_uuid(opt: Option<&Self>, replica_uuid: &ReplicaId) -> Self {
        opt.unwrap_or(&Self::from_uuid(replica_uuid)).clone()
    }
    /// Derive Self from a replica_uuid, todo: needed until we fix CAS-1107
    pub fn from_uuid(replica_uuid: &ReplicaId) -> Self {
        // CAS-1107 -> replica uuid are not checked to be unique, so until that is fixed
        // use the name as uuid (since the name is guaranteed to be unique)
        ReplicaName(replica_uuid.to_string())
    }
    /// Create new `Self` derived from the replica and volume id's
    pub fn new(replica_uuid: &ReplicaId, _volume_uuid: Option<&VolumeId>) -> Self {
        // CAS-1107 -> replica uuid are not checked to be unique, so until that is fixed
        // use the name as uuid (since the name is guaranteed to be unique)
        ReplicaName(replica_uuid.to_string())
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
        Self::new(
            src.node,
            src.pool,
            src.share,
            src.size,
            src.status,
            src.thin,
            src.uri,
            apis::Uuid::try_from(src.uuid).unwrap(),
        )
    }
}

bus_impl_string_uuid!(ReplicaId, "UUID of a mayastor pool replica");

impl From<Replica> for DestroyReplica {
    fn from(replica: Replica) -> Self {
        Self {
            node: replica.node,
            pool: replica.pool,
            uuid: replica.uuid,
            name: replica.name.into(),
            disowners: Default::default(),
        }
    }
}

/// Create Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// name of the replica
    pub name: Option<ReplicaName>,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// id of the pool
    pub pool: PoolId,
    /// size of the replica in bytes
    pub size: u64,
    /// thin provisioning
    pub thin: bool,
    /// protocol to expose the replica over
    pub share: Protocol,
    /// Managed by our control plane
    pub managed: bool,
    /// Owners of the resource
    pub owners: ReplicaOwners,
}

/// Replica owners which is a volume or none and a list of nexuses
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub struct ReplicaOwners {
    volume: Option<VolumeId>,
    nexuses: Vec<NexusId>,
}
impl ReplicaOwners {
    /// Create new owners from the given volume and nexus id's
    pub fn new(volume: Option<VolumeId>, nexuses: Vec<NexusId>) -> Self {
        Self { volume, nexuses }
    }
    /// Return the volume owner, if any
    pub fn volume(&self) -> Option<&VolumeId> {
        self.volume.as_ref()
    }
    /// Return the nexuses that own the replica
    pub fn nexuses(&self) -> &Vec<NexusId> {
        self.nexuses.as_ref()
    }
    /// Check if this replica is owned by any nexuses or a volume
    pub fn is_owned(&self) -> bool {
        self.volume.is_some() || !self.nexuses.is_empty()
    }
    /// Check if this replica is owned by the volume
    pub fn owned_by(&self, id: &VolumeId) -> bool {
        self.volume.as_ref() == Some(id)
    }
    /// Check if this replica is owned by the nexus
    pub fn owned_by_nexus(&self, id: &NexusId) -> bool {
        self.nexuses.iter().any(|n| n == id)
    }
    /// Check if this replica is owned by a nexus
    pub fn owned_by_a_nexus(&self) -> bool {
        !self.nexuses.is_empty()
    }
    /// Create new owners from the volume Id
    pub fn from_volume(volume: &VolumeId) -> Self {
        Self {
            volume: Some(volume.clone()),
            nexuses: vec![],
        }
    }
    /// The replica is no longer part of the volume
    pub fn disowned_by_volume(&mut self) {
        let _ = self.volume.take();
    }
    /// The replica is no longer part of the nexus
    pub fn disowned_by_nexus(&mut self, nexus: &NexusId) {
        self.nexuses.retain(|n| n != nexus)
    }
    /// The replica is no longer part of the provided owners
    pub fn disown(&mut self, disowner: &Self) {
        if self.volume == disowner.volume {
            self.volume = None;
        }
        self.nexuses = self
            .nexuses
            .iter()
            .filter(|n| !disowner.owned_by_nexus(n))
            .cloned()
            .collect();
    }
    pub fn disown_all(&mut self) {
        self.volume.take();
        self.nexuses.clear();
    }
    /// Add new nexus owner
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

/// Destroy Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub pool: PoolId,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// name of the replica
    pub name: Option<ReplicaName>,
    /// delete by owners
    pub disowners: ReplicaOwners,
}
impl DestroyReplica {
    /// Return a new `Self` from the provided arguments
    pub fn new(
        node: &NodeId,
        pool: &PoolId,
        name: &ReplicaName,
        uuid: &ReplicaId,
        disowners: &ReplicaOwners,
    ) -> Self {
        Self {
            node: node.clone(),
            pool: pool.clone(),
            uuid: uuid.clone(),
            name: name.clone().into(),
            disowners: disowners.clone(),
        }
    }
}

/// Share Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ShareReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub pool: PoolId,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// name of the replica,
    pub name: Option<ReplicaName>,
    /// protocol used for exposing the replica
    pub protocol: ReplicaShareProtocol,
}

impl From<ShareReplica> for UnshareReplica {
    fn from(share: ShareReplica) -> Self {
        Self {
            node: share.node,
            pool: share.pool,
            uuid: share.uuid,
            name: share.name,
        }
    }
}
impl From<&Replica> for ShareReplica {
    fn from(from: &Replica) -> Self {
        Self {
            node: from.node.clone(),
            pool: from.pool.clone(),
            uuid: from.uuid.clone(),
            name: from.name.clone().into(),
            protocol: ReplicaShareProtocol::Nvmf,
        }
    }
}
impl From<&Replica> for UnshareReplica {
    fn from(from: &Replica) -> Self {
        let from = from.clone();
        Self {
            node: from.node,
            pool: from.pool,
            uuid: from.uuid,
            name: from.name.into(),
        }
    }
}
impl From<UnshareReplica> for ShareReplica {
    fn from(share: UnshareReplica) -> Self {
        Self {
            node: share.node,
            pool: share.pool,
            uuid: share.uuid,
            name: share.name,
            protocol: ReplicaShareProtocol::Nvmf,
        }
    }
}

/// Unshare Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnshareReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub pool: PoolId,
    /// uuid of the replica
    pub uuid: ReplicaId,
    /// name of the replica
    pub name: Option<ReplicaName>,
}

/// The protocol used to share the replica.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, EnumString, ToString, Eq, PartialEq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReplicaShareProtocol {
    /// shared as NVMe-oF TCP
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
            _ => panic!("Invalid replica share protocol {}", src),
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

/// State of the Replica
#[derive(Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReplicaStatus {
    /// unknown state
    Unknown = 0,
    /// the replica is in normal working order
    Online = 1,
    /// the replica has experienced a failure but can still function
    Degraded = 2,
    /// the replica is completely inaccessible
    Faulted = 3,
}
impl ReplicaStatus {
    /// check if the state is online
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

/// Add Replica to Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AddNexusReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub nexus: NexusId,
    /// UUID and URI of the replica to be added
    pub replica: ReplicaUri,
    /// auto start rebuilding
    pub auto_rebuild: bool,
}
impl AddNexusReplica {
    /// Return new `Self` from it's properties
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

/// Remove Replica from Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoveNexusReplica {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub nexus: NexusId,
    /// UUID and URI of the replica to be added
    pub replica: ReplicaUri,
}
impl RemoveNexusReplica {
    /// Return new `Self`
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
