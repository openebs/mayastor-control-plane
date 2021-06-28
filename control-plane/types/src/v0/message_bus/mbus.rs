#![allow(clippy::field_reassign_with_default)]
use paperclip::{
    actix::Apiv2Schema,
    v2::{
        models::{DataType, DataTypeFormat},
        schema::TypedData,
    },
};
use percent_encoding::percent_decode_str;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, convert::TryFrom, fmt::Debug};

use crate::v0::store::{nexus, pool, replica, volume};
use std::{ops::Deref, str::FromStr};
use strum_macros::{EnumString, ToString};

pub const VERSION: &str = "v0";

/// Available Message Bus channels
#[derive(Clone, Debug)]
#[allow(non_camel_case_types)]
pub enum Channel {
    /// Version 0 of the Channels
    v0(ChannelVs),
}

impl FromStr for Channel {
    type Err = strum::ParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source.split('/').next() {
            Some(VERSION) => {
                let c: ChannelVs = source[VERSION.len() + 1 ..].parse()?;
                Ok(Self::v0(c))
            }
            _ => Err(strum::ParseError::VariantNotFound),
        }
    }
}

impl ToString for Channel {
    fn to_string(&self) -> String {
        match self {
            Self::v0(channel) => format!("v0/{}", channel.to_string()),
        }
    }
}

impl Default for Channel {
    fn default() -> Self {
        Channel::v0(ChannelVs::Default)
    }
}

/// Versioned Channels
#[derive(Clone, Debug, EnumString, ToString)]
#[strum(serialize_all = "camelCase")]
pub enum ChannelVs {
    /// Default
    Default,
    /// Registration of mayastor instances with the control plane
    Registry,
    /// Node Service which exposes the registered mayastor instances
    Node,
    /// Pool Service which manages mayastor pools and replicas
    Pool,
    /// Volume Service which manages mayastor volumes
    Volume,
    /// Nexus Service which manages mayastor nexuses
    Nexus,
    /// Keep it In Sync Service
    Kiiss,
    /// Json gRPC Agent
    JsonGrpc,
    /// Core Agent combines Node, Pool and Volume services
    Core,
    /// Watcher Agent
    Watcher,
}
impl Default for ChannelVs {
    fn default() -> Self {
        ChannelVs::Default
    }
}

impl From<ChannelVs> for Channel {
    fn from(channel: ChannelVs) -> Self {
        Channel::v0(channel)
    }
}

/// Versioned Message Id's
#[derive(Debug, PartialEq, Clone, ToString, EnumString)]
#[strum(serialize_all = "camelCase")]
pub enum MessageIdVs {
    /// Default
    Default,
    /// Liveness Probe
    Liveness,
    /// Update Config
    ConfigUpdate,
    /// Request current Config
    ConfigGetCurrent,
    /// Register mayastor
    Register,
    /// Deregister mayastor
    Deregister,
    /// Node Service
    /// Get all node information
    GetNodes,
    /// Pool Service
    ///
    /// Get pools with filter
    GetPools,
    /// Create Pool,
    CreatePool,
    /// Destroy Pool,
    DestroyPool,
    /// Get replicas with filter
    GetReplicas,
    /// Create Replica,
    CreateReplica,
    /// Destroy Replica,
    DestroyReplica,
    /// Share Replica,
    ShareReplica,
    /// Unshare Replica,
    UnshareReplica,
    /// Volume Service
    ///
    /// Get nexuses with filter
    GetNexuses,
    /// Create nexus
    CreateNexus,
    /// Destroy Nexus
    DestroyNexus,
    /// Share Nexus
    ShareNexus,
    /// Unshare Nexus
    UnshareNexus,
    /// Remove a child from its parent nexus
    RemoveNexusChild,
    /// Add a child to a nexus
    AddNexusChild,
    /// Get all volumes
    GetVolumes,
    /// Create Volume,
    CreateVolume,
    /// Delete Volume
    DestroyVolume,
    /// Publish Volume,
    PublishVolume,
    /// Unpublish Volume
    UnpublishVolume,
    /// Share Volume
    ShareVolume,
    /// Unshare Volume
    UnshareVolume,
    /// Add nexus to volume
    AddVolumeNexus,
    /// Remove nexus from volume
    RemoveVolumeNexus,
    /// Generic JSON gRPC message
    JsonGrpc,
    /// Get block devices
    GetBlockDevices,
    /// Create new Resource Watch
    CreateWatch,
    /// Get watches
    GetWatches,
    /// Delete Resource Watch
    DeleteWatch,
    /// Get Specs
    GetSpecs,
}

/// Liveness Probe
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Liveness {}

/// Mayastor configurations
/// Currently, we have the global mayastor config and the child states config
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
pub enum Config {
    /// Mayastor global config
    MayastorConfig,
    /// Mayastor child states config
    ChildStatesConfig,
}
impl Default for Config {
    fn default() -> Self {
        Config::MayastorConfig
    }
}

/// Config Messages

/// Update mayastor configuration
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ConfigUpdate {
    /// type of config being updated
    pub kind: Config,
    /// actual config data
    pub data: Vec<u8>,
}

/// Request message configuration used by mayastor to request configuration
/// from a control plane service
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ConfigGetCurrent {
    /// type of config requested
    pub kind: Config,
}
/// Reply message configuration returned by a controle plane service to mayastor
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct ReplyConfig {
    /// config data
    pub config: Vec<u8>,
}

/// Registration

/// Register message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Register {
    /// id of the mayastor instance
    pub id: NodeId,
    /// grpc_endpoint of the mayastor instance
    pub grpc_endpoint: String,
}

/// Deregister message payload
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct Deregister {
    /// id of the mayastor instance
    pub id: NodeId,
}

/// Node Service
///
/// Get all the nodes
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetNodes {}

/// State of the Node
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq, Apiv2Schema,
)]
pub enum NodeState {
    /// Node has unexpectedly disappeared
    Unknown,
    /// Node is deemed online if it has not missed the
    /// registration keep alive deadline
    Online,
    /// Node is deemed offline if has missed the
    /// registration keep alive deadline
    Offline,
}

impl Default for NodeState {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Node information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct Node {
    /// id of the mayastor instance
    pub id: NodeId,
    /// grpc_endpoint of the mayastor instance
    pub grpc_endpoint: String,
    /// deemed state of the node
    pub state: NodeState,
}

/// Filter Objects based on one of the following criteria
/// # Example:
/// // Get all nexuses from the node `node_id`
/// let nexuses =
///     MessageBus::get_nexuses(Filter::Node(node_id)).await.unwrap();
#[derive(Serialize, Deserialize, Debug, Clone, strum_macros::ToString)] // likely this ToString does not do the right thing...
pub enum Filter {
    /// All objects
    None,
    /// Filter by Node id
    Node(NodeId),
    /// Pool filters
    ///
    /// Filter by Pool id
    Pool(PoolId),
    /// Filter by Node and Pool id
    NodePool(NodeId, PoolId),
    /// Filter by Node and Replica id
    NodeReplica(NodeId, ReplicaId),
    /// Filter by Node, Pool and Replica id
    NodePoolReplica(NodeId, PoolId, ReplicaId),
    /// Filter by Pool and Replica id
    PoolReplica(PoolId, ReplicaId),
    /// Filter by Replica id
    Replica(ReplicaId),
    /// Volume filters
    ///
    /// Filter by Node and Nexus
    NodeNexus(NodeId, NexusId),
    /// Filter by Nexus
    Nexus(NexusId),
    /// Filter by Node and Volume
    NodeVolume(NodeId, VolumeId),
    /// Filter by Volume
    Volume(VolumeId),
}
impl Default for Filter {
    fn default() -> Self {
        Self::None
    }
}

macro_rules! bus_impl_string_id_inner {
    ($Name:ident, $Doc:literal) => {
        #[doc = $Doc]
        #[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Hash)]
        pub struct $Name(String);

        impl std::fmt::Display for $Name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl $Name {
            /// Build Self from a string trait id
            pub fn as_str<'a>(&'a self) -> &'a str {
                self.0.as_str()
            }
        }

        impl From<&str> for $Name {
            fn from(id: &str) -> Self {
                $Name::from(id)
            }
        }
        impl From<String> for $Name {
            fn from(id: String) -> Self {
                $Name::from(id.as_str())
            }
        }

        impl From<&$Name> for $Name {
            fn from(id: &$Name) -> $Name {
                id.clone()
            }
        }

        impl From<$Name> for String {
            fn from(id: $Name) -> String {
                id.to_string()
            }
        }
    };
}

macro_rules! bus_impl_string_id {
    ($Name:ident, $Doc:literal) => {
        bus_impl_string_id_inner!($Name, $Doc);
        impl Default for $Name {
            /// Generates new blank identifier
            fn default() -> Self {
                $Name(uuid::Uuid::default().to_string())
            }
        }
        impl $Name {
            /// Build Self from a string trait id
            pub fn from<T: Into<String>>(id: T) -> Self {
                $Name(id.into())
            }
            /// Generates new random identifier
            pub fn new() -> Self {
                $Name(uuid::Uuid::new_v4().to_string())
            }
        }
        impl TypedData for $Name {
            fn data_type() -> DataType {
                DataType::String
            }
            fn format() -> Option<DataTypeFormat> {
                None
            }
        }
    };
}

macro_rules! bus_impl_string_uuid {
    ($Name:ident, $Doc:literal) => {
        bus_impl_string_id_inner!($Name, $Doc);
        impl Default for $Name {
            /// Generates new blank identifier
            fn default() -> Self {
                $Name(uuid::Uuid::default().to_string())
            }
        }
        impl $Name {
            /// Build Self from a string trait id
            pub fn from<T: Into<String>>(id: T) -> Self {
                $Name(id.into())
            }
            /// Generates new random identifier
            pub fn new() -> Self {
                $Name(uuid::Uuid::new_v4().to_string())
            }
        }
        impl TypedData for $Name {
            fn data_type() -> DataType {
                DataType::String
            }
            fn format() -> Option<DataTypeFormat> {
                Some(DataTypeFormat::Uuid)
            }
        }
    };
}

macro_rules! bus_impl_string_id_percent_decoding {
    ($Name:ident, $Doc:literal) => {
        bus_impl_string_id_inner!($Name, $Doc);
        impl Default for $Name {
            fn default() -> Self {
                $Name("".to_string())
            }
        }
        impl $Name {
            /// Build Self from a string trait id
            pub fn from<T: Into<String>>(id: T) -> Self {
                let src: String = id.into();
                let decoded_src = percent_decode_str(src.clone().as_str())
                    .decode_utf8()
                    .unwrap_or(src.into())
                    .to_string();
                $Name(decoded_src)
            }
        }
        impl TypedData for $Name {
            fn data_type() -> DataType {
                DataType::String
            }
            fn format() -> Option<DataTypeFormat> {
                None
            }
        }
    };
}

bus_impl_string_id!(NodeId, "ID of a mayastor node");
bus_impl_string_id!(PoolId, "ID of a mayastor pool");
bus_impl_string_uuid!(ReplicaId, "UUID of a mayastor pool replica");
bus_impl_string_uuid!(NexusId, "UUID of a mayastor nexus");
bus_impl_string_id_percent_decoding!(ChildUri, "URI of a mayastor nexus child");
bus_impl_string_uuid!(VolumeId, "UUID of a mayastor volume");
bus_impl_string_id!(JsonGrpcMethod, "JSON gRPC method");
bus_impl_string_id!(
    JsonGrpcParams,
    "Parameters to be passed to a JSON gRPC method"
);

/// Pool Service
/// Get all the pools from specific node or None for all nodes
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetPools {
    /// Filter request
    pub filter: Filter,
}

/// State of the Pool
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq, Apiv2Schema,
)]
pub enum PoolState {
    /// unknown state
    Unknown = 0,
    /// the pool is in normal working order
    Online = 1,
    /// the pool has experienced a failure but can still function
    Degraded = 2,
    /// the pool is completely inaccessible
    Faulted = 3,
}

impl Default for PoolState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for PoolState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}

/// Pool information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct Pool {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
    /// absolute disk paths claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
    /// current state of the pool
    pub state: PoolState,
    /// size of the pool in bytes
    pub capacity: u64,
    /// used bytes from the pool
    pub used: u64,
}

// online > degraded > unknown/faulted
impl PartialOrd for PoolState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self {
            PoolState::Unknown => match other {
                PoolState::Unknown => None,
                PoolState::Online => Some(Ordering::Less),
                PoolState::Degraded => Some(Ordering::Less),
                PoolState::Faulted => None,
            },
            PoolState::Online => match other {
                PoolState::Unknown => Some(Ordering::Greater),
                PoolState::Online => Some(Ordering::Equal),
                PoolState::Degraded => Some(Ordering::Greater),
                PoolState::Faulted => Some(Ordering::Greater),
            },
            PoolState::Degraded => match other {
                PoolState::Unknown => Some(Ordering::Greater),
                PoolState::Online => Some(Ordering::Less),
                PoolState::Degraded => Some(Ordering::Equal),
                PoolState::Faulted => Some(Ordering::Greater),
            },
            PoolState::Faulted => match other {
                PoolState::Unknown => None,
                PoolState::Online => Some(Ordering::Less),
                PoolState::Degraded => Some(Ordering::Less),
                PoolState::Faulted => Some(Ordering::Equal),
            },
        }
    }
}

/// Create Pool Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreatePool {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
    /// disk device paths or URIs to be claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
}

/// Destroy Pool Request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DestroyPool {
    /// id of the mayastor instance
    pub node: NodeId,
    /// id of the pool
    pub id: PoolId,
}

/// Get all the replicas from specific node and pool
/// or None for all nodes or all pools
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetReplicas {
    /// Filter request
    pub filter: Filter,
}

/// Replica information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct Replica {
    /// id of the mayastor instance
    pub node: NodeId,
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
    /// state of the replica
    pub state: ReplicaState,
}

impl From<Replica> for DestroyReplica {
    fn from(replica: Replica) -> Self {
        Self {
            node: replica.node,
            pool: replica.pool,
            uuid: replica.uuid,
        }
    }
}

/// Create Replica Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateReplica {
    /// id of the mayastor instance
    pub node: NodeId,
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
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq, Apiv2Schema)]
pub struct ReplicaOwners {
    volume: Option<VolumeId>,
    nexuses: Vec<NexusId>,
}
impl ReplicaOwners {
    /// Check if this replica is owned by any nexuses or a volume
    pub fn is_owned(&self) -> bool {
        self.volume.is_some() || !self.nexuses.is_empty()
    }
    /// Check if this replica is owned by this volume
    pub fn owned_by(&self, id: &VolumeId) -> bool {
        self.volume.as_ref() == Some(id)
    }
    /// Create new owners from the volume Id
    pub fn new(volume: &VolumeId) -> Self {
        Self {
            volume: Some(volume.clone()),
            nexuses: vec![],
        }
    }
    /// The replica is no longer part of the volume
    pub fn disowned_by_volume(&mut self) {
        let _ = self.volume.take();
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
    /// protocol used for exposing the replica
    pub protocol: ReplicaShareProtocol,
}

impl From<ShareReplica> for UnshareReplica {
    fn from(share: ShareReplica) -> Self {
        Self {
            node: share.node,
            pool: share.pool,
            uuid: share.uuid,
        }
    }
}
impl From<&Replica> for ShareReplica {
    fn from(from: &Replica) -> Self {
        Self {
            node: from.node.clone(),
            pool: from.pool.clone(),
            uuid: from.uuid.clone(),
            protocol: ReplicaShareProtocol::Nvmf,
        }
    }
}
impl From<&Replica> for UnshareReplica {
    fn from(from: &Replica) -> Self {
        Self {
            node: from.node.clone(),
            pool: from.pool.clone(),
            uuid: from.uuid.clone(),
        }
    }
}
impl From<UnshareReplica> for ShareReplica {
    fn from(share: UnshareReplica) -> Self {
        Self {
            node: share.node,
            pool: share.pool,
            uuid: share.uuid,
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
}

/// Indicates what protocol the bdev is shared as
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq, Apiv2Schema,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum Protocol {
    /// not shared by any of the variants
    Off = 0,
    /// shared as NVMe-oF TCP
    Nvmf = 1,
    /// shared as iSCSI
    Iscsi = 2,
    /// shared as NBD
    Nbd = 3,
}

impl Protocol {
    /// Is the protocol set to be shared
    pub fn shared(&self) -> bool {
        self != &Self::Off
    }
}
impl Default for Protocol {
    fn default() -> Self {
        Self::Off
    }
}
impl From<i32> for Protocol {
    fn from(src: i32) -> Self {
        match src {
            0 => Self::Off,
            1 => Self::Nvmf,
            2 => Self::Iscsi,
            _ => Self::Off,
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
impl From<NexusShareProtocol> for Protocol {
    fn from(src: NexusShareProtocol) -> Self {
        match src {
            NexusShareProtocol::Nvmf => Self::Nvmf,
            NexusShareProtocol::Iscsi => Self::Iscsi,
        }
    }
}
/// Convert a device URI to a share Protocol
/// Uses the URI scheme to determine the protocol
/// Temporary WA until the share is added to the mayastor RPC
impl TryFrom<&str> for Protocol {
    type Error = String;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(if value.is_empty() {
            Protocol::Off
        } else {
            match url::Url::from_str(value) {
                Ok(url) => match url.scheme() {
                    "nvmf" => Self::Nvmf,
                    "iscsi" => Self::Iscsi,
                    "nbd" => Self::Nbd,
                    other => return Err(format!("Invalid nexus protocol: {}", other)),
                },
                Err(error) => {
                    tracing::error!("error parsing uri's ({}) protocol: {}", value, error);
                    return Err(error.to_string());
                }
            }
        })
    }
}

/// The protocol used to share the nexus.
#[derive(
    Serialize, Deserialize, Debug, Copy, Clone, EnumString, ToString, Eq, PartialEq, Apiv2Schema,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum NexusShareProtocol {
    /// shared as NVMe-oF TCP
    Nvmf = 1,
    /// shared as iSCSI
    Iscsi = 2,
}

impl std::cmp::PartialEq<Protocol> for NexusShareProtocol {
    fn eq(&self, other: &Protocol) -> bool {
        &Protocol::from(*self) == other
    }
}
impl Default for NexusShareProtocol {
    fn default() -> Self {
        Self::Nvmf
    }
}
impl From<i32> for NexusShareProtocol {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Nvmf,
            2 => Self::Iscsi,
            _ => panic!("Invalid nexus share protocol {}", src),
        }
    }
}

/// The protocol used to share the replica.
#[derive(
    Serialize, Deserialize, Debug, Clone, Copy, EnumString, ToString, Eq, PartialEq, Apiv2Schema,
)]
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

/// State of the Replica
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq, Apiv2Schema,
)]
#[strum(serialize_all = "camelCase")]
#[serde(rename_all = "camelCase")]
pub enum ReplicaState {
    /// unknown state
    Unknown = 0,
    /// the replica is in normal working order
    Online = 1,
    /// the replica has experienced a failure but can still function
    Degraded = 2,
    /// the replica is completely inaccessible
    Faulted = 3,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for ReplicaState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}

/// Volume Nexuses
///
/// Get all the nexuses with a filter selection
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct GetNexuses {
    /// Filter request
    pub filter: Filter,
}

/// Nexus information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct Nexus {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub uuid: NexusId,
    /// size of the volume in bytes
    pub size: u64,
    /// current state of the nexus
    pub state: NexusState,
    /// array of children
    pub children: Vec<Child>,
    /// URI of the device for the volume (missing if not published).
    /// Missing property and empty string are treated the same.
    pub device_uri: String,
    /// total number of rebuild tasks
    pub rebuilds: u32,
    /// protocol used for exposing the nexus
    pub share: Protocol,
}

/// Child information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct Child {
    /// uri of the child device
    pub uri: ChildUri,
    /// state of the child
    pub state: ChildState,
    /// current rebuild progress (%)
    pub rebuild_progress: Option<i32>,
}

/// Child State information
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub enum ChildState {
    /// Default Unknown state
    Unknown = 0,
    /// healthy and contains the latest bits
    Online = 1,
    /// rebuild is in progress (or other recoverable error)
    Degraded = 2,
    /// unrecoverable error (control plane must act)
    Faulted = 3,
}
impl Default for ChildState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for ChildState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}

/// Nexus State information
#[derive(
    Serialize, Deserialize, Debug, Clone, EnumString, ToString, Eq, PartialEq, Apiv2Schema,
)]
pub enum NexusState {
    /// Default Unknown state
    Unknown = 0,
    /// healthy and working
    Online = 1,
    /// not healthy but is able to serve IO (i.e. rebuild is in progress)
    Degraded = 2,
    /// broken and unable to serve IO
    Faulted = 3,
}
impl Default for NexusState {
    fn default() -> Self {
        Self::Unknown
    }
}
impl From<i32> for NexusState {
    fn from(src: i32) -> Self {
        match src {
            1 => Self::Online,
            2 => Self::Degraded,
            3 => Self::Faulted,
            _ => Self::Unknown,
        }
    }
}

/// Create Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct CreateNexus {
    /// id of the mayastor instance
    pub node: NodeId,
    /// the nexus uuid will be set to this
    pub uuid: NexusId,
    /// size of the device in bytes
    pub size: u64,
    /// replica can be iscsi and nvmf remote targets or a local spdk bdev
    /// (i.e. bdev:///name-of-the-bdev).
    ///
    /// uris to the targets we connect to
    pub children: Vec<ChildUri>,
    /// Managed by our control plane
    pub managed: bool,
    /// Volume which owns this nexus, if any
    pub owner: Option<VolumeId>,
}

/// Destroy Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct DestroyNexus {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub uuid: NexusId,
}

impl From<Nexus> for DestroyNexus {
    fn from(nexus: Nexus) -> Self {
        Self {
            node: nexus.node,
            uuid: nexus.uuid,
        }
    }
}

/// Share Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct ShareNexus {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub uuid: NexusId,
    /// encryption key
    pub key: Option<String>,
    /// share protocol
    pub protocol: NexusShareProtocol,
}

impl From<(&Nexus, Option<String>, NexusShareProtocol)> for ShareNexus {
    fn from((nexus, key, protocol): (&Nexus, Option<String>, NexusShareProtocol)) -> Self {
        Self {
            node: nexus.node.clone(),
            uuid: nexus.uuid.clone(),
            key,
            protocol,
        }
    }
}
impl From<&Nexus> for UnshareNexus {
    fn from(from: &Nexus) -> Self {
        Self {
            node: from.node.clone(),
            uuid: from.uuid.clone(),
        }
    }
}
impl From<ShareNexus> for UnshareNexus {
    fn from(share: ShareNexus) -> Self {
        Self {
            node: share.node,
            uuid: share.uuid,
        }
    }
}

/// Unshare Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct UnshareNexus {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub uuid: NexusId,
}

/// Remove Child from Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RemoveNexusChild {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub nexus: NexusId,
    /// URI of the child device to be removed
    pub uri: ChildUri,
}

impl From<AddNexusChild> for RemoveNexusChild {
    fn from(add: AddNexusChild) -> Self {
        Self {
            node: add.node,
            nexus: add.nexus,
            uri: add.uri,
        }
    }
}

/// Add child to Nexus Request
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct AddNexusChild {
    /// id of the mayastor instance
    pub node: NodeId,
    /// uuid of the nexus
    pub nexus: NexusId,
    /// URI of the child device to be added
    pub uri: ChildUri,
    /// auto start rebuilding
    pub auto_rebuild: bool,
}

/// Volumes
///
/// Volume information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
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
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub struct LabelTopology {
    /// node topology
    node_topology: NodeTopology,
    /// pool topology
    pool_topology: PoolTopology,
}

/// Volume topology used to determine how to place/distribute the data
/// Should either be labelled or explicit, not both.
/// If neither is used then the control plane will select from all available resources.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
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
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
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
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub struct InclusiveLabel(
    /// inner label
    pub String,
);

/// Placement node topology used by volume operations
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub struct NodeTopology {
    /// exclusive labels
    #[serde(default)]
    pub exclusion: Vec<ExclusiveLabel>,
    /// inclusive labels
    #[serde(default)]
    pub inclusion: Vec<InclusiveLabel>,
}

/// Placement pool topology used by volume operations
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub struct PoolTopology {
    /// inclusive labels
    #[serde(default)]
    pub inclusion: Vec<InclusiveLabel>,
}

/// Explicit node placement Selection for a volume
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub struct ExplicitTopology {
    /// replicas can only be placed on these nodes
    #[serde(default)]
    pub allowed_nodes: Vec<NodeId>,
    /// preferred nodes to place the replicas
    #[serde(default)]
    pub preferred_nodes: Vec<NodeId>,
}

/// Volume Healing policy used to determine if and how to replace a replica
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
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

/// Generic JSON gRPC request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JsonGrpcRequest {
    /// id of the mayastor instance
    pub node: NodeId,
    /// JSON gRPC method to call
    pub method: JsonGrpcMethod,
    /// parameters to be passed to the above method
    pub params: JsonGrpcParams,
}

/// Partition information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub struct Partition {
    /// devname of parent device to which this partition belongs
    pub parent: String,
    /// partition number
    pub number: u32,
    /// partition name
    pub name: String,
    /// partition scheme: gpt, dos, ...
    pub scheme: String,
    /// partition type identifier
    pub typeid: String,
    /// UUID identifying partition
    pub uuid: String,
}

/// Filesystem information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub struct Filesystem {
    /// filesystem type: ext3, ntfs, ...
    pub fstype: String,
    /// volume label
    pub label: String,
    /// UUID identifying the volume (filesystem)
    pub uuid: String,
    /// path where filesystem is currently mounted
    pub mountpoint: String,
}

/// Block device information
#[derive(Serialize, Deserialize, Default, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct BlockDevice {
    /// entry in /dev associated with device
    pub devname: String,
    /// currently "disk" or "partition"
    pub devtype: String,
    /// major device number
    pub devmajor: u32,
    /// minor device number
    pub devminor: u32,
    /// device model - useful for identifying mayastor devices
    pub model: String,
    /// official device path
    pub devpath: String,
    /// list of udev generated symlinks by which device may be identified
    pub devlinks: Vec<String>,
    /// size of device in (512 byte) blocks
    pub size: u64,
    /// partition information in case where device represents a partition
    pub partition: Partition,
    /// filesystem information in case where a filesystem is present
    pub filesystem: Filesystem,
    /// identifies if device is available for use (ie. is not "currently" in
    /// use)
    pub available: bool,
}
/// Get block devices
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetBlockDevices {
    /// id of the mayastor instance
    pub node: NodeId,
    /// specifies whether to get all devices or only usable devices
    pub all: bool,
}

///
/// Watcher Agent

/// Create new Resource Watch
/// Uniquely identifiable by resource_id and callback
pub type CreateWatch = Watch;

/// Watch Resource in the store
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Watch {
    /// id of the resource to watch on
    pub id: WatchResourceId,
    /// callback used to notify the watcher of a change
    pub callback: WatchCallback,
    /// type of watch
    pub watch_type: WatchType,
}

/// Get Resource Watches
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetWatchers {
    /// id of the resource to get
    pub resource: WatchResourceId,
}

/// Uniquely Identify a Resource
pub type Resource = WatchResourceId;

/// The different resource types that can be watched
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
#[allow(dead_code)]
pub enum WatchResourceId {
    /// nodes
    Node(NodeId),
    /// pools
    Pool(PoolId),
    /// replicas
    Replica(ReplicaId),
    /// replica state
    ReplicaState(ReplicaId),
    /// replica spec
    ReplicaSpec(ReplicaId),
    /// nexuses
    Nexus(NexusId),
    /// volumes
    Volume(VolumeId),
}
impl Default for WatchResourceId {
    fn default() -> Self {
        Self::Node(Default::default())
    }
}
impl ToString for WatchResourceId {
    fn to_string(&self) -> String {
        match self {
            WatchResourceId::Node(id) => format!("nodes/{}", id.to_string()),
            WatchResourceId::Pool(id) => format!("pools/{}", id.to_string()),
            WatchResourceId::Replica(id) => {
                format!("replicas/{}", id.to_string())
            }
            WatchResourceId::ReplicaState(id) => {
                format!("replicas_state/{}", id.to_string())
            }
            WatchResourceId::ReplicaSpec(id) => {
                format!("replicas_spec/{}", id.to_string())
            }
            WatchResourceId::Nexus(id) => format!("nexuses/{}", id.to_string()),
            WatchResourceId::Volume(id) => format!("volumes/{}", id.to_string()),
        }
    }
}

/// The difference types of watches
#[derive(Serialize, Deserialize, Debug, Clone, Apiv2Schema, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum WatchType {
    /// Watch for changes on the desired state
    Desired,
    /// Watch for changes on the actual state
    Actual,
    /// Watch for both `Desired` and `Actual` changes
    All,
}
impl Default for WatchType {
    fn default() -> Self {
        Self::All
    }
}

/// Delete Watch which was previously created by CreateWatcher
/// Fields should match the ones used for the creation
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct DeleteWatch {
    /// id of the resource to delete the watch from
    pub id: WatchResourceId,
    /// callback to be deleted
    pub callback: WatchCallback,
    /// type of watch to be deleted
    pub watch_type: WatchType,
}

/// Watcher Callback types
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum WatchCallback {
    /// HTTP URI callback
    Uri(String),
}
impl Default for WatchCallback {
    fn default() -> Self {
        Self::Uri(Default::default())
    }
}

/// Retrieve all specs from core agent
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct GetSpecs {}

/// Specs detailing the requested configuration of the objects.
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Specs {
    /// volume specs
    pub volumes: Vec<volume::VolumeSpec>,
    /// nexus specs
    pub nexuses: Vec<nexus::NexusSpec>,
    /// pool specs
    pub pools: Vec<pool::PoolSpec>,
    /// replica specs
    pub replicas: Vec<replica::ReplicaSpec>,
}

/// Pool device URI
/// Can be specified in the form of a file path or a URI
/// eg: /dev/sda, aio:///dev/sda, malloc:///disk?size_mb=100
#[derive(Serialize, Deserialize, Debug, Clone, Eq, PartialEq, Apiv2Schema)]
pub struct PoolDeviceUri(String);
impl Deref for PoolDeviceUri {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl Default for PoolDeviceUri {
    fn default() -> Self {
        Self("malloc:///disk?size_mb=100".into())
    }
}
impl From<&str> for PoolDeviceUri {
    fn from(device: &str) -> Self {
        Self(device.to_string())
    }
}
impl From<&String> for PoolDeviceUri {
    fn from(device: &String) -> Self {
        Self(device.clone())
    }
}
impl From<String> for PoolDeviceUri {
    fn from(device: String) -> Self {
        Self(device)
    }
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
