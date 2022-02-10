#![allow(clippy::field_reassign_with_default)]
use super::super::RestClient;

pub use common_lib::{
    mbus_api,
    types::v0::{
        message_bus::{
            AddNexusChild, BlockDevice, Child, ChildUri, CreateNexus, CreatePool, CreateReplica,
            CreateVolume, DestroyNexus, DestroyPool, DestroyReplica, DestroyVolume, Filter,
            GetBlockDevices, JsonGrpcRequest, Nexus, NexusId, Node, NodeId, Pool, PoolDeviceUri,
            PoolId, Protocol, RemoveNexusChild, Replica, ReplicaId, ReplicaShareProtocol,
            ShareNexus, ShareReplica, Specs, Topology, UnshareNexus, UnshareReplica, VolumeId,
            VolumeLabels, VolumePolicy, Watch, WatchCallback, WatchResourceId,
        },
        openapi::{apis, apis::actix_server::RestError, models, tower::client},
        store::pool::PoolLabel,
    },
};

use common_lib::{IntoOption, IntoVec};
pub use models::rest_json_error::Kind as RestJsonErrorKind;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Create Replica Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreateReplicaBody {
    /// size of the replica in bytes
    pub size: u64,
    /// thin provisioning
    pub thin: bool,
    /// protocol to expose the replica over
    pub share: Protocol,
}
impl From<models::CreateReplicaBody> for CreateReplicaBody {
    fn from(src: models::CreateReplicaBody) -> Self {
        Self {
            size: src.size as u64,
            thin: src.thin,
            share: match src.share {
                None => Protocol::None,
                Some(models::ReplicaShareProtocol::Nvmf) => Protocol::Nvmf,
            },
        }
    }
}

/// Create Pool Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreatePoolBody {
    /// disk device paths or URIs to be claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
    /// labels to be set on the pool
    pub labels: Option<PoolLabel>,
}
impl From<models::CreatePoolBody> for CreatePoolBody {
    fn from(src: models::CreatePoolBody) -> Self {
        Self {
            disks: src.disks.iter().cloned().map(From::from).collect(),
            labels: src.labels,
        }
    }
}
impl From<CreatePool> for CreatePoolBody {
    fn from(create: CreatePool) -> Self {
        CreatePoolBody {
            disks: create.disks,
            labels: create.labels,
        }
    }
}
impl CreatePoolBody {
    /// convert into message bus type
    pub fn bus_request(&self, node_id: NodeId, pool_id: PoolId) -> CreatePool {
        CreatePool {
            node: node_id,
            id: pool_id,
            disks: self.disks.clone(),
            labels: self.labels.clone(),
        }
    }
}
impl From<CreateReplica> for CreateReplicaBody {
    fn from(create: CreateReplica) -> Self {
        CreateReplicaBody {
            size: create.size,
            thin: create.thin,
            share: create.share,
        }
    }
}
impl CreateReplicaBody {
    /// convert into message bus type
    pub fn bus_request(&self, node_id: NodeId, pool_id: PoolId, uuid: ReplicaId) -> CreateReplica {
        CreateReplica {
            node: node_id,
            name: None,
            uuid,
            pool: pool_id,
            size: self.size,
            thin: self.thin,
            share: self.share,
            managed: false,
            owners: Default::default(),
        }
    }
}

/// Create Nexus Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreateNexusBody {
    /// size of the device in bytes
    pub size: u64,
    /// replica can be iscsi and nvmf remote targets or a local spdk bdev
    /// (i.e. bdev:///name-of-the-bdev).
    ///
    /// uris to the targets we connect to
    pub children: Vec<ChildUri>,
}
impl From<CreateNexus> for CreateNexusBody {
    fn from(create: CreateNexus) -> Self {
        Self {
            size: create.size,
            children: create.children.into_vec(),
        }
    }
}
impl From<models::CreateNexusBody> for CreateNexusBody {
    fn from(src: models::CreateNexusBody) -> Self {
        Self {
            size: src.size as u64,
            children: src.children.into_iter().map(From::from).collect(),
        }
    }
}
impl CreateNexusBody {
    /// convert into message bus type
    pub fn bus_request(&self, node_id: NodeId, nexus_id: NexusId) -> CreateNexus {
        CreateNexus {
            node: node_id,
            uuid: nexus_id,
            size: self.size,
            children: self.children.clone().into_vec(),
            managed: false,
            owner: None,
            config: None,
        }
    }
}

/// Create Volume Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreateVolumeBody {
    /// size of the volume in bytes
    pub size: u64,
    /// number of storage replicas
    pub replicas: u64,
    /// Volume policy used to determine if and how to replace a replica
    pub policy: VolumePolicy,
    /// Volume topology used to determine how to place/distribute the data
    pub topology: Option<Topology>,
    /// Volume labels, used ot store custom volume information
    pub labels: Option<VolumeLabels>,
}
impl From<models::CreateVolumeBody> for CreateVolumeBody {
    fn from(src: models::CreateVolumeBody) -> Self {
        Self {
            size: src.size as u64,
            replicas: src.replicas as u64,
            policy: src.policy.into(),
            topology: src.topology.into_opt(),
            labels: src.labels,
        }
    }
}
impl From<CreateVolume> for CreateVolumeBody {
    fn from(create: CreateVolume) -> Self {
        CreateVolumeBody {
            size: create.size,
            replicas: create.replicas,
            policy: create.policy,
            topology: create.topology,
            labels: create.labels,
        }
    }
}
impl CreateVolumeBody {
    /// convert into message bus type
    pub fn to_create_volume(&self, volume_id: VolumeId) -> CreateVolume {
        CreateVolume {
            uuid: volume_id,
            size: self.size,
            replicas: self.replicas,
            policy: self.policy.clone(),
            topology: self.topology.clone(),
            labels: self.labels.clone(),
        }
    }
}

impl RestClient {
    /// Get Autogenerated Openapi client v0
    pub fn v00(&self) -> client::direct::ApiClient {
        self.openapi_client_v0.clone()
    }
}
