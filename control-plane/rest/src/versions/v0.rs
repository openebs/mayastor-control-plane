#![allow(clippy::field_reassign_with_default)]
use super::super::ActixRestClient;
use crate::{ClientError, ClientResult};
use actix_web::body::Body;

pub use common_lib::{
    mbus_api,
    types::v0::{
        message_bus::{
            AddNexusChild, BlockDevice, Child, ChildUri, CreateNexus, CreatePool, CreateReplica,
            CreateVolume, DestroyNexus, DestroyPool, DestroyReplica, DestroyVolume, Filter,
            GetBlockDevices, JsonGrpcRequest, Nexus, NexusId, Node, NodeId, Pool, PoolDeviceUri,
            PoolId, Protocol, RemoveNexusChild, Replica, ReplicaId, ReplicaShareProtocol,
            ShareNexus, ShareReplica, Specs, Topology, UnshareNexus, UnshareReplica,
            VolumeHealPolicy, VolumeId, Watch, WatchCallback, WatchResourceId,
        },
        openapi::{apis, models},
    },
};
use common_lib::{types::v0::message_bus::States, IntoVec};
pub use models::rest_json_error::Kind as RestJsonErrorKind;

use async_trait::async_trait;
use common_lib::types::v0::message_bus::Volume;
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, fmt::Debug, string::ToString};
use strum_macros::{self, Display};

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
            share: src.share.into(),
        }
    }
}

/// Create Pool Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
pub struct CreatePoolBody {
    /// disk device paths or URIs to be claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
}
impl From<models::CreatePoolBody> for CreatePoolBody {
    fn from(src: models::CreatePoolBody) -> Self {
        Self {
            disks: src.disks.iter().cloned().map(From::from).collect(),
        }
    }
}
impl From<CreatePool> for CreatePoolBody {
    fn from(create: CreatePool) -> Self {
        CreatePoolBody {
            disks: create.disks,
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
            uuid,
            pool: pool_id,
            size: self.size,
            thin: self.thin,
            share: self.share.clone(),
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
    // docs will be auto generated from the actual types
    #[allow(missing_docs)]
    pub policy: VolumeHealPolicy,
    #[allow(missing_docs)]
    pub topology: Topology,
}
impl From<models::CreateVolumeBody> for CreateVolumeBody {
    fn from(src: models::CreateVolumeBody) -> Self {
        Self {
            size: src.size as u64,
            replicas: src.replicas as u64,
            policy: src.policy.into(),
            topology: src.topology.into(),
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
        }
    }
}
impl CreateVolumeBody {
    /// convert into message bus type
    pub fn bus_request(&self, volume_id: VolumeId) -> CreateVolume {
        CreateVolume {
            uuid: volume_id,
            size: self.size,
            replicas: self.replicas,
            policy: self.policy.clone(),
            topology: self.topology.clone(),
        }
    }
}

/// Watch Resource in the store
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RestWatch {
    /// id of the resource to watch on
    pub resource: String,
    /// callback used to notify the watcher of a change
    pub callback: String,
}

impl TryFrom<&Watch> for RestWatch {
    type Error = ();
    fn try_from(value: &Watch) -> Result<Self, Self::Error> {
        match &value.callback {
            WatchCallback::Uri(uri) => Ok(Self {
                resource: value.id.to_string(),
                callback: uri.to_string(),
            }),
            /* other types are not implemented yet and should map to an error
             * _ => Err(()), */
        }
    }
}
impl From<models::RestWatch> for RestWatch {
    fn from(value: models::RestWatch) -> Self {
        RestWatch {
            resource: value.resource,
            callback: value.callback,
        }
    }
}

/// RestClient interface
#[async_trait(?Send)]
pub trait RestClient {
    /// Get all the known nodes
    async fn get_nodes(&self) -> ClientResult<Vec<Node>>;
    /// Get all the known pools
    async fn get_pools(&self, filter: Filter) -> ClientResult<Vec<Pool>>;
    /// Create new pool with arguments
    async fn create_pool(&self, args: CreatePool) -> ClientResult<Pool>;
    /// Destroy pool with arguments
    async fn destroy_pool(&self, args: DestroyPool) -> ClientResult<()>;
    /// Get all the known replicas
    async fn get_replicas(&self, filter: Filter) -> ClientResult<Vec<Replica>>;
    /// Create new replica with arguments
    async fn create_replica(&self, args: CreateReplica) -> ClientResult<Replica>;
    /// Destroy replica with arguments
    async fn destroy_replica(&self, args: DestroyReplica) -> ClientResult<()>;
    /// Share replica with arguments
    async fn share_replica(&self, args: ShareReplica) -> ClientResult<String>;
    /// Unshare replica with arguments
    async fn unshare_replica(&self, args: UnshareReplica) -> ClientResult<()>;
    /// Get all the known nexuses
    async fn get_nexuses(&self, filter: Filter) -> ClientResult<Vec<Nexus>>;
    /// Create new nexus with arguments
    async fn create_nexus(&self, args: CreateNexus) -> ClientResult<Nexus>;
    /// Destroy nexus with arguments
    async fn destroy_nexus(&self, args: DestroyNexus) -> ClientResult<()>;
    /// Share nexus
    async fn share_nexus(&self, args: ShareNexus) -> ClientResult<String>;
    /// Unshare nexus
    async fn unshare_nexus(&self, args: UnshareNexus) -> ClientResult<()>;
    /// Remove nexus child
    async fn remove_nexus_child(&self, args: RemoveNexusChild) -> ClientResult<()>;
    /// Add nexus child
    async fn add_nexus_child(&self, args: AddNexusChild) -> ClientResult<Child>;
    /// Get all children by filter
    async fn get_nexus_children(&self, filter: Filter) -> ClientResult<Vec<Child>>;
    /// Get all volumes by filter
    async fn get_volumes(&self, filter: Filter) -> ClientResult<Vec<Volume>>;
    /// Create volume
    async fn create_volume(&self, args: CreateVolume) -> ClientResult<Volume>;
    /// Destroy volume
    async fn destroy_volume(&self, args: DestroyVolume) -> ClientResult<()>;
    /// Generic JSON gRPC call
    async fn json_grpc(&self, args: JsonGrpcRequest) -> ClientResult<serde_json::Value>;
    /// Get block devices
    async fn get_block_devices(&self, args: GetBlockDevices) -> ClientResult<Vec<BlockDevice>>;
    /// Get all watches for resource
    async fn get_watches(&self, resource: WatchResourceId) -> ClientResult<Vec<RestWatch>>;
    /// Create new watch
    async fn create_watch(&self, resource: WatchResourceId, callback: url::Url)
        -> ClientResult<()>;
    /// Delete watch
    async fn delete_watch(&self, resource: WatchResourceId, callback: url::Url)
        -> ClientResult<()>;
    /// Get resource specs
    async fn get_specs(&self) -> ClientResult<models::Specs>;
    /// Get resource states
    async fn get_states(&self) -> ClientResult<States>;
}

#[derive(Display, Debug)]
#[allow(clippy::enum_variant_names)]
enum RestUrns {
    #[strum(serialize = "nodes")]
    GetNodes(Node),
    #[strum(serialize = "pools")]
    GetPools(Pool),
    #[strum(serialize = "replicas")]
    GetReplicas(Replica),
    #[strum(serialize = "nexuses")]
    GetNexuses(Nexus),
    #[strum(serialize = "children")]
    GetChildren(Child),
    #[strum(serialize = "volumes")]
    GetVolumes(Box<Volume>),
    /* does not work as expect as format! only takes literals...
     * #[strum(serialize = "nodes/{}/pools/{}")]
     * PutPool(Pool), */
}

macro_rules! get_all {
    ($S:ident, $T:ident) => {
        $S.get_vec(format!(
            "/v0/{}",
            RestUrns::$T(Default::default()).to_string()
        ))
    };
}
macro_rules! get_filter {
    ($S:ident, $F:ident, $T:ident) => {
        $S.get_vec(format!(
            "/v0/{}",
            get_filtered_urn($F, &RestUrns::$T(Default::default()))?
        ))
    };
}

fn get_filtered_urn(filter: Filter, r: &RestUrns) -> ClientResult<String> {
    let urn = match r {
        RestUrns::GetNodes(_) => match filter {
            Filter::None => "nodes".to_string(),
            Filter::Node(id) => format!("nodes/{}", id),
            _ => return Err(ClientError::filter("Invalid filter for Nodes")),
        },
        RestUrns::GetPools(_) => match filter {
            Filter::None => "pools".to_string(),
            Filter::Node(id) => format!("nodes/{}/pools", id),
            Filter::Pool(id) => format!("pools/{}", id),
            Filter::NodePool(n, p) => format!("nodes/{}/pools/{}", n, p),
            _ => return Err(ClientError::filter("Invalid filter for pools")),
        },
        RestUrns::GetReplicas(_) => match filter {
            Filter::None => "replicas".to_string(),
            Filter::Node(id) => format!("nodes/{}/replicas", id),
            Filter::Pool(id) => format!("pools/{}/replicas", id),
            Filter::Replica(id) => format!("replicas/{}", id),
            Filter::NodePool(n, p) => {
                format!("nodes/{}/pools/{}/replicas", n, p)
            }
            Filter::NodeReplica(n, r) => format!("nodes/{}/replicas/{}", n, r),
            Filter::NodePoolReplica(n, p, r) => {
                format!("nodes/{}/pools/{}/replicas/{}", n, p, r)
            }
            Filter::PoolReplica(p, r) => format!("pools/{}/replicas/{}", p, r),
            _ => return Err(ClientError::filter("Invalid filter for replicas")),
        },
        RestUrns::GetNexuses(_) => match filter {
            Filter::None => "nexuses".to_string(),
            Filter::Node(n) => format!("nodes/{}/nexuses", n),
            Filter::NodeNexus(n, x) => format!("nodes/{}/nexuses/{}", n, x),
            Filter::Nexus(x) => format!("nexuses/{}", x),
            _ => return Err(ClientError::filter("Invalid filter for nexuses")),
        },
        RestUrns::GetChildren(_) => match filter {
            Filter::NodeNexus(n, x) => {
                format!("nodes/{}/nexuses/{}/children", n, x)
            }
            Filter::Nexus(x) => format!("nexuses/{}/children", x),
            _ => return Err(ClientError::filter("Invalid filter for nexuses")),
        },
        RestUrns::GetVolumes(_) => match filter {
            Filter::None => "volumes".to_string(),
            Filter::Node(n) => format!("nodes/{}/volumes", n),
            Filter::Volume(x) => format!("volumes/{}", x),
            _ => return Err(ClientError::filter("Invalid filter for volumes")),
        },
    };

    Ok(urn)
}

#[async_trait(?Send)]
impl RestClient for ActixRestClient {
    async fn get_nodes(&self) -> ClientResult<Vec<Node>> {
        let nodes: Vec<models::Node> = get_all!(self, GetNodes).await?;
        Ok(nodes.into_iter().map(From::from).collect())
    }

    async fn get_pools(&self, filter: Filter) -> ClientResult<Vec<Pool>> {
        let pools: Vec<models::Pool> = get_filter!(self, filter, GetPools).await?;
        Ok(pools.into_iter().map(From::from).collect())
    }

    async fn create_pool(&self, args: CreatePool) -> ClientResult<Pool> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.id);
        let pool: models::Pool = self.put(urn, CreatePoolBody::from(args)).await?;
        Ok(pool.into())
    }

    async fn destroy_pool(&self, args: DestroyPool) -> ClientResult<()> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.id);
        self.del(urn).await?;
        Ok(())
    }

    async fn get_replicas(&self, filter: Filter) -> ClientResult<Vec<Replica>> {
        let replicas: Vec<models::Replica> = get_filter!(self, filter, GetReplicas).await?;
        Ok(replicas.into_iter().map(From::from).collect())
    }

    async fn create_replica(&self, args: CreateReplica) -> ClientResult<Replica> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}",
            &args.node, &args.pool, &args.uuid
        );
        let replica: models::Replica = self.put(urn, CreateReplicaBody::from(args)).await?;
        Ok(replica.into())
    }

    async fn destroy_replica(&self, args: DestroyReplica) -> ClientResult<()> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}",
            &args.node, &args.pool, &args.uuid
        );
        self.del(urn).await?;
        Ok(())
    }

    /// Share replica with arguments
    async fn share_replica(&self, args: ShareReplica) -> ClientResult<String> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}/share/{}",
            &args.node,
            &args.pool,
            &args.uuid,
            args.protocol.to_string()
        );
        let share = self.put(urn, Body::Empty).await?;
        Ok(share)
    }
    /// Unshare replica with arguments
    async fn unshare_replica(&self, args: UnshareReplica) -> ClientResult<()> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}/share",
            &args.node, &args.pool, &args.uuid
        );
        self.del(urn).await?;
        Ok(())
    }

    async fn get_nexuses(&self, filter: Filter) -> ClientResult<Vec<Nexus>> {
        let nexuses: Vec<models::Nexus> = get_filter!(self, filter, GetNexuses).await?;
        Ok(nexuses.into_iter().map(From::from).collect())
    }

    async fn create_nexus(&self, args: CreateNexus) -> ClientResult<Nexus> {
        let urn = format!("/v0/nodes/{}/nexuses/{}", &args.node, &args.uuid);
        let nexus: models::Nexus = self.put(urn, CreateNexusBody::from(args)).await?;
        Ok(nexus.into())
    }

    async fn destroy_nexus(&self, args: DestroyNexus) -> ClientResult<()> {
        let urn = format!("/v0/nodes/{}/nexuses/{}", &args.node, &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    /// Share nexus
    async fn share_nexus(&self, args: ShareNexus) -> ClientResult<String> {
        let urn = format!(
            "/v0/nodes/{}/nexuses/{}/share/{}",
            &args.node,
            &args.uuid,
            args.protocol.to_string()
        );
        let nexus = self.put(urn, Body::Empty).await?;
        Ok(nexus)
    }

    /// Unshare nexus
    async fn unshare_nexus(&self, args: UnshareNexus) -> ClientResult<()> {
        let urn = format!("/v0/nodes/{}/nexuses/{}/share", &args.node, &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    async fn remove_nexus_child(&self, args: RemoveNexusChild) -> ClientResult<()> {
        let urn = match url::Url::parse(args.uri.as_str()) {
            Ok(uri) => {
                // remove initial '/'
                uri.path()[1 ..].to_string()
            }
            _ => args.uri.to_string(),
        };
        self.del(urn).await?;
        Ok(())
    }

    async fn add_nexus_child(&self, args: AddNexusChild) -> ClientResult<Child> {
        let urn = format!(
            "/v0/nodes/{}/nexuses/{}/children/{}",
            &args.node, &args.nexus, &args.uri
        );
        let child: models::Child = self.put(urn, Body::Empty).await?;
        Ok(child.into())
    }
    async fn get_nexus_children(&self, filter: Filter) -> ClientResult<Vec<Child>> {
        let children: Vec<models::Child> = get_filter!(self, filter, GetChildren).await?;
        Ok(children.into_iter().map(From::from).collect())
    }

    async fn get_volumes(&self, filter: Filter) -> ClientResult<Vec<Volume>> {
        let volumes: Vec<models::Volume> = get_filter!(self, filter, GetVolumes).await?;
        Ok(volumes.into_iter().map(From::from).collect())
    }

    async fn create_volume(&self, args: CreateVolume) -> ClientResult<Volume> {
        let urn = format!("/v0/volumes/{}", &args.uuid);
        let volume: models::Volume = self.put(urn, CreateVolumeBody::from(args)).await?;
        Ok(volume.into())
    }

    async fn destroy_volume(&self, args: DestroyVolume) -> ClientResult<()> {
        let urn = format!("/v0/volumes/{}", &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    async fn json_grpc(&self, args: JsonGrpcRequest) -> ClientResult<serde_json::Value> {
        let urn = format!("/v0/nodes/{}/jsongrpc/{}", args.node, args.method);
        self.put(urn, Body::from(args.params.to_string())).await
    }

    async fn get_block_devices(&self, args: GetBlockDevices) -> ClientResult<Vec<BlockDevice>> {
        let urn = format!("/v0/nodes/{}/block_devices?all={}", args.node, args.all);
        let devices: Vec<models::BlockDevice> = self.get_vec(urn).await?;
        Ok(devices.into_iter().map(From::from).collect())
    }

    async fn get_watches(&self, resource: WatchResourceId) -> ClientResult<Vec<RestWatch>> {
        let urn = format!("/v0/watches/{}", resource.to_string());
        let watches: Vec<models::RestWatch> = self.get_vec(urn).await?;
        Ok(watches.into_iter().map(From::from).collect())
    }

    async fn create_watch(
        &self,
        resource: WatchResourceId,
        callback: url::Url,
    ) -> ClientResult<()> {
        let urn = format!(
            "/v0/watches/{}?callback={}",
            resource.to_string(),
            callback.to_string()
        );
        self.put(urn, Body::Empty).await
    }

    async fn delete_watch(
        &self,
        resource: WatchResourceId,
        callback: url::Url,
    ) -> ClientResult<()> {
        let urn = format!(
            "/v0/watches/{}?callback={}",
            resource.to_string(),
            callback.to_string()
        );
        self.del(urn).await
    }

    async fn get_specs(&self) -> ClientResult<models::Specs> {
        let urn = "/v0/specs".to_string();
        self.get(urn).await
    }

    async fn get_states(&self) -> ClientResult<States> {
        let urn = "/v0/states".to_string();
        self.get(urn).await
    }
}

impl From<CreatePoolBody> for Body {
    fn from(src: CreatePoolBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap().to_string())
    }
}
impl From<CreateReplicaBody> for Body {
    fn from(src: CreateReplicaBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap().to_string())
    }
}
impl From<CreateNexusBody> for Body {
    fn from(src: CreateNexusBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap().to_string())
    }
}
impl From<CreateVolumeBody> for Body {
    fn from(src: CreateVolumeBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap().to_string())
    }
}

impl ActixRestClient {
    /// Get RestClient v0
    pub fn v0(&self) -> impl RestClient {
        self.clone()
    }
    /// Get Autogenerated Openapi client v0
    pub fn v00(&self) -> apis::client::ApiClient {
        self.openapi_client_v0.clone()
    }
}
