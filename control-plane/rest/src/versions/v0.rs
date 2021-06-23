#![allow(clippy::field_reassign_with_default)]
use super::super::ActixRestClient;
use crate::{ClientError, ClientResult, JsonGeneric, RestUri};
use actix_web::{body::Body, http::StatusCode, web::Json, HttpResponse, ResponseError};
use async_trait::async_trait;
use mbus_api::{ReplyError, ReplyErrorKind};
use paperclip::actix::{api_v2_errors, api_v2_errors_overlay, Apiv2Schema};
use serde::{Deserialize, Serialize};
use std::{
    convert::TryFrom,
    fmt::{Display, Formatter},
    string::ToString,
};
use strum_macros::{self, Display};
pub use types::v0::message_bus::mbus::{
    AddNexusChild, BlockDevice, Child, ChildUri, CreateNexus, CreatePool, CreateReplica,
    CreateVolume, DestroyNexus, DestroyPool, DestroyReplica, DestroyVolume, Filter,
    GetBlockDevices, JsonGrpcRequest, Nexus, NexusId, Node, NodeId, Pool, PoolDeviceUri, PoolId,
    Protocol, RemoveNexusChild, Replica, ReplicaId, ReplicaShareProtocol, ShareNexus, ShareReplica,
    Specs, Topology, UnshareNexus, UnshareReplica, Volume, VolumeHealPolicy, VolumeId, Watch,
    WatchCallback, WatchResourceId,
};

pub use mbus_api::message_bus::v0::Message;

/// Create Replica Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema)]
pub struct CreateReplicaBody {
    /// size of the replica in bytes
    pub size: u64,
    /// thin provisioning
    pub thin: bool,
    /// protocol to expose the replica over
    pub share: Protocol,
}
/// Create Pool Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema)]
pub struct CreatePoolBody {
    /// disk device paths or URIs to be claimed by the pool
    pub disks: Vec<PoolDeviceUri>,
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
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema)]
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
        CreateNexusBody {
            size: create.size,
            children: create.children,
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
            children: self.children.clone(),
            managed: false,
            owner: None,
        }
    }
}

/// Create Volume Body JSON
#[derive(Serialize, Deserialize, Default, Debug, Clone, Apiv2Schema)]
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

/// Contains the query parameters that can be passed when calling
/// get_block_devices
#[derive(Deserialize, Serialize, Default, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct GetBlockDeviceQueryParams {
    /// specifies whether to list all devices or only usable ones
    pub all: Option<bool>,
}

/// Watch query parameters used by various watch calls
#[derive(Deserialize, Serialize, Default, Apiv2Schema)]
#[serde(rename_all = "camelCase")]
pub struct WatchTypeQueryParam {
    /// URL callback
    pub callback: RestUri,
}

/// Watch Resource in the store
#[derive(Serialize, Deserialize, Debug, Default, Clone, Apiv2Schema, Eq, PartialEq)]
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
    async fn json_grpc(&self, args: JsonGrpcRequest) -> ClientResult<JsonGeneric>;
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
    async fn get_specs(&self) -> ClientResult<Specs>;
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
    GetVolumes(Volume),
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
        let nodes = get_all!(self, GetNodes).await?;
        Ok(nodes)
    }

    async fn get_pools(&self, filter: Filter) -> ClientResult<Vec<Pool>> {
        let pools = get_filter!(self, filter, GetPools).await?;
        Ok(pools)
    }

    async fn create_pool(&self, args: CreatePool) -> ClientResult<Pool> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.id);
        let pool = self.put(urn, CreatePoolBody::from(args)).await?;
        Ok(pool)
    }

    async fn destroy_pool(&self, args: DestroyPool) -> ClientResult<()> {
        let urn = format!("/v0/nodes/{}/pools/{}", &args.node, &args.id);
        self.del(urn).await?;
        Ok(())
    }

    async fn get_replicas(&self, filter: Filter) -> ClientResult<Vec<Replica>> {
        let replicas = get_filter!(self, filter, GetReplicas).await?;
        Ok(replicas)
    }

    async fn create_replica(&self, args: CreateReplica) -> ClientResult<Replica> {
        let urn = format!(
            "/v0/nodes/{}/pools/{}/replicas/{}",
            &args.node, &args.pool, &args.uuid
        );
        let replica = self.put(urn, CreateReplicaBody::from(args)).await?;
        Ok(replica)
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
        let nexuses = get_filter!(self, filter, GetNexuses).await?;
        Ok(nexuses)
    }

    async fn create_nexus(&self, args: CreateNexus) -> ClientResult<Nexus> {
        let urn = format!("/v0/nodes/{}/nexuses/{}", &args.node, &args.uuid);
        let replica = self.put(urn, CreateNexusBody::from(args)).await?;
        Ok(replica)
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
        let replica = self.put(urn, Body::Empty).await?;
        Ok(replica)
    }
    async fn get_nexus_children(&self, filter: Filter) -> ClientResult<Vec<Child>> {
        let children = get_filter!(self, filter, GetChildren).await?;
        Ok(children)
    }

    async fn get_volumes(&self, filter: Filter) -> ClientResult<Vec<Volume>> {
        let volumes = get_filter!(self, filter, GetVolumes).await?;
        Ok(volumes)
    }

    async fn create_volume(&self, args: CreateVolume) -> ClientResult<Volume> {
        let urn = format!("/v0/volumes/{}", &args.uuid);
        let volume = self.put(urn, CreateVolumeBody::from(args)).await?;
        Ok(volume)
    }

    async fn destroy_volume(&self, args: DestroyVolume) -> ClientResult<()> {
        let urn = format!("/v0/volumes/{}", &args.uuid);
        self.del(urn).await?;
        Ok(())
    }

    async fn json_grpc(&self, args: JsonGrpcRequest) -> ClientResult<JsonGeneric> {
        let urn = format!("/v0/nodes/{}/jsongrpc/{}", args.node, args.method);
        self.put(urn, Body::from(args.params.to_string())).await
    }

    async fn get_block_devices(&self, args: GetBlockDevices) -> ClientResult<Vec<BlockDevice>> {
        let urn = format!("/v0/nodes/{}/block_devices?all={}", args.node, args.all);
        self.get_vec(urn).await
    }

    async fn get_watches(&self, resource: WatchResourceId) -> ClientResult<Vec<RestWatch>> {
        let urn = format!("/v0/watches/{}", resource.to_string());
        self.get_vec(urn).await
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

    async fn get_specs(&self) -> ClientResult<Specs> {
        let urn = "/v0/specs".to_string();
        self.get(urn).await
    }
}

impl From<CreatePoolBody> for Body {
    fn from(src: CreatePoolBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap())
    }
}
impl From<CreateReplicaBody> for Body {
    fn from(src: CreateReplicaBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap())
    }
}
impl From<CreateNexusBody> for Body {
    fn from(src: CreateNexusBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap())
    }
}
impl From<CreateVolumeBody> for Body {
    fn from(src: CreateVolumeBody) -> Self {
        Body::from(serde_json::to_value(src).unwrap())
    }
}

impl ActixRestClient {
    /// Get RestClient v0
    pub fn v0(&self) -> impl RestClient {
        self.clone()
    }
}

/// Rest Error
#[api_v2_errors(
    code = 400,
    description = "Request Timeout",
    code = 401,
    description = "Unauthorized",
    code = 404,
    description = "Not Found",
    code = 408,
    description = "Bad Request",
    code = 416,
    description = "Range Not satisfiable",
    code = 412,
    description = "Precondition Failed",
    code = 422,
    description = "Unprocessable entity",
    code = 500,
    description = "Internal Server Error",
    code = 501,
    description = "Not Implemented",
    code = 503,
    description = "Service Unavailable",
    code = 504,
    description = "Gateway Timeout",
    code = 507,
    description = "Insufficient Storage",
    default_schema = "RestJsonError"
)]
#[derive(Debug)]
pub struct RestError {
    inner: ReplyError,
}

/// Rest Cluster Error
/// (RestError without 404 NotFound) used for Get /$resources handlers
#[api_v2_errors_overlay(404)]
#[derive(Debug)]
pub struct RestClusterError(pub RestError);

impl From<RestError> for RestClusterError {
    fn from(error: RestError) -> Self {
        RestClusterError(error)
    }
}

/// Rest Json Error format
#[derive(Serialize, Deserialize, Debug, Default, Apiv2Schema)]
pub struct RestJsonError {
    /// error kind
    error: RestJsonErrorKind,
    /// detailed error information
    details: String,
}

/// RestJson error kind
#[derive(Serialize, Deserialize, Debug, Apiv2Schema)]
#[allow(missing_docs)]
pub enum RestJsonErrorKind {
    // code=400, description="Request Timeout",
    Timeout,
    // code=500, description="Internal Error",
    Deserialize,
    // code=500, description="Internal Error",
    Internal,
    // code=400, description="Bad Request",
    InvalidArgument,
    // code=504, description="Gateway Timeout",
    DeadlineExceeded,
    // code=404, description="Not Found",
    NotFound,
    // code=422, description="Unprocessable entity",
    AlreadyExists,
    // code=401, description="Unauthorized",
    PermissionDenied,
    // code=507, description="Insufficient Storage",
    ResourceExhausted,
    // code=412, description="Precondition Failed",
    FailedPrecondition,
    // code=412, description="Precondition Failed",
    NotShared,
    // code=412, description="Precondition Failed",
    NotPublished,
    // code=412, description="Precondition Failed",
    AlreadyPublished,
    // code=412, description="Precondition Failed",
    AlreadyShared,
    // code=503, description="Service Unavailable",
    Aborted,
    // code=416, description="Range Not satisfiable",
    OutOfRange,
    // code=501, description="Not Implemented",
    Unimplemented,
    // code=503, description="Service Unavailable",
    Unavailable,
    // code=401, description="Unauthorized",
    Unauthenticated,
    // code=401, description="Unauthorized",
    Unauthorized,
    // code=409, description="Conflict",
    Conflict,
    // code=507, description="Insufficient Storage",
    FailedPersist,
}

impl Default for RestJsonErrorKind {
    fn default() -> Self {
        Self::NotFound
    }
}

impl RestJsonError {
    fn new(error: RestJsonErrorKind, details: &str) -> Self {
        Self {
            error,
            details: details.to_string(),
        }
    }
}

impl RestError {
    fn get_resp_error(&self) -> HttpResponse {
        let details = self.inner.extra.clone();
        match &self.inner.kind {
            ReplyErrorKind::WithMessage => {
                let error = RestJsonError::new(RestJsonErrorKind::Internal, &details);
                HttpResponse::InternalServerError().json(error)
            }
            ReplyErrorKind::DeserializeReq => {
                let error = RestJsonError::new(RestJsonErrorKind::Deserialize, &details);
                HttpResponse::BadRequest().json(error)
            }
            ReplyErrorKind::Internal => {
                let error = RestJsonError::new(RestJsonErrorKind::Internal, &details);
                HttpResponse::InternalServerError().json(error)
            }
            ReplyErrorKind::Timeout => {
                let error = RestJsonError::new(RestJsonErrorKind::Timeout, &details);
                HttpResponse::RequestTimeout().json(error)
            }
            ReplyErrorKind::InvalidArgument => {
                let error = RestJsonError::new(RestJsonErrorKind::InvalidArgument, &details);
                HttpResponse::BadRequest().json(error)
            }
            ReplyErrorKind::DeadlineExceeded => {
                let error = RestJsonError::new(RestJsonErrorKind::DeadlineExceeded, &details);
                HttpResponse::GatewayTimeout().json(error)
            }
            ReplyErrorKind::NotFound => {
                let error = RestJsonError::new(RestJsonErrorKind::NotFound, &details);
                HttpResponse::NotFound().json(error)
            }
            ReplyErrorKind::AlreadyExists => {
                let error = RestJsonError::new(RestJsonErrorKind::AlreadyExists, &details);
                HttpResponse::UnprocessableEntity().json(error)
            }
            ReplyErrorKind::PermissionDenied => {
                let error = RestJsonError::new(RestJsonErrorKind::PermissionDenied, &details);
                HttpResponse::Unauthorized().json(error)
            }
            ReplyErrorKind::ResourceExhausted => {
                let error = RestJsonError::new(RestJsonErrorKind::ResourceExhausted, &details);
                HttpResponse::InsufficientStorage().json(error)
            }
            ReplyErrorKind::FailedPrecondition => {
                let error = RestJsonError::new(RestJsonErrorKind::FailedPrecondition, &details);
                HttpResponse::PreconditionFailed().json(error)
            }
            ReplyErrorKind::Aborted => {
                let error = RestJsonError::new(RestJsonErrorKind::Aborted, &details);
                HttpResponse::ServiceUnavailable().json(error)
            }
            ReplyErrorKind::OutOfRange => {
                let error = RestJsonError::new(RestJsonErrorKind::OutOfRange, &details);
                HttpResponse::RangeNotSatisfiable().json(error)
            }
            ReplyErrorKind::Unimplemented => {
                let error = RestJsonError::new(RestJsonErrorKind::Unimplemented, &details);
                HttpResponse::NotImplemented().json(error)
            }
            ReplyErrorKind::Unavailable => {
                let error = RestJsonError::new(RestJsonErrorKind::Unavailable, &details);
                HttpResponse::ServiceUnavailable().json(error)
            }
            ReplyErrorKind::Unauthenticated => {
                let error = RestJsonError::new(RestJsonErrorKind::Unauthenticated, &details);
                HttpResponse::Unauthorized().json(error)
            }
            ReplyErrorKind::Unauthorized => {
                let error = RestJsonError::new(RestJsonErrorKind::Unauthorized, &details);
                HttpResponse::Unauthorized().json(error)
            }
            ReplyErrorKind::Conflict => {
                let error = RestJsonError::new(RestJsonErrorKind::Conflict, &details);
                HttpResponse::Conflict().json(error)
            }
            ReplyErrorKind::FailedPersist => {
                let error = RestJsonError::new(RestJsonErrorKind::FailedPersist, &details);
                HttpResponse::InsufficientStorage().json(error)
            }
            ReplyErrorKind::AlreadyShared => {
                let error = RestJsonError::new(RestJsonErrorKind::AlreadyShared, &details);
                HttpResponse::PreconditionFailed().json(error)
            }
            ReplyErrorKind::NotShared => {
                let error = RestJsonError::new(RestJsonErrorKind::NotShared, &details);
                HttpResponse::PreconditionFailed().json(error)
            }
            ReplyErrorKind::NotPublished => {
                let error = RestJsonError::new(RestJsonErrorKind::NotPublished, &details);
                HttpResponse::PreconditionFailed().json(error)
            }
            ReplyErrorKind::AlreadyPublished => {
                let error = RestJsonError::new(RestJsonErrorKind::AlreadyPublished, &details);
                HttpResponse::PreconditionFailed().json(error)
            }
        }
    }
}
// used by the trait ResponseError only when the default error_response trait
// method is used.
impl Display for RestError {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
impl ResponseError for RestError {
    fn status_code(&self) -> StatusCode {
        self.get_resp_error().status()
    }
    fn error_response(&self) -> HttpResponse {
        self.get_resp_error()
    }
}
impl From<ReplyError> for RestError {
    fn from(inner: ReplyError) -> Self {
        Self { inner }
    }
}
impl From<mbus_api::Error> for RestError {
    fn from(from: mbus_api::Error) -> Self {
        Self { inner: from.into() }
    }
}
impl From<RestError> for HttpResponse {
    fn from(src: RestError) -> Self {
        src.get_resp_error()
    }
}

/// Respond using a message bus response Result<Response,ReplyError>
/// In case of success the Response is sent via the body of a HttpResponse with
/// StatusCode OK.
/// Otherwise, the RestError is returned, also as a HttpResponse/ResponseError.
#[derive(Debug)]
pub struct RestRespond<T>(Result<T, RestError>);

// used by the trait ResponseError only when the default error_response trait
// method is used.
impl<T> Display for RestRespond<T> {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!()
    }
}
impl<T: Serialize> RestRespond<T> {
    /// Respond with a Result<T, ReplyError>
    pub fn result(from: Result<T, ReplyError>) -> Result<Json<T>, RestError> {
        match from {
            Ok(v) => Ok(Json::<T>(v)),
            Err(e) => Err(e.into()),
        }
    }
    /// Respond T with success
    pub fn ok(object: T) -> Result<Json<T>, RestError> {
        Ok(Json(object))
    }
}
impl<T> From<Result<T, ReplyError>> for RestRespond<T> {
    fn from(src: Result<T, ReplyError>) -> Self {
        RestRespond(src.map_err(RestError::from))
    }
}
impl<T: Serialize> From<RestRespond<T>> for HttpResponse {
    fn from(src: RestRespond<T>) -> Self {
        match src.0 {
            Ok(resp) => HttpResponse::Ok().json(resp),
            Err(error) => error.into(),
        }
    }
}
