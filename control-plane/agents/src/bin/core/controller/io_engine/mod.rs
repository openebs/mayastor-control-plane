/// Message translation to agent types from rpc v0,v1 types.
mod translation;
pub(crate) mod v0;
pub(crate) mod v1;

use crate::node::service::NodeCommsTimeout;
use agents::errors::{GrpcConnectUri, SvcError};
use async_trait::async_trait;
use common_lib::{
    transport_api::{v0::BlockDevices, MessageId},
    types::v0::transport::{
        AddNexusChild, ApiVersion, Child, CreateNexus, CreatePool, CreateReplica, DestroyNexus,
        DestroyPool, DestroyReplica, FaultNexusChild, GetBlockDevices, Nexus, NodeId, PoolState,
        Register, RemoveNexusChild, Replica, ShareNexus, ShareReplica, ShutdownNexus, UnshareNexus,
        UnshareReplica,
    },
};
use grpc::context::timeout_grpc;

use snafu::ResultExt;
use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
    time::Duration,
};

/// Context with a gRPC client and a lock to serialize mutating gRPC calls.
#[derive(Clone, Debug)]
pub(crate) struct GrpcContext {
    /// The gRPC CRUD lock.
    lock: Arc<tokio::sync::Mutex<()>>,
    /// The node identifier.
    node: NodeId,
    /// The gRPC socket endpoint.
    socket_endpoint: std::net::SocketAddr,
    /// The gRPC URI endpoint.
    endpoint: tonic::transport::Endpoint,
    /// The gRPC connect and request timeouts.
    comms_timeouts: NodeCommsTimeout,
    /// The api version to be used for calls.
    api_version: ApiVersion,
}

impl GrpcContext {
    /// Return a new `Self` from the given parameters.
    pub(crate) fn new(
        lock: Arc<tokio::sync::Mutex<()>>,
        node: &NodeId,
        socket_endpoint: std::net::SocketAddr,
        comms_timeouts: &NodeCommsTimeout,
        request: Option<MessageId>,
        api_version: ApiVersion,
    ) -> Result<Self, SvcError> {
        let uri = http::uri::Uri::builder()
            .scheme("http")
            .authority(socket_endpoint.to_string())
            .path_and_query("")
            .build()
            .context(GrpcConnectUri {
                node_id: node.to_string(),
                uri: socket_endpoint.to_string(),
            })?;

        let timeout = request
            .map(|r| timeout_grpc(r, comms_timeouts.opts().clone()))
            .unwrap_or_else(|| comms_timeouts.request());

        let endpoint = tonic::transport::Endpoint::from(uri)
            .connect_timeout(comms_timeouts.connect() + Duration::from_millis(500))
            .timeout(timeout);

        Ok(Self {
            node: node.clone(),
            lock,
            socket_endpoint,
            endpoint,
            comms_timeouts: comms_timeouts.clone(),
            api_version,
        })
    }
    /// Override the timeout config in the context for the given request.
    fn override_timeout(&mut self, _request: Option<MessageId>) {
        self.endpoint = self
            .endpoint
            .clone()
            .connect_timeout(self.comms_timeouts.connect() + Duration::from_millis(500))
            .timeout(self.comms_timeouts.request());
    }
    /// Get the inner lock guard for this context.
    pub(crate) async fn lock(&self) -> GrpcLockGuard {
        self.lock.clone().lock_owned().await
    }
    /// Use this context to connect a grpc client and return it.
    pub(crate) async fn connect(&self) -> Result<GrpcClient, SvcError> {
        GrpcClient::new(self).await
    }
    /// Use this context to connect a grpc client and return it.
    /// This client differs from `GrpcClient` from `Self::connect` in that it ensures only 1 request
    /// is sent at a time among all `GrpcClientLocked` clients.
    pub(crate) async fn connect_locked(
        &self,
    ) -> Result<GrpcClientLocked, (GrpcLockGuard, SvcError)> {
        GrpcClientLocked::new(self).await
    }
    /// Get the socket endpoint.
    pub(crate) fn endpoint(&self) -> std::net::SocketAddr {
        self.socket_endpoint
    }
}

/// Wrapper over all gRPC Clients types
#[derive(Clone)]
pub(crate) struct GrpcClient {
    /// The vX gRPC IoEngine Client.
    pub(crate) client: Box<dyn NodeApi>,
}

impl Deref for GrpcClient {
    type Target = Box<dyn NodeApi>;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl GrpcClient {
    /// Create a new grpc client with a context.
    pub(crate) async fn new(context: &GrpcContext) -> Result<Self, SvcError> {
        Ok(Self {
            client: match context.api_version {
                ApiVersion::V0 => Box::new(v0::RpcClient::new(context).await?),
                ApiVersion::V1 => Box::new(v1::RpcClient::new(context).await?),
            },
        })
    }
}

/// Async Lock guard for gRPC operations.
/// It's used by the GrpcClientLocked to ensure there's only one operation in progress
/// at a time while still allowing for multiple gRPC clients.
type GrpcLockGuard = tokio::sync::OwnedMutexGuard<()>;

/// Wrapper over all gRPC Clients types with implicit locking for serialization.
pub(crate) struct GrpcClientLocked {
    /// The gRPC auto CRUD guard lock.
    _lock: GrpcLockGuard,
    client: GrpcClient,
    context: GrpcContext,
}

impl GrpcClientLocked {
    /// Create new locked client from the given context
    /// A connection is established with the timeouts specified from the context.
    /// Only one `Self` is allowed at a time by making use of a lock guard.
    pub(crate) async fn new(context: &GrpcContext) -> Result<Self, (GrpcLockGuard, SvcError)> {
        let _lock = context.lock().await;

        let client = match GrpcClient::new(context).await {
            Ok(client) => client,
            Err(error) => return Err((_lock, error)),
        };

        Ok(Self {
            _lock,
            client,
            context: context.clone(),
        })
    }
    /// Reconnect the client to use for the given request
    /// This is useful when we want to issue the next gRPC using a different timeout
    /// todo: tower should allow us to handle this better by keeping the same "backend" client
    /// but modifying the timeout layer?
    pub(crate) async fn reconnect(self, request: MessageId) -> Result<Self, SvcError> {
        let mut context = self.context.clone();
        context.override_timeout(Some(request));

        let client = GrpcClient::new(&context).await?;

        Ok(Self {
            _lock: self._lock,
            client,
            context,
        })
    }
}

impl Deref for GrpcClientLocked {
    type Target = GrpcClient;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl DerefMut for GrpcClientLocked {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

#[async_trait]
#[dyn_clonable::clonable]
pub(crate) trait NodeApi:
    PoolListApi
    + PoolApi
    + ReplicaListApi
    + ReplicaApi
    + NexusListApi
    + NexusApi
    + HostApi
    + Sync
    + Send
    + Clone
{
}

#[async_trait]
pub(crate) trait PoolListApi {
    /// List pools based on api version in context.
    async fn list_pools(&self, id: &NodeId) -> Result<Vec<PoolState>, SvcError>;
}

#[async_trait]
pub(crate) trait PoolApi {
    /// Create a pool on the node via gRPC.
    async fn create_pool(&self, request: &CreatePool) -> Result<PoolState, SvcError>;
    /// Destroy a pool on the node via gRPC.
    async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError>;
}

#[async_trait]
pub(crate) trait ReplicaListApi {
    /// List replicas based on api version in context.
    async fn list_replicas(&self, id: &NodeId) -> Result<Vec<Replica>, SvcError>;
}

#[async_trait]
pub(crate) trait ReplicaApi {
    /// Create a replica on the pool via gRPC.
    async fn create_replica(&self, request: &CreateReplica) -> Result<Replica, SvcError>;
    /// Destroy a replica on the pool via gRPC.
    async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError>;

    /// Share a replica on the pool via gRPC.
    async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError>;
    /// Unshare a replica on the pool via gRPC.
    async fn unshare_replica(&self, request: &UnshareReplica) -> Result<String, SvcError>;
}

#[async_trait]
pub(crate) trait NexusListApi {
    /// List nexus based on api version in context.
    async fn list_nexus(&self, id: &NodeId) -> Result<Vec<Nexus>, SvcError>;
}

#[async_trait]
pub(crate) trait NexusApi {
    /// Create a nexus on a node via gRPC.
    async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError>;
    /// Destroy a nexus on a node via gRPC.
    async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError>;

    /// Share a nexus on the node via gRPC.
    async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError>;
    /// Unshare a nexus on the node via gRPC.
    async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<(), SvcError>;

    /// Add a child to a nexus via gRPC.
    async fn add_child(&self, request: &AddNexusChild) -> Result<Child, SvcError>;
    /// Remove a child from its parent nexus via gRPC.
    async fn remove_child(&self, request: &RemoveNexusChild) -> Result<(), SvcError>;
    /// Fault a child from its parent nexus via gRPC.
    async fn fault_child(&self, request: &FaultNexusChild) -> Result<(), SvcError>;

    /// Shutdown a nexus via gRPC.
    async fn shutdown_nexus(&self, request: &ShutdownNexus) -> Result<(), SvcError>;
}

#[async_trait]
pub(crate) trait HostApi {
    /// Probe node for liveness based on api version in context.
    async fn liveness_probe(&self) -> Result<Register, SvcError>;
    /// List blockdevices based on api versions.
    async fn list_blockdevices(&self, request: &GetBlockDevices) -> Result<BlockDevices, SvcError>;
}
