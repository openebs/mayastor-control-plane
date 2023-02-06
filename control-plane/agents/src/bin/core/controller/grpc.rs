use crate::node::service::NodeCommsTimeout;
use agents::{
    errors::{GrpcConnect, GrpcConnectUri, GrpcRequest as GrpcRequestError, SvcError},
    msg_translation::{
        v0::{
            rpc_nexus_v2_to_agent as v0_rpc_nexus_v2_to_agent,
            rpc_pool_to_agent as v0_rpc_pool_to_agent,
            rpc_replica_to_agent as v0_rpc_replica_to_agent,
        },
        v1::{
            rpc_nexus_to_agent as v1_rpc_nexus_to_agent, rpc_pool_to_agent as v1_rpc_pool_to_agent,
            rpc_replica_to_agent as v1_rpc_replica_to_agent,
        },
        IoEngineToAgent,
    },
};
use common_lib::{
    transport_api::{v0::BlockDevices, MessageId, ResourceKind},
    types::v0::transport::{
        ApiVersion, GetBlockDevices, Nexus, NodeId, PoolState, Register, Replica,
    },
};
use grpc::{context::timeout_grpc, operations::registration};
use rpc::{
    io_engine::{IoEngineClientV0, ListBlockDevicesRequest as V0ListBlockDevicesRequest, Null},
    v1::{
        host::ListBlockDevicesRequest as V1ListBlockDevicesRequest, nexus::ListNexusOptions,
        pool::ListPoolOptions, replica::ListReplicaOptions,
    },
};
use snafu::ResultExt;
use std::{
    ops::{Deref, DerefMut},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tonic::transport::Channel;

/// Context with a gRPC client and a lock to serialize mutating gRPC calls.
#[derive(Clone)]
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

/// V0 Mayastor client.
pub(crate) type MayaClientV0 = IoEngineClientV0<Channel>;
/// V1 HostClient
pub(crate) type HostClient = rpc::v1::host::host_rpc_client::HostRpcClient<Channel>;
/// V1 ReplicaClient
pub(crate) type ReplicaClient = rpc::v1::replica::replica_rpc_client::ReplicaRpcClient<Channel>;
/// V1 NexusClient
pub(crate) type NexusClient = rpc::v1::nexus::nexus_rpc_client::NexusRpcClient<Channel>;

/// The V1 PoolClient.
pub(crate) type PoolClient = rpc::v1::pool::pool_rpc_client::PoolRpcClient<Channel>;

/// A collection of all collects for the V1 dataplane interface.
#[derive(Clone, Debug)]
pub(crate) struct MayaClientV1 {
    host: HostClient,
    replica: ReplicaClient,
    nexus: NexusClient,
    pool: PoolClient,
}

impl MayaClientV1 {
    /// Get the v1 replica client.
    pub(crate) fn replica(&self) -> ReplicaClient {
        self.replica.clone()
    }
    /// Get the v1 nexus client.
    pub(crate) fn nexus(self) -> NexusClient {
        self.nexus
    }
    /// Get the v1 pool client.
    pub(crate) fn pool(&self) -> PoolClient {
        self.pool.clone()
    }
}

/// Wrapper over all gRPC Clients types
#[derive(Clone)]
pub(crate) struct GrpcClient {
    #[allow(dead_code)]
    context: GrpcContext,
    /// The v0 gRPC IoEngine Client.
    pub(crate) io_engine_v0: Option<MayaClientV0>,
    /// The v1 gRPC IoEngine Client.
    pub(crate) io_engine_v1: Option<MayaClientV1>,
}

impl GrpcClient {
    /// Create a new grpc client with a context.
    pub(crate) async fn new(context: &GrpcContext) -> Result<Self, SvcError> {
        match context.api_version {
            ApiVersion::V0 => {
                let client_v0 = match tokio::time::timeout(
                    context.comms_timeouts.connect(),
                    MayaClientV0::connect(context.endpoint.clone()),
                )
                .await
                {
                    Err(_) => Err(SvcError::GrpcConnectTimeout {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                        timeout: context.comms_timeouts.connect(),
                    }),
                    Ok(client) => Ok(client.context(GrpcConnect {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                    })?),
                }?;

                Ok(Self {
                    context: context.clone(),
                    io_engine_v0: Some(client_v0),
                    io_engine_v1: None,
                })
            }
            ApiVersion::V1 => {
                let host = match tokio::time::timeout(
                    context.comms_timeouts.connect(),
                    HostClient::connect(context.endpoint.clone()),
                )
                .await
                {
                    Err(_) => Err(SvcError::GrpcConnectTimeout {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                        timeout: context.comms_timeouts.connect(),
                    }),
                    Ok(client) => Ok(client.context(GrpcConnect {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                    })?),
                }?;
                let replica = match tokio::time::timeout(
                    context.comms_timeouts.connect(),
                    ReplicaClient::connect(context.endpoint.clone()),
                )
                .await
                {
                    Err(_) => Err(SvcError::GrpcConnectTimeout {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                        timeout: context.comms_timeouts.connect(),
                    }),
                    Ok(client) => Ok(client.context(GrpcConnect {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                    })?),
                }?;
                let nexus = match tokio::time::timeout(
                    context.comms_timeouts.connect(),
                    NexusClient::connect(context.endpoint.clone()),
                )
                .await
                {
                    Err(_) => Err(SvcError::GrpcConnectTimeout {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                        timeout: context.comms_timeouts.connect(),
                    }),
                    Ok(client) => Ok(client.context(GrpcConnect {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                    })?),
                }?;
                let pool = match tokio::time::timeout(
                    context.comms_timeouts.connect(),
                    PoolClient::connect(context.endpoint.clone()),
                )
                .await
                {
                    Err(_) => Err(SvcError::GrpcConnectTimeout {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                        timeout: context.comms_timeouts.connect(),
                    }),
                    Ok(client) => Ok(client.context(GrpcConnect {
                        node_id: context.node.to_string(),
                        endpoint: context.endpoint.uri().to_string(),
                    })?),
                }?;
                Ok(Self {
                    context: context.clone(),
                    io_engine_v0: None,
                    io_engine_v1: Some(MayaClientV1 {
                        host,
                        replica,
                        nexus,
                        pool,
                    }),
                })
            }
        }
    }

    /// Get the v0 api client.
    pub(crate) fn client_v0(&self) -> Result<MayaClientV0, SvcError> {
        match self.io_engine_v0.clone() {
            Some(client) => Ok(client),
            None => Err(SvcError::InvalidArguments {}),
        }
    }

    /// Get the v1 api client wrapper.
    pub(crate) fn client_v1(&self) -> Result<MayaClientV1, SvcError> {
        match self.io_engine_v1.clone() {
            Some(client) => Ok(client),
            None => Err(SvcError::InvalidArguments {}),
        }
    }

    /// List blockdevices based on api versions.
    pub(crate) async fn list_blockdevices(
        &self,
        request: &GetBlockDevices,
    ) -> Result<BlockDevices, SvcError> {
        match self.context.api_version {
            ApiVersion::V0 => {
                let result = self
                    .client_v0()?
                    .list_block_devices(V0ListBlockDevicesRequest { all: request.all })
                    .await;

                let response = result
                    .context(GrpcRequestError {
                        resource: ResourceKind::Block,
                        request: "list_block_devices",
                    })?
                    .into_inner();

                let bdevs = response
                    .devices
                    .iter()
                    .map(|rpc_bdev| rpc_bdev.to_agent())
                    .collect();
                Ok(BlockDevices(bdevs))
            }
            ApiVersion::V1 => {
                let result = self
                    .client_v1()?
                    .host
                    .list_block_devices(V1ListBlockDevicesRequest { all: request.all })
                    .await;

                let response = result
                    .context(GrpcRequestError {
                        resource: ResourceKind::Block,
                        request: "list_block_devices",
                    })?
                    .into_inner();

                let bdevs = response
                    .devices
                    .iter()
                    .map(|rpc_bdev| rpc_bdev.to_agent())
                    .collect();
                Ok(BlockDevices(bdevs))
            }
        }
    }

    /// Probe node for liveness based on api version in context.
    pub(crate) async fn liveness_probe(&self) -> Result<Register, SvcError> {
        match self.context.api_version {
            ApiVersion::V0 => {
                self.client_v0()?
                    .get_mayastor_info(rpc::io_engine::Null {})
                    .await
                    .context(GrpcRequestError {
                        resource: ResourceKind::Node,
                        request: "v0::get_mayastor_info",
                    })?;

                // V0 GetMayastorInfo Liveness call doesn't return the registration info,
                // thus fill it from context and hard-code the version as V0
                Ok(Register {
                    id: self.context.node.clone(),
                    grpc_endpoint: self.context.endpoint(),
                    api_versions: Some(vec![ApiVersion::V0]),
                    instance_uuid: None,
                    node_nqn: None,
                })
            }
            ApiVersion::V1 => {
                // V1 liveness sends registration_info, which can be used to get the
                // actual state of dataplane
                let data = self.client_v1()?.host.get_mayastor_info(()).await.context(
                    GrpcRequestError {
                        resource: ResourceKind::Node,
                        request: "v1::get_mayastor_info",
                    },
                )?;

                let registration_info = match data.into_inner().registration_info {
                    Some(info) => info,
                    None => {
                        // The dataplane did not send anything in registration info, which should
                        // not happen.
                        return Err(SvcError::NodeNotOnline {
                            node: self.context.node.clone(),
                        });
                    }
                };

                Ok(Register {
                    id: registration_info.id.into(),
                    grpc_endpoint: std::net::SocketAddr::from_str(&registration_info.grpc_endpoint)
                        .map_err(|error| SvcError::NodeGrpcEndpoint {
                            node: self.context.node.clone(),
                            socket: registration_info.grpc_endpoint,
                            error,
                        })?,
                    api_versions: Some(
                        registration_info
                            .api_version
                            .into_iter()
                            .map(|v| {
                                // Get the list of supported api versions from dataplane
                                registration::traits::ApiVersion(
                                    rpc::v1::registration::ApiVersion::from_i32(v)
                                        .unwrap_or_default(),
                                )
                                .into()
                            })
                            .collect(),
                    ),
                    instance_uuid: registration_info
                        .instance_uuid
                        .and_then(|u| uuid::Uuid::parse_str(&u).ok()),
                    node_nqn: registration_info.hostnqn.and_then(|h| h.try_into().ok()),
                })
            }
        }
    }

    /// List replicas based on api version in context.
    pub(crate) async fn list_replicas(&self, id: &NodeId) -> Result<Vec<Replica>, SvcError> {
        match self.context.api_version {
            ApiVersion::V0 => {
                let rpc_replicas = self.client_v0()?.list_replicas_v2(Null {}).await.context(
                    GrpcRequestError {
                        resource: ResourceKind::Replica,
                        request: "list_replicas",
                    },
                )?;

                let rpc_replicas = &rpc_replicas.get_ref().replicas;

                let replicas = rpc_replicas
                    .iter()
                    .filter_map(|p| match v0_rpc_replica_to_agent(p, id) {
                        Ok(r) => Some(r),
                        Err(error) => {
                            tracing::error!(error=%error, "Could not convert rpc replica");
                            None
                        }
                    })
                    .collect();

                Ok(replicas)
            }
            ApiVersion::V1 => {
                let rpc_replicas = self
                    .client_v1()?
                    .replica()
                    .list_replicas(ListReplicaOptions {
                        name: None,
                        poolname: None,
                        uuid: None,
                    })
                    .await
                    .context(GrpcRequestError {
                        resource: ResourceKind::Replica,
                        request: "list_replicas",
                    })?;

                let rpc_replicas = &rpc_replicas.get_ref().replicas;

                let replicas = rpc_replicas
                    .iter()
                    .filter_map(|p| match v1_rpc_replica_to_agent(p, id) {
                        Ok(r) => Some(r),
                        Err(error) => {
                            tracing::error!(error=%error, "Could not convert rpc replica");
                            None
                        }
                    })
                    .collect();

                Ok(replicas)
            }
        }
    }

    /// List pools based on api version in context.
    pub(crate) async fn list_pools(&self, id: &NodeId) -> Result<Vec<PoolState>, SvcError> {
        match self.context.api_version {
            ApiVersion::V0 => {
                let rpc_pools =
                    self.client_v0()?
                        .list_pools(Null {})
                        .await
                        .context(GrpcRequestError {
                            resource: ResourceKind::Pool,
                            request: "list_pools",
                        })?;

                let rpc_pools = &rpc_pools.get_ref().pools;

                let pools = rpc_pools
                    .iter()
                    .map(|p| v0_rpc_pool_to_agent(p, id))
                    .collect();

                Ok(pools)
            }
            ApiVersion::V1 => {
                let rpc_pools = self
                    .client_v1()?
                    .pool()
                    .list_pools(ListPoolOptions {
                        name: None,
                        pooltype: None,
                        uuid: None,
                    })
                    .await
                    .context(GrpcRequestError {
                        resource: ResourceKind::Pool,
                        request: "list_pools",
                    })?;
                let rpc_pools = &rpc_pools.get_ref().pools;
                let pools = rpc_pools
                    .iter()
                    .map(|p| v1_rpc_pool_to_agent(p, id))
                    .collect();
                Ok(pools)
            }
        }
    }

    /// List nexus based on api version in context.
    pub(crate) async fn list_nexus(&self, id: &NodeId) -> Result<Vec<Nexus>, SvcError> {
        match self.context.api_version {
            ApiVersion::V0 => {
                let rpc_nexuses =
                    self.client_v0()?
                        .list_nexus_v2(Null {})
                        .await
                        .context(GrpcRequestError {
                            resource: ResourceKind::Nexus,
                            request: "list_nexus",
                        })?;

                let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;

                let nexuses = rpc_nexuses
                    .iter()
                    .filter_map(|n| match v0_rpc_nexus_v2_to_agent(n, id) {
                        Ok(n) => Some(n),
                        Err(error) => {
                            tracing::error!(error=%error, "Could not convert rpc nexus");
                            None
                        }
                    })
                    .collect();

                Ok(nexuses)
            }
            ApiVersion::V1 => {
                let rpc_nexuses = self
                    .client_v1()?
                    .nexus
                    .list_nexus(ListNexusOptions {
                        name: None,
                        uuid: None,
                    })
                    .await
                    .context(GrpcRequestError {
                        resource: ResourceKind::Nexus,
                        request: "list_nexus",
                    })?;

                let rpc_nexuses = &rpc_nexuses.get_ref().nexus_list;

                let nexuses = rpc_nexuses
                    .iter()
                    .filter_map(|n| match v1_rpc_nexus_to_agent(n, id) {
                        Ok(n) => Some(n),
                        Err(error) => {
                            tracing::error!(error=%error, "Could not convert rpc nexus");
                            None
                        }
                    })
                    .collect();

                Ok(nexuses)
            }
        }
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

        Ok(Self { _lock, client })
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
        })
    }
    /// Get the api version from grpc context.
    pub(crate) fn api_version(&self) -> ApiVersion {
        self.context.api_version.clone()
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
