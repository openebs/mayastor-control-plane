use crate::{
    core::wrapper::{rpc_nexus_v2_to_agent, rpc_pool_to_agent, rpc_replica_to_agent},
    node::service::NodeCommsTimeout,
};
use common::{
    errors::{GrpcConnect, GrpcConnectUri, GrpcRequestError, SvcError},
    msg_translation::IoEngineToAgent,
};
use common_lib::{
    transport_api::{v0::BlockDevices, MessageId, ResourceKind},
    types::v0::transport::{
        APIVersion, GetBlockDevices, Nexus, NodeId, PoolState, Register, Replica,
    },
};
use grpc::{context::timeout_grpc, operations::registration::traits::ApiVersion};
use rpc::{
    io_engine::{IoEngineClient, ListBlockDevicesRequest as V0ListBlockDevicesRequest, Null},
    v1::host::ListBlockDevicesRequest as V1ListBlockDevicesRequest,
};
use snafu::ResultExt;
use std::{
    ops::{Deref, DerefMut},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use tonic::transport::Channel;

/// Context with a gRPC client and a lock to serialize mutating gRPC calls
#[derive(Clone)]
pub(crate) struct GrpcContext {
    /// gRPC CRUD lock
    lock: Arc<tokio::sync::Mutex<()>>,
    /// node identifier
    node: NodeId,
    /// gRPC URI endpoint
    endpoint: tonic::transport::Endpoint,
    /// gRPC connect and request timeouts
    comms_timeouts: NodeCommsTimeout,
    /// the api version to be used for calls
    api_version: APIVersion,
}

impl GrpcContext {
    pub(crate) fn new(
        lock: Arc<tokio::sync::Mutex<()>>,
        node: &NodeId,
        endpoint: &str,
        comms_timeouts: &NodeCommsTimeout,
        request: Option<MessageId>,
        api_version: APIVersion,
    ) -> Result<Self, SvcError> {
        let uri = format!("http://{}", endpoint);
        let uri = http::uri::Uri::from_str(&uri).context(GrpcConnectUri {
            node_id: node.to_string(),
            uri: uri.clone(),
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
            endpoint,
            comms_timeouts: comms_timeouts.clone(),
            api_version,
        })
    }
    /// Override the timeout config in the context for the given request
    fn override_timeout(&mut self, _request: Option<MessageId>) {
        // let timeout = request
        //     .map(|r| r.timeout(self.comms_timeouts.request(), &bus()))
        //     .unwrap_or_else(|| self.comms_timeouts.request());

        self.endpoint = self
            .endpoint
            .clone()
            .connect_timeout(self.comms_timeouts.connect() + Duration::from_millis(500))
            .timeout(self.comms_timeouts.request());
    }
    pub(crate) async fn lock(&self) -> GrpcLockGuard {
        self.lock.clone().lock_owned().await
    }
    pub(crate) async fn connect(&self) -> Result<GrpcClient, SvcError> {
        GrpcClient::new(self).await
    }
    pub(crate) async fn connect_locked(
        &self,
    ) -> Result<GrpcClientLocked, (GrpcLockGuard, SvcError)> {
        GrpcClientLocked::new(self).await
    }
}

pub(crate) type MayaClientV0 = IoEngineClient<Channel>;
/// V1 HostClient
pub(crate) type HostClient = rpc::v1::host::host_rpc_client::HostRpcClient<Channel>;

#[derive(Clone, Debug)]
pub(crate) struct MayaClientV1 {
    host: HostClient,
}

/// Wrapper over all gRPC Clients types
#[derive(Clone)]
pub(crate) struct GrpcClient {
    #[allow(dead_code)]
    context: GrpcContext,
    /// v0 gRPC IoEngine Client
    pub(crate) io_engine_v0: Option<MayaClientV0>,
    /// v1 gRPC IoEngine Client
    pub(crate) io_engine_v1: Option<MayaClientV1>,
}

impl GrpcClient {
    /// create a new grpc client with a context
    pub(crate) async fn new(context: &GrpcContext) -> Result<Self, SvcError> {
        match context.api_version {
            APIVersion::V0 => {
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
            APIVersion::V1 => {
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
                Ok(Self {
                    context: context.clone(),
                    io_engine_v0: None,
                    io_engine_v1: Some(MayaClientV1 { host }),
                })
            }
        }
    }

    /// get the v0 api client
    pub(crate) fn client_v0(&self) -> Result<MayaClientV0, SvcError> {
        match self.io_engine_v0.clone() {
            Some(client) => Ok(client),
            None => Err(SvcError::InvalidArguments {}),
        }
    }

    /// get the v1 api client wrapper
    pub(crate) fn client_v1(&self) -> Result<MayaClientV1, SvcError> {
        match self.io_engine_v1.clone() {
            Some(client) => Ok(client),
            None => Err(SvcError::InvalidArguments {}),
        }
    }

    /// list blockdevices based on api versions
    pub(crate) async fn list_blockdevices(
        &self,
        request: &GetBlockDevices,
    ) -> Result<BlockDevices, SvcError> {
        match self.context.api_version {
            APIVersion::V0 => {
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
            APIVersion::V1 => {
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

    /// probe node for liveness based on api version in context
    pub(crate) async fn liveness_probe(&self, id: &NodeId) -> Result<Register, SvcError> {
        match self.context.api_version {
            APIVersion::V0 => {
                self.client_v0()?
                    .get_mayastor_info(rpc::io_engine::Null {})
                    .await
                    .map_err(|_| SvcError::NodeNotOnline {
                        node: id.to_owned(),
                    })?;

                // V0 GetMayastorInfo Liveness call doesn't return the registration info,
                // thus fill it from context and hard-code the version as V0
                Ok(Register {
                    id: self.context.node.clone(),
                    grpc_endpoint: self.context.endpoint.uri().to_string(),
                    api_versions: Some(vec![APIVersion::V0]),
                })
            }
            APIVersion::V1 => {
                // V1 liveness sends registration_info, which can be used to get the
                // actual state of dataplane
                let data = self
                    .client_v1()?
                    .host
                    .get_mayastor_info(())
                    .await
                    .map_err(|_| SvcError::NodeNotOnline {
                        node: id.to_owned(),
                    })?;

                let registration_info = match data.into_inner().registration_info {
                    Some(info) => info,
                    None => {
                        // The dataplane did not send anything in registration info, which should
                        // not happen, probably the dataplane is not ready?
                        return Err(SvcError::NotReady {
                            kind: ResourceKind::Node,
                            id: self.context.node.to_string(),
                        });
                    }
                };

                Ok(Register {
                    id: registration_info.id.into(),
                    grpc_endpoint: registration_info.grpc_endpoint,
                    api_versions: Some(
                        registration_info
                            .api_version
                            .into_iter()
                            .map(|v| {
                                // Get the list of supported api versions from dataplane
                                ApiVersion(
                                    rpc::v1::registration::ApiVersion::from_i32(v)
                                        .unwrap_or_default(),
                                )
                                .into()
                            })
                            .collect(),
                    ),
                })
            }
        }
    }

    /// list replicas based on api version in context
    pub(crate) async fn list_replicas(&self, id: &NodeId) -> Result<Vec<Replica>, SvcError> {
        match self.context.api_version {
            APIVersion::V0 => {
                let rpc_replicas = self.client_v0()?.list_replicas_v2(Null {}).await.context(
                    GrpcRequestError {
                        resource: ResourceKind::Replica,
                        request: "list_replicas",
                    },
                )?;

                let rpc_replicas = &rpc_replicas.get_ref().replicas;

                let replicas = rpc_replicas
                    .iter()
                    .filter_map(|p| match rpc_replica_to_agent(p, id) {
                        Ok(r) => Some(r),
                        Err(error) => {
                            tracing::error!(error=%error, "Could not convert rpc replica");
                            None
                        }
                    })
                    .collect();

                Ok(replicas)
            }
            APIVersion::V1 => {
                unimplemented!()
            }
        }
    }

    /// list pools based on api version in context
    pub(crate) async fn list_pools(&self, id: &NodeId) -> Result<Vec<PoolState>, SvcError> {
        match self.context.api_version {
            APIVersion::V0 => {
                let rpc_pools =
                    self.client_v0()?
                        .list_pools(Null {})
                        .await
                        .context(GrpcRequestError {
                            resource: ResourceKind::Pool,
                            request: "list_pools",
                        })?;

                let rpc_pools = &rpc_pools.get_ref().pools;

                let pools = rpc_pools.iter().map(|p| rpc_pool_to_agent(p, id)).collect();

                Ok(pools)
            }
            APIVersion::V1 => {
                unimplemented!()
            }
        }
    }

    /// list nexus based on api version in context
    pub(crate) async fn list_nexus(&self, id: &NodeId) -> Result<Vec<Nexus>, SvcError> {
        match self.context.api_version {
            APIVersion::V0 => {
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
                    .filter_map(|n| match rpc_nexus_v2_to_agent(n, id) {
                        Ok(n) => Some(n),
                        Err(error) => {
                            tracing::error!(error=%error, "Could not convert rpc nexus");
                            None
                        }
                    })
                    .collect();

                Ok(nexuses)
            }
            APIVersion::V1 => {
                unimplemented!()
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
    /// gRPC auto CRUD guard lock
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
    /// get the api version from grpc context
    pub(crate) fn api_version(&self) -> APIVersion {
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
