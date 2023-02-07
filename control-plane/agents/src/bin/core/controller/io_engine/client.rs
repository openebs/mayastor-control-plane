use crate::{controller::io_engine::NodeApi, node::service::NodeCommsTimeout};
use agents::errors::{GrpcConnectUri, SvcError};
use common_lib::{
    transport_api::MessageId,
    types::v0::transport::{ApiVersion, NodeId},
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
    pub(crate) fn override_timeout(&mut self, request: Option<MessageId>) {
        self.endpoint = self
            .endpoint
            .clone()
            .connect_timeout(self.comms_timeouts.connect() + Duration::from_millis(500))
            .timeout(match request {
                None => self.comms_timeouts.request(),
                Some(request) => timeout_grpc(request, self.comms_timeouts.opts().clone()),
            });
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
    /// Get the tonic endpoint.
    pub(crate) fn tonic_endpoint(&self) -> tonic::transport::Endpoint {
        self.endpoint.clone()
    }
    /// Get the node identifier.
    pub(crate) fn node(&self) -> &NodeId {
        &self.node
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
                ApiVersion::V0 => Box::new(super::v0::RpcClient::new(context).await?),
                ApiVersion::V1 => Box::new(super::v1::RpcClient::new(context).await?),
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
