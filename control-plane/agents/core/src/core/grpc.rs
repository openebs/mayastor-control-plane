use crate::node::service::NodeCommsTimeout;
use common::errors::{GrpcConnect, GrpcConnectUri, SvcError};
use common_lib::{mbus_api::MessageIdTimeout, types::v0::message_bus::NodeId};
use rpc::mayastor::mayastor_client::MayastorClient;
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
}

impl GrpcContext {
    pub(crate) fn new<T: MessageIdTimeout>(
        lock: Arc<tokio::sync::Mutex<()>>,
        node: &NodeId,
        endpoint: &str,
        comms_timeouts: &NodeCommsTimeout,
        _request: Option<T>,
    ) -> Result<Self, SvcError> {
        let uri = format!("http://{}", endpoint);
        let uri = http::uri::Uri::from_str(&uri).context(GrpcConnectUri {
            node_id: node.to_string(),
            uri: uri.clone(),
        })?;

        // let timeout = request
        //     .map(|r| r.timeout(comms_timeouts.request(), &bus()))
        //     .unwrap_or_else(|| comms_timeouts.request());

        let endpoint = tonic::transport::Endpoint::from(uri)
            .connect_timeout(comms_timeouts.connect() + Duration::from_millis(500))
            .timeout(comms_timeouts.request());

        Ok(Self {
            node: node.clone(),
            lock,
            endpoint,
            comms_timeouts: comms_timeouts.clone(),
        })
    }
    /// Override the timeout config in the context for the given request
    fn override_timeout<R: MessageIdTimeout>(&mut self, _request: Option<R>) {
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

/// Wrapper over all gRPC Clients types
#[derive(Clone)]
pub(crate) struct GrpcClient {
    #[allow(dead_code)]
    context: GrpcContext,
    /// gRPC Mayastor Client
    pub(crate) mayastor: MayaClient,
}
pub(crate) type MayaClient = MayastorClient<Channel>;
impl GrpcClient {
    pub(crate) async fn new(context: &GrpcContext) -> Result<Self, SvcError> {
        let client = match tokio::time::timeout(
            context.comms_timeouts.connect(),
            MayaClient::connect(context.endpoint.clone()),
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
            mayastor: client,
        })
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
    pub(crate) async fn reconnect<R: MessageIdTimeout>(self, request: R) -> Result<Self, SvcError> {
        let mut context = self.context.clone();
        context.override_timeout(Some(request));

        let client = GrpcClient::new(&context).await?;

        Ok(Self {
            _lock: self._lock,
            client,
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
