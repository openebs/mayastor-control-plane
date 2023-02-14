use crate::tracing::OpenTelClient;
use std::time::Duration;
pub use stor_port::transport_api::TimeoutOptions;
use stor_port::{
    transport_api::{ClientId, MessageId},
    types::v0::transport::MessageIdVs,
};
use tonic::{
    transport::{Channel, Uri},
    IntoRequest,
};
use utils::tracing_telemetry::trace::FutureExt;

use utils::DEFAULT_REQ_TIMEOUT;

/// Request specific minimum timeouts
/// zeroing replicas on create/destroy takes some time (observed up to 7seconds)
/// nexus creation by itself can take up to 4 seconds... it can take even longer if etcd is not up
#[derive(Debug, Clone)]
pub struct RequestMinTimeout {
    replica: Duration,
    nexus: Duration,
}

impl Default for RequestMinTimeout {
    fn default() -> Self {
        Self {
            replica: Duration::from_secs(10),
            nexus: Duration::from_secs(30),
        }
    }
}
impl RequestMinTimeout {
    /// minimum timeout for a replica operation
    pub fn replica(&self) -> Duration {
        self.replica
    }
    /// minimum timeout for a nexus operation
    pub fn nexus(&self) -> Duration {
        self.nexus
    }
}

/// get the default timeout for each type of request if a timeout is not specified.
/// timeouts vary with different types of requests
pub fn timeout_grpc(op_id: MessageId, timeout_opts: TimeoutOptions) -> Duration {
    let base = timeout_opts.base_timeout();
    if let Some(min_timeouts) = timeout_opts.request_min_timeout() {
        let op_timeout = match op_id {
            MessageId::v0(op_id) => match op_id {
                MessageIdVs::CreateVolume => min_timeouts.replica() * 3 + min_timeouts.nexus(),
                MessageIdVs::DestroyVolume => min_timeouts.replica() * 3 + min_timeouts.nexus(),
                MessageIdVs::PublishVolume => min_timeouts.nexus(),
                MessageIdVs::UnpublishVolume => min_timeouts.nexus(),
                MessageIdVs::RepublishVolume => min_timeouts.nexus() * 2,

                MessageIdVs::CreateNexus => min_timeouts.nexus(),
                MessageIdVs::DestroyNexus => min_timeouts.nexus(),
                MessageIdVs::ShutdownNexus => min_timeouts.nexus_shutdown(),

                MessageIdVs::CreateReplica => min_timeouts.replica(),
                MessageIdVs::DestroyReplica => min_timeouts.replica(),

                MessageIdVs::CreatePool => min_timeouts.pool(),
                MessageIdVs::DestroyPool => min_timeouts.pool(),
                _ => base,
            },
        };
        let timeout = Duration::max(base, op_timeout).min(Duration::from_secs(59));
        match timeout_opts.client() {
            // the rest server should have some slack to allow for the CoreAgent to timeout first.
            ClientId::RestServer => timeout + Duration::from_secs(1),
            ClientId::CoreAgent => timeout,
            _ => timeout,
        }
    } else {
        base
    }
}

/// context to be sent along with each request encapsulating the extra add ons that changes the
/// behaviour of each request.
#[derive(Clone, Debug)]
pub struct Context {
    timeout_opts: Option<TimeoutOptions>,
}

impl Context {
    /// Generate a new context with the provided `TimeoutOptions`.
    pub fn new(timeout_opts: impl Into<Option<TimeoutOptions>>) -> Self {
        Self {
            timeout_opts: timeout_opts.into(),
        }
    }

    /// Get the optional `TimeoutOptions`.
    pub fn timeout_opts(&self) -> Option<TimeoutOptions> {
        self.timeout_opts.clone()
    }

    /// Get the base timeout if specified, or `DEFAULT_REQ_TIMEOUT`.
    pub fn base_timeout(&self) -> Duration {
        self.timeout_opts
            .as_ref()
            .map(|o| o.base_timeout())
            .unwrap_or_else(|| humantime::parse_duration(DEFAULT_REQ_TIMEOUT).unwrap())
    }

    /// Get the http2 keep alive interval.
    pub fn keep_alive_interval(&self) -> Duration {
        self.timeout_opts
            .clone()
            .unwrap_or_default()
            .keep_alive_interval()
    }
    /// Get the http2 keep alive timeout.
    pub fn keep_alive_timeout(&self) -> Duration {
        self.timeout_opts
            .clone()
            .unwrap_or_default()
            .keep_alive_timeout()
    }

    /// Create a new endpoint that connects to the provided Uri.
    /// This endpoint has default connect and request timeouts.
    fn endpoint(&self, uri: Uri) -> tonic::transport::Endpoint {
        let timeout = self.base_timeout();
        tonic::transport::Endpoint::from(uri)
            // we use the same timeout for the connection so we can pass the existing nats tests
            // todo: use a shorter connect timeout
            .connect_timeout(timeout)
            // todo: Channel will pick the shorter timeout, rather than override the default
            // timeout with a per-request timeout. For now set this default timeout high, but
            // we probably want to use our own timeout logic instead.
            .timeout(std::time::Duration::from_secs(60))
            .http2_keep_alive_interval(self.keep_alive_interval())
            .keep_alive_timeout(self.keep_alive_timeout())
            .concurrency_limit(utils::DEFAULT_GRPC_CLIENT_CONCURRENCY)
    }

    pub fn spawn<T>(future: T) -> tokio::task::JoinHandle<T::Output>
    where
        T: std::future::Future + Send + 'static,
        T::Output: Send + 'static,
    {
        let context = utils::tracing_telemetry::Context::current();
        tokio::spawn(future.with_context(context))
    }
}

/// Tonic Channel with added gRPC tracing
pub(crate) type TracedChannel = crate::tracing::OpenTelClientService<Channel>;

/// Generic RPC Client.
#[derive(Clone)]
pub struct Client<C: Clone> {
    context: Context,
    client: C,
}

impl<C: Clone> Client<C> {
    /// Creates a generic RPC client based on the provided arguments.
    /// options: Timeout options which are used for connection and request timeouts.
    /// make_client: Creates a client of the appropriate type.
    pub(crate) async fn new<O, M>(uri: Uri, options: O, make_client: M) -> Self
    where
        O: Into<Option<TimeoutOptions>>,
        M: FnOnce(TracedChannel) -> C,
    {
        let context = Context::new(options);
        let endpoint = context.endpoint(uri);
        let channel = endpoint.connect_lazy();

        let channel = tower::ServiceBuilder::new()
            .layer(OpenTelClient::new())
            .service(channel);
        let client = make_client(channel);
        Self { context, client }
    }

    /// Prepares a new `tonic::Request<T>` for the given request `R: Into<T>`.
    /// If `context` is specified the timeout of the request will be set to the base_timeout of
    /// context. Otherwise, `op_id` will be used to select an appropriate timeout.
    pub(crate) fn request<T, R: Into<T>, M: Into<MessageId>>(
        &self,
        request: R,
        context: Option<Context>,
        op_id: M,
    ) -> tonic::Request<T> {
        let timeout_opts = self.context.timeout_opts.clone().unwrap_or_default();
        let timeout = context
            .map(|c| c.base_timeout())
            .unwrap_or_else(|| timeout_grpc(op_id.into(), timeout_opts));
        let mut request = request.into().into_request();
        request.set_timeout(timeout);
        request
    }
    /// Returns a new client.
    pub(crate) fn client(&self) -> C {
        self.client.clone()
    }
}
