#![warn(missing_docs)]
//! All the different messages which can be sent/received to/from the control
//! plane services and mayastor
//! We could split these out further into categories when they start to grow

mod mbus_nats;
/// Message bus client interface
pub mod message_bus;
/// received message traits
pub mod receive;
/// send messages traits
pub mod send;
/// Version 0 of the messages
pub mod v0;

use crate::types::{
    v0::message_bus::{MessageIdVs, VERSION},
    Channel,
};
use async_trait::async_trait;
use dyn_clonable::clonable;
pub use mbus_nats::{bus, message_bus_init, message_bus_init_options, NatsMessageBus};
use opentelemetry::propagation::{Extractor, Injector};
pub use receive::*;
pub use send::*;
use serde::{de::StdError, Deserialize, Serialize};
use snafu::{ResultExt, Snafu};
use std::{
    collections::HashMap, fmt::Debug, io, marker::PhantomData, ops::Deref, str::FromStr,
    time::Duration,
};
use strum_macros::{AsRefStr, ToString};
use tokio::task::JoinError;
use tonic::{Code, Status};

/// Result wrapper for send/receive
pub type BusResult<T> = Result<T, Error>;
/// Common error type for send/receive
#[derive(Debug, Snafu, strum_macros::AsRefStr)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Message with wrong message id received. Received '{}' but Expected '{}'", received.to_string(), expected.to_string()))]
    WrongMessageId {
        received: MessageId,
        expected: MessageId,
    },
    #[snafu(display("Failed to serialize the publish payload on channel '{}'", channel.to_string()))]
    SerializeSend {
        source: serde_json::Error,
        channel: Channel,
    },
    #[snafu(display(
        "Failed to deserialize the publish payload: '{:?}' into type '{}'",
        payload,
        receiver
    ))]
    DeserializeSend {
        payload: Result<String, std::string::FromUtf8Error>,
        receiver: String,
        source: serde_json::Error,
    },
    #[snafu(display("Failed to serialize the reply payload for request message id '{}'", request.to_string()))]
    SerializeReply {
        request: MessageId,
        source: serde_json::Error,
    },
    #[snafu(display(
        "Failed to deserialize the reply payload '{:?}' for message: '{:?}'",
        reply,
        request
    ))]
    DeserializeReceive {
        request: Result<String, serde_json::Error>,
        reply: Result<String, std::string::FromUtf8Error>,
        source: serde_json::Error,
    },
    #[snafu(display(
        "Failed to send message '{:?}' through the message bus on channel '{}'",
        payload,
        channel
    ))]
    Publish {
        channel: String,
        payload: Result<String, std::string::FromUtf8Error>,
        source: io::Error,
    },
    #[snafu(display(
        "Timed out waiting for a reply to message '{:?}' on channel '{:?}' with options '{:?}'.",
        payload,
        channel,
        options
    ))]
    RequestTimeout {
        channel: String,
        payload: Result<String, std::string::FromUtf8Error>,
        options: TimeoutOptions,
    },
    #[snafu(display(
        "Failed to reply back to message id '{}' through the message bus",
        request.to_string()
    ))]
    Reply {
        request: MessageId,
        source: io::Error,
    },
    #[snafu(display("Failed to flush the message bus"))]
    Flush { source: io::Error },
    #[snafu(display("Failed to subscribe to channel '{}' on the message bus", channel))]
    Subscribe { channel: String, source: io::Error },
    #[snafu(display("Reply message came back with an error"))]
    ReplyWithError { source: ReplyError },
}

/// Report error chain
pub trait ErrorChain {
    /// full error chain as a string separated by ':'
    fn full_string(&self) -> String;
}

impl<T> ErrorChain for T
where
    T: std::error::Error,
{
    /// loops through the error chain and formats into a single string
    /// containing all the lower level errors
    fn full_string(&self) -> String {
        let mut msg = format!("{}", self);
        let mut opt_source = self.source();
        while let Some(source) = opt_source {
            msg = format!("{}: {}", msg, source);
            opt_source = source.source();
        }
        msg
    }
}

/// Message id which uniquely identifies every type of unsolicited message
/// The solicited (replies) message do not currently carry an id as they
/// are sent to a specific requested channel
#[derive(Debug, PartialEq, Clone)]
#[allow(non_camel_case_types)]
pub enum MessageId {
    /// Version 0
    v0(MessageIdVs),
}

/// Exposes specific timeouts for different MessageId's
pub trait MessageIdTimeout: Send {
    /// Get the default `TimeoutOptions` for this message
    fn timeout_opts(&self, opts: TimeoutOptions, bus: &DynBus) -> TimeoutOptions;
    /// Get the default timeout `Duration` for this message
    fn timeout(&self, timeout: Duration, bus: &DynBus) -> Duration;
}

impl MessageIdTimeout for MessageId {
    fn timeout_opts(&self, opts: TimeoutOptions, bus: &DynBus) -> TimeoutOptions {
        match self {
            MessageId::v0(id) => id.timeout_opts(opts, bus),
        }
    }
    fn timeout(&self, timeout: Duration, bus: &DynBus) -> Duration {
        match self {
            MessageId::v0(id) => id.timeout(timeout, bus),
        }
    }
}

impl Serialize for MessageId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}
impl<'de> Deserialize<'de> for MessageId {
    fn deserialize<D>(deserializer: D) -> Result<MessageId, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        match string.parse() {
            Ok(id) => Ok(id),
            Err(error) => {
                let error = format!("Failed to parse into MessageId, error: {}", error);
                Err(serde::de::Error::custom(error))
            }
        }
    }
}

impl FromStr for MessageId {
    type Err = strum::ParseError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        match source.split('/').next() {
            Some(VERSION) => {
                let id: MessageIdVs = source[VERSION.len() + 1 ..].parse()?;
                Ok(Self::v0(id))
            }
            _ => Err(strum::ParseError::VariantNotFound),
        }
    }
}
impl ToString for MessageId {
    fn to_string(&self) -> String {
        match self {
            Self::v0(id) => format!("{}/{}", VERSION, id.to_string()),
        }
    }
}

/// Sender identification (eg which mayastor instance sent the message)
pub type SenderId = String;

/// This trait defines all Bus Messages which must:
/// 1 - be uniquely identifiable via MessageId
/// 2 - have a default Channel on which they are sent/received
#[async_trait]
pub trait Message {
    /// type which is sent back in response to a request
    type Reply;

    /// identification of this object according to the `MessageId`
    fn id(&self) -> MessageId;
    /// default channel where this object is sent to
    fn channel(&self) -> Channel;

    /// publish a message with no delivery guarantees
    async fn publish(&self) -> BusResult<()>;
    /// publish a message with a request for a `Self::Reply` reply
    async fn request(&self) -> BusResult<Self::Reply>;
    /// publish a message on the given channel with a request for a
    /// `Self::Reply` reply
    async fn request_on<C: Into<Channel> + Send>(&self, channel: C) -> BusResult<Self::Reply>;
    /// publish a message with a request for a `Self::Reply` reply
    /// and non default timeout options
    async fn request_ext(&self, options: TimeoutOptions) -> BusResult<Self::Reply>;
    /// publish a message with a request for a `Self::Reply` reply
    /// and non default timeout options on the given channel
    async fn request_on_ext<C: Into<Channel> + Send>(
        &self,
        channel: C,
        options: TimeoutOptions,
    ) -> BusResult<Self::Reply>;
    /// publish a message with a request for a `Self::Reply` reply
    /// and non default timeout options on the given channel and bus
    async fn request_on_bus<C: Into<Channel> + Send>(
        &self,
        channel: C,
        bus: DynBus,
    ) -> BusResult<Self::Reply>;
}

/// The preamble is used to peek into messages so allowing for them to be routed
/// by their identifier
#[derive(Serialize, Deserialize, Debug)]
struct Preamble {
    id: MessageId,
    sender: SenderId,
    trace_context: Option<TraceContext>,
}

/// Opentelemetry trace context
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct TraceContext(HashMap<String, String>);
impl TraceContext {
    /// Get an empty `Self`
    pub fn new() -> Self {
        Self(HashMap::new())
    }
}
impl Injector for TraceContext {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}
impl Extractor for TraceContext {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|header| header.as_str()).collect()
    }
}

/// Unsolicited (send) messages carry the message identifier, the sender
/// identifier and finally the message payload itself
#[derive(Serialize, Deserialize)]
struct SendPayload<T> {
    #[serde(flatten)]
    preamble: Preamble,
    data: T,
}

impl<T> Deref for SendPayload<T> {
    type Target = Preamble;
    fn deref(&self) -> &Self::Target {
        &self.preamble
    }
}

/// All the different variants of Resources
#[derive(Serialize, Deserialize, Debug, Clone, AsRefStr, ToString)]
pub enum ResourceKind {
    /// Unknown or unspecified resource
    Unknown,
    /// Node resource
    Node,
    /// Pool resource
    Pool,
    /// Replica resource
    Replica,
    /// Replica state
    ReplicaState,
    /// Replica spec
    ReplicaSpec,
    /// Nexus resource
    Nexus,
    /// Child resource
    Child,
    /// Volume resource
    Volume,
    /// Json Grpc methods
    JsonGrpc,
    /// Block devices
    Block,
    /// Watch
    Watch,
}

/// Error type which is returned over the bus
/// for any other operation
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReplyError {
    /// error kind
    pub kind: ReplyErrorKind,
    /// resource kind
    pub resource: ResourceKind,
    /// last source of this error
    pub source: String,
    /// extra information
    pub extra: String,
}

impl From<tonic::Status> for ReplyError {
    fn from(status: Status) -> Self {
        Self::tonic_reply_error(status.to_string(), status.full_string())
    }
}

impl From<ReplyError> for tonic::Status {
    fn from(err: ReplyError) -> Self {
        tonic::Status::new(Code::Aborted, err.full_string())
    }
}

impl From<tonic::transport::Error> for ReplyError {
    fn from(e: tonic::transport::Error) -> Self {
        Self::tonic_reply_error(e.to_string(), e.full_string())
    }
}

impl From<JoinError> for ReplyError {
    fn from(error: JoinError) -> Self {
        Self::aborted_error(error)
    }
}

impl StdError for ReplyError {}
impl ReplyError {
    /// extend error with source
    /// useful when another error wraps around a `ReplyError` and we want to
    /// convert back to `ReplyError` so we can send it over the wire
    pub fn extend(&mut self, source: &str, extra: &str) {
        self.source = format!("{}::{}", source, self.source);
        self.extra = format!("{}::{}", extra, self.extra);
    }
    /// useful when the grpc server is dropped due to panic
    pub fn aborted_error(error: JoinError) -> Self {
        Self {
            kind: ReplyErrorKind::Aborted,
            resource: ResourceKind::Unknown,
            source: error.to_string(),
            extra: "Failed to wait for thread".to_string(),
        }
    }
    /// useful when the grpc server is dropped due to panic
    pub fn tonic_reply_error(source: String, extra: String) -> Self {
        Self {
            kind: ReplyErrorKind::Aborted,
            resource: ResourceKind::Unknown,
            source,
            extra,
        }
    }
    /// used only for testing, not used in code
    pub fn invalid_reply_error(msg: String) -> Self {
        Self {
            kind: ReplyErrorKind::Aborted,
            resource: ResourceKind::Unknown,
            source: "Test Library".to_string(),
            extra: msg,
        }
    }
    /// used when we get an empty response from the grpc server
    pub fn invalid_response(resource: ResourceKind) -> Self {
        Self {
            kind: ReplyErrorKind::Aborted,
            resource,
            source: "Empty response reply received from grpc server".to_string(),
            extra: "".to_string(),
        }
    }
    /// used when we get an invalid argument
    pub fn invalid_argument(resource: ResourceKind, arg_name: &str, error: String) -> Self {
        Self {
            kind: ReplyErrorKind::InvalidArgument,
            resource,
            source: error,
            extra: format!("Invalid {} was provided", arg_name),
        }
    }
    /// used when we encounter a missing argument
    pub fn missing_argument(resource: ResourceKind, arg_name: &str) -> Self {
        Self {
            kind: ReplyErrorKind::InvalidArgument,
            resource,
            source: arg_name.to_string(),
            extra: format!("Argument {} was not provided", arg_name),
        }
    }
}

impl std::fmt::Display for ReplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "'{}' Error on '{}' resources, from Error '{}', extra: '{}'",
            self.kind.as_ref(),
            self.resource.as_ref(),
            self.source,
            self.extra
        )
    }
}

/// All the different variants of `ReplyError`
#[derive(Serialize, Deserialize, Debug, Clone, strum_macros::AsRefStr)]
#[allow(missing_docs)]
pub enum ReplyErrorKind {
    WithMessage,
    DeserializeReq,
    Internal,
    Timeout,
    InvalidArgument,
    DeadlineExceeded,
    NotFound,
    AlreadyExists,
    PermissionDenied,
    ResourceExhausted,
    FailedPrecondition,
    Aborted,
    OutOfRange,
    Unimplemented,
    Unavailable,
    Unauthenticated,
    Unauthorized,
    Conflict,
    FailedPersist,
    NotShared,
    AlreadyShared,
    NotPublished,
    AlreadyPublished,
    Deleting,
    ReplicaCountAchieved,
    ReplicaChangeCount,
    ReplicaIncrease,
    ReplicaCreateNumber,
    VolumeNoReplicas,
    InUse,
}

impl From<Error> for ReplyError {
    fn from(error: Error) -> Self {
        #[allow(deprecated)]
        let source_name = error.description().to_string();
        match error {
            Error::RequestTimeout { .. } => Self {
                kind: ReplyErrorKind::Timeout,
                resource: ResourceKind::Unknown,
                source: source_name,
                extra: error.to_string(),
            },
            Error::ReplyWithError { source } => source,
            _ => Self {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Unknown,
                extra: error.to_string(),
                source: source_name,
            },
        }
    }
}

/// Payload returned to the sender
/// Includes an error as the operations may be fallible
#[derive(Serialize, Deserialize, Debug)]
pub struct ReplyPayload<T>(pub Result<T, ReplyError>);

// todo: implement thin wrappers on these
/// MessageBus raw Message
pub type BusMessage = nats::asynk::Message;
/// MessageBus subscription
pub type BusSubscription = nats::asynk::Subscription;
/// MessageBus configuration options
pub type BusOptions = nats::asynk::Options;
/// Save on typing
pub type DynBus = Box<dyn Bus>;

/// Timeout for receiving a reply to a request message
/// Max number of retries until it gives up
#[derive(Clone, Debug)]
pub struct TimeoutOptions {
    /// initial request message timeout
    pub(crate) timeout: std::time::Duration,
    /// request message incremental timeout step
    pub(crate) timeout_step: std::time::Duration,
    /// max number of retries following the initial attempt's timeout
    pub(crate) max_retries: Option<u32>,
    /// Server tcp read timeout when no messages are received.
    /// Shen this timeout is triggered we attempt to send a Ping to the server. If a Pong is not
    /// received within the same timeout the nats client disconnects from the server.
    tcp_read_timeout: std::time::Duration,

    /// Request specific minimum timeouts
    request_timeout: Option<RequestMinTimeout>,

    /// Http2 keep alive interval.
    keep_alive_interval: std::time::Duration,
    /// Http2 keep alive timeout.
    keep_alive_timeout: std::time::Duration,
}

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

impl TimeoutOptions {
    /// Default timeout waiting for a reply.
    pub(crate) fn default_timeout() -> Duration {
        Duration::from_secs(6)
    }
    /// Default time between retries when a timeout is hit.
    pub(crate) fn default_timeout_step() -> Duration {
        Duration::from_secs(1)
    }
    /// Default max number of retries until the request is given up on.
    pub(crate) fn default_max_retries() -> u32 {
        6
    }
    /// Default `RequestMinTimeout` which specified timeouts for specific operations.
    pub(crate) fn default_request_timeouts() -> Option<RequestMinTimeout> {
        Some(RequestMinTimeout::default())
    }
    /// Default Server tcp read timeout when no messages are received.
    pub(crate) fn default_tcp_read_timeout() -> Duration {
        Duration::from_secs(6)
    }
    /// Get the tcp read timeout
    pub(crate) fn tcp_read_timeout(&self) -> Duration {
        self.tcp_read_timeout
    }
    /// Get the base timeout
    pub fn base_timeout(&self) -> Duration {
        self.timeout
    }
    /// Default http2 Keep Alive interval.
    pub(crate) fn default_keep_alive_interval() -> std::time::Duration {
        Duration::from_secs(10)
    }
    /// Default http2 Keep Alive timeout.
    pub(crate) fn default_keep_alive_timeout() -> std::time::Duration {
        Duration::from_secs(20)
    }
}

impl Default for TimeoutOptions {
    fn default() -> Self {
        Self {
            timeout: Self::default_timeout(),
            timeout_step: Self::default_timeout_step(),
            max_retries: Some(Self::default_max_retries()),
            tcp_read_timeout: Self::default_tcp_read_timeout(),
            request_timeout: Self::default_request_timeouts(),
            keep_alive_timeout: Self::default_keep_alive_timeout(),
            keep_alive_interval: Self::default_keep_alive_interval(),
        }
    }
}

impl TimeoutOptions {
    /// New options with default values
    #[must_use]
    pub fn new() -> Self {
        Default::default()
    }

    /// New options with default values but with no retries
    #[must_use]
    pub fn new_no_retries() -> Self {
        Self::new().with_max_retries(0)
    }

    /// Timeout after which we'll either fail the request or start retrying
    /// if max_retries is greater than 0 or None
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Timeout multiplied at each iteration
    #[must_use]
    pub fn with_timeout_backoff(mut self, timeout: Duration) -> Self {
        self.timeout_step = timeout;
        self
    }

    /// Specify a max number of retries before giving up
    /// None for unlimited retries
    #[must_use]
    pub fn with_max_retries<M: Into<Option<u32>>>(mut self, max_retries: M) -> Self {
        self.max_retries = max_retries.into();
        self
    }

    /// Minimum timeouts for specific requests
    #[must_use]
    pub fn with_req_timeout(mut self, timeout: impl Into<Option<RequestMinTimeout>>) -> Self {
        self.request_timeout = timeout.into();
        self
    }

    /// Get the minimum request timeouts
    pub fn request_timeout(&self) -> Option<&RequestMinTimeout> {
        self.request_timeout.as_ref()
    }

    /// Get the http2 Keep Alive interval.
    pub fn keep_alive_interval(&self) -> Duration {
        self.keep_alive_interval
    }
    /// Get the http2 Keep Alive timeout.
    pub fn keep_alive_timeout(&self) -> Duration {
        self.keep_alive_timeout
    }

    /// get the max retries
    pub fn max_retries(&self) -> Option<u32> {
        self.max_retries
    }
}

/// Messaging Bus trait with "generic" publish and request/reply semantics
#[async_trait]
#[clonable]
pub trait Bus: Clone + Send + Sync {
    /// publish a message - not guaranteed to be sent or received (fire and
    /// forget)
    async fn publish(&self, channel: Channel, message: &[u8]) -> BusResult<()>;
    /// Send a message and wait for it to be received by the target component
    async fn send(&self, channel: Channel, message: &[u8]) -> BusResult<()>;
    /// Send a message and request a reply from the target component
    async fn request(
        &self,
        channel: Channel,
        message: &[u8],
        options: Option<TimeoutOptions>,
    ) -> BusResult<BusMessage>;
    /// Flush queued messages to the server
    async fn flush(&self) -> BusResult<()>;
    /// Create a subscription on the given channel which can be
    /// polled for messages until it is either explicitly closed or
    /// when the bus is closed
    async fn subscribe(&self, channel: Channel) -> BusResult<BusSubscription>;
    /// Get this client's name
    fn client_name(&self) -> &BusClient;
    /// Get the configured timeout options
    fn timeout_opts(&self) -> &TimeoutOptions;
}

/// Identifies which client is using the message bus
#[derive(Debug, Clone)]
pub enum BusClient {
    /// The Rest Server
    RestServer,
    /// The Core Agent
    CoreAgent,
    /// The JsonGrpc Agent
    JsonGrpcAgent,
    /// Not Specified
    Unnamed,
}
