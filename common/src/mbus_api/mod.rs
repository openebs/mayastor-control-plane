#![warn(missing_docs)]
//! All the different messages which can be sent/received to/from the control
//! plane services and io-engine
//! We could split these out further into categories when they start to grow

mod mbus_nats;
/// send messages traits
pub mod send;
/// Version 0 of the messages
pub mod v0;

pub use mbus_nats::{message_bus_init, message_bus_init_options, NatsMessageBus};
pub use send::*;

use crate::types::{
    v0::message_bus::{MessageIdVs, VERSION},
    Channel,
};
use async_trait::async_trait;
use dyn_clonable::clonable;
use opentelemetry::propagation::{Extractor, Injector};
use serde::{de::StdError, Deserialize, Serialize};

use std::{collections::HashMap, fmt::Debug, num::TryFromIntError, str::FromStr, time::Duration};
use strum_macros::{AsRefStr, ToString};
use tokio::task::JoinError;
use tonic::{Code, Status};

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

/// Sender identification (eg which io-engine instance sent the message)
pub type SenderId = String;

/// This trait defines all Bus Messages which must:
/// 1 - be uniquely identifiable via MessageId
/// 2 - have a default Channel on which they are sent/received
#[async_trait]
pub trait Message {
    /// identification of this object according to the `MessageId`
    fn id(&self) -> MessageId;
    /// default channel where this object is sent to
    fn channel(&self) -> Channel;
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
    /// Spec
    Spec,
    /// State
    State,
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

/// Error sending/receiving
/// Common error type for send/receive
pub type BusError = ReplyError;

impl From<tonic::Status> for ReplyError {
    fn from(status: Status) -> Self {
        Self::tonic_reply_error(
            status.code().into(),
            status.message().to_string(),
            status.full_string(),
        )
    }
}

impl From<ReplyError> for tonic::Status {
    fn from(err: ReplyError) -> Self {
        tonic::Status::new(Code::Aborted, err.full_string())
    }
}

impl From<tonic::transport::Error> for ReplyError {
    fn from(e: tonic::transport::Error) -> Self {
        Self::tonic_reply_error(ReplyErrorKind::Aborted, e.to_string(), e.full_string())
    }
}

/// Error type for invalid integer type conversion
impl From<TryFromIntError> for ReplyError {
    fn from(e: TryFromIntError) -> Self {
        Self::invalid_argument(ResourceKind::Unknown, "", e.to_string())
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
    pub fn tonic_reply_error(kind: ReplyErrorKind, source: String, extra: String) -> Self {
        Self {
            kind,
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
    /// for errors that can occur when serializing or deserializing JSON data
    pub fn serde_error(
        resource: ResourceKind,
        error_kind: ReplyErrorKind,
        error: serde_json::Error,
    ) -> Self {
        Self {
            kind: error_kind,
            resource,
            source: error.to_string(),
            extra: "".to_string(),
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
#[derive(Serialize, Deserialize, Debug, Clone, strum_macros::AsRefStr, Eq, PartialEq)]
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

impl From<tonic::Code> for ReplyErrorKind {
    fn from(code: tonic::Code) -> Self {
        match code {
            Code::InvalidArgument => Self::InvalidArgument,
            Code::DeadlineExceeded => Self::DeadlineExceeded,
            Code::NotFound => Self::NotFound,
            Code::AlreadyExists => Self::AlreadyExists,
            Code::PermissionDenied => Self::PermissionDenied,
            Code::ResourceExhausted => Self::ResourceExhausted,
            Code::FailedPrecondition => Self::FailedPrecondition,
            Code::Aborted => Self::Aborted,
            Code::OutOfRange => Self::OutOfRange,
            Code::Unimplemented => Self::Unimplemented,
            Code::Internal => Self::Internal,
            Code::Unavailable => Self::Unavailable,
            Code::DataLoss => Self::FailedPersist,
            Code::Unauthenticated => Self::Unauthenticated,
            _ => Self::Aborted,
        }
    }
}

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
    /// minimum timeout for a replica operation.
    pub fn replica(&self) -> Duration {
        self.replica
    }
    /// minimum timeout for a nexus operation.
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
    /// Get the tcp read timeout.
    pub fn tcp_read_timeout(&self) -> Duration {
        self.tcp_read_timeout
    }
    /// Get the base timeout.
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
