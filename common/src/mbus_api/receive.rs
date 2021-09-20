use super::*;
use opentelemetry::{
    global,
    global::BoxedTracer,
    trace::{SpanKind, StatusCode, TraceContextExt, Tracer},
    KeyValue,
};
use std::sync::Arc;

/// Type safe wrapper over a message bus message which decodes the raw
/// message into the actual payload `S` and allows only for a response type `R`.
///
/// # Example:
/// ```
/// let raw_msg = &subscriber.next().await?;
/// let msg: ReceivedMessageExt<RequestConfig, ReplyConfig> =
///             raw_msg.try_into()?;
///
/// msg.respond(ReplyConfig {}).await.unwrap();
/// // or we can also use the same fn to return an error
/// msg.respond(Err(Error::Message("failure".into()))).await.unwrap();
/// ```
pub struct ReceivedMessageExt<'a, S, R> {
    request: SendPayload<S>,
    bus_message: Arc<ReceivedRawMessage<'a>>,
    reply_type: PhantomData<R>,
}

/// Specialization of type safe wrapper over a message bus message which decodes
/// the raw message into the actual payload `S` and allows only for a response
/// type `R` which is determined based on `S: Message` as a `Message::Reply`
/// type.
///
/// # Example:
/// ```
/// let raw_msg = &subscriber.next().await?;
/// let msg: ReceivedMessage<RequestConfig> =
///             raw_msg.try_into()?;
///
/// msg.respond(ReplyConfig {}).await.unwrap();
/// // or we can also use the same fn to return an error
/// msg.respond(Err(Error::Message("failure".into()))).await.unwrap();
/// ```
pub type ReceivedMessage<'a, S> = ReceivedMessageExt<'a, S, <S as Message>::Reply>;

impl<'a, S, R> ReceivedMessageExt<'a, S, R>
where
    for<'de> S: Deserialize<'de> + 'a + Debug + Clone + Message,
    R: Serialize,
{
    /// Get a clone of the actual payload data which was received.
    pub fn inner(&self) -> S {
        self.request.data.clone()
    }
    /// Get the sender identifier
    pub fn sender(&self) -> SenderId {
        self.request.sender.clone()
    }

    /// Reply back to the sender with the `reply` payload wrapped by
    /// a Result-like type.
    /// May fail if serialization of the reply fails or if the
    /// message bus fails to respond.
    /// Can receive either `R`, `Err()` or `ReplyPayload<R>`.
    pub async fn reply<T: Serialize + Into<ReplyPayload<R>>>(&self, reply: T) -> BusResult<()> {
        self.bus_message.respond(&reply).await
    }

    /// Create a new received message object which wraps the send and
    /// receive types around a raw bus message.
    fn new(message: Arc<ReceivedRawMessage<'a>>) -> Result<Self, Error> {
        let bus_message = message.bus_msg;
        let request: SendPayload<S> =
            serde_json::from_slice(&bus_message.data).context(DeserializeSend {
                receiver: std::any::type_name::<S>(),
                payload: String::from_utf8(bus_message.data.clone()),
            })?;
        if request.id == request.data.id() {
            log::trace!(
                "Received message from '{}': {:?}",
                request.sender,
                request.data
            );
            Ok(Self {
                request,
                bus_message: message,
                reply_type: Default::default(),
            })
        } else {
            Err(Error::WrongMessageId {
                received: request.preamble.id,
                expected: request.data.id(),
            })
        }
    }
}

/// Message received over the message bus with a reply serialization wrapper
/// For type safety refer to `ReceivedMessage<'a,S,R>`.
#[derive(Clone)]
pub struct ReceivedRawMessage<'a> {
    bus_msg: &'a BusMessage,
    context: Option<opentelemetry::Context>,
}

impl std::fmt::Display for ReceivedRawMessage<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "channel: {}, msg_id: {:?}, reply_id: {:?}, data: {:?}",
            self.bus_msg.subject,
            self.id(),
            self.bus_msg.reply,
            std::str::from_utf8(&self.bus_msg.data)
        )
    }
}

impl<'a> ReceivedRawMessage<'a> {
    /// Get a copy of the actual payload data which was sent
    /// May fail if the raw data cannot be deserialized into `S`
    pub fn inner<S: Deserialize<'a> + Message>(&self) -> BusResult<S> {
        let request: SendPayload<S> =
            serde_json::from_slice(&self.bus_msg.data).context(DeserializeSend {
                receiver: std::any::type_name::<S>(),
                payload: String::from_utf8(self.bus_msg.data.clone()),
            })?;
        Ok(request.data)
    }

    /// Get the trace context and message identifier
    fn trace_context(&self) -> Option<(MessageId, TraceContext)> {
        let preamble: Preamble = serde_json::from_slice(&self.bus_msg.data).ok()?;
        let id = preamble.id;
        let context = preamble.trace_context;
        context.map(|ctx| (id, ctx))
    }

    /// Set the tracer context
    pub fn set_context(&mut self, tracer: Option<&BoxedTracer>) {
        if let Some((id, trace)) = self.trace_context() {
            let tracer: &BoxedTracer = match tracer {
                Some(tracer) => tracer,
                _ => return,
            };
            let parent_context =
                global::get_text_map_propagator(|propagator| propagator.extract(&trace));

            let mut builder = tracer.span_builder(id.to_string());
            builder.parent_context = parent_context;
            builder.span_kind = Some(SpanKind::Server);

            let attributes = vec![
                KeyValue::new(
                    opentelemetry_semantic_conventions::trace::MESSAGING_SYSTEM,
                    "NATS",
                ),
                KeyValue::new(
                    opentelemetry_semantic_conventions::trace::MESSAGING_DESTINATION,
                    self.bus_msg.subject.clone(),
                ),
                KeyValue::new(
                    opentelemetry_semantic_conventions::trace::MESSAGING_MESSAGE_ID,
                    id.to_string(),
                ),
            ];
            builder.attributes = Some(attributes);

            let span = tracer.build(builder);
            self.context = Some(opentelemetry::Context::current_with_span(span));
        }
    }

    /// Get a copy of the OpenTelemetry Context
    pub fn context(&self) -> opentelemetry::Context {
        self.context.clone().unwrap_or_default()
    }

    /// Get the identifier of this message.
    /// May fail if the raw data cannot be deserialized into the preamble.
    pub fn id(&self) -> BusResult<MessageId> {
        let preamble: Preamble =
            serde_json::from_slice(&self.bus_msg.data).context(DeserializeSend {
                receiver: std::any::type_name::<Preamble>(),
                payload: String::from_utf8(self.bus_msg.data.clone()),
            })?;
        Ok(preamble.id)
    }

    /// Channel where this message traversed
    pub fn channel(&self) -> Channel {
        self.bus_msg.subject.clone().parse().unwrap()
    }

    /// Respond back to the sender with the `reply` payload wrapped by
    /// a Result-like type.
    /// May fail if serialization of the reply fails or if the
    /// message bus fails to respond.
    /// Can receive either `Serialize`, `Err()` or `ReplyPayload<Serialize>`.
    pub async fn respond<T: Serialize, R: Serialize + Into<ReplyPayload<T>>>(
        &self,
        reply: R,
    ) -> BusResult<()> {
        let reply: ReplyPayload<T> = reply.into();
        let payload = serde_json::to_vec(&reply).context(SerializeReply {
            request: self.id()?,
        })?;

        match &self.context {
            None => {}
            Some(ctx) => {
                let span = ctx.span();
                match reply.0 {
                    Ok(_) => {
                        span.set_status(StatusCode::Ok, "".into());
                    }
                    Err(error) => {
                        span.set_status(
                            StatusCode::Error,
                            serde_json::to_string(&error).unwrap_or_default(),
                        );
                    }
                }
                span.end();
            }
        }
        self.bus_msg.respond(&payload).await.context(Reply {
            request: self.id()?,
        })
    }
}

impl<'a> std::convert::From<&'a BusMessage> for ReceivedRawMessage<'a> {
    fn from(value: &'a BusMessage) -> Self {
        Self {
            bus_msg: value,
            context: None,
        }
    }
}

impl<'a, S, R> std::convert::TryFrom<Arc<ReceivedRawMessage<'a>>> for ReceivedMessageExt<'a, S, R>
where
    for<'de> S: Deserialize<'de> + 'a + Debug + Clone + Message,
    R: Serialize,
{
    type Error = Error;

    fn try_from(value: Arc<ReceivedRawMessage<'a>>) -> Result<Self, Self::Error> {
        ReceivedMessageExt::<S, R>::new(value)
    }
}

impl<T> From<T> for ReplyPayload<T> {
    fn from(val: T) -> Self {
        ReplyPayload(Ok(val))
    }
}

impl<T> From<Result<T, ReplyError>> for ReplyPayload<T> {
    fn from(val: Result<T, ReplyError>) -> Self {
        ReplyPayload(val)
    }
}
