#![warn(missing_docs)]
//! Control Plane Agents library with emphasis on the message bus interaction
//! including errors.
//!
//! It's meant to facilitate the creation of agents with a helper builder to
//! subscribe handlers for different message identifiers.

use std::{
    collections::HashMap,
    convert::{Into, TryInto},
    sync::Arc,
};

use async_trait::async_trait;
use dyn_clonable::clonable;
use futures::{future::join_all, Future};
use snafu::{OptionExt, ResultExt, Snafu};
use state::Container;
use tracing::{debug, error};

use crate::errors::SvcError;
use common_lib::{
    mbus_api,
    mbus_api::{
        BusClient, DynBus, Error, ErrorChain, Message, MessageId, ReceivedMessage,
        ReceivedRawMessage, TimeoutOptions,
    },
    types::{
        v0::message_bus::{ChannelVs, Liveness},
        Channel,
    },
};
use opentelemetry::trace::FutureExt;

/// Agent level errors
pub mod errors;
/// Messages required by a common handler
pub mod handler;
/// Version 0 of the message bus types
pub mod v0;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ServiceError {
    #[snafu(display("Channel '{}' has been closed.", channel.to_string()))]
    GetMessage { channel: Channel },
    #[snafu(display("Failed to subscribe on Channel '{}'", channel.to_string()))]
    Subscribe { channel: Channel, source: Error },
    #[snafu(display("Failed to get message Id on Channel '{}'", channel.to_string()))]
    GetMessageId { channel: Channel, source: Error },
    #[snafu(display("Failed to find subscription '{}' on Channel '{}'", id.to_string(), channel.to_string()))]
    FindSubscription { channel: Channel, id: MessageId },
    #[snafu(display("Subscription Semaphore closed"))]
    SubscriptionClosed {},
    #[snafu(display("Failed to handle message id '{}' on Channel '{}', details: {}", id.to_string(), channel.to_string(), details))]
    HandleMessage {
        channel: Channel,
        id: MessageId,
        details: String,
    },
}

impl From<tokio::sync::AcquireError> for ServiceError {
    fn from(_: tokio::sync::AcquireError) -> Self {
        Self::SubscriptionClosed {}
    }
}

/// Runnable service with N subscriptions which listen on a given
/// message bus channel on a specific ID
pub struct Service {
    server: String,
    server_connected: bool,
    no_min_timeouts: bool,
    channel: Channel,
    subscriptions: HashMap<String, Vec<Box<dyn ServiceSubscriber>>>,
    shared_state: std::sync::Arc<Container![Send + Sync]>,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            server: "".to_string(),
            server_connected: false,
            channel: Default::default(),
            subscriptions: Default::default(),
            shared_state: std::sync::Arc::new(<Container![Send + Sync]>::new()),
            no_min_timeouts: !common_lib::ENABLE_MIN_TIMEOUTS,
        }
    }
}

#[derive(Clone)]
/// Service Arguments for the service handler callback
pub struct Arguments<'a> {
    /// Service context, like access to the message bus
    pub context: Context,
    /// Access to the actual message bus request
    pub request: Arc<Request<'a>>,
}

impl<'a> Arguments<'a> {
    /// Returns a new Service Argument to be use by a Service Handler
    pub fn new(context: Context, request: Arc<ReceivedRawMessage<'a>>) -> Self {
        Self { context, request }
    }
}

/// Service handling context
/// the message bus which triggered the service callback
#[derive(Clone)]
pub struct Context {
    bus: Arc<DynBus>,
    state: Arc<Container![Send + Sync]>,
}

impl Context {
    /// create a new context
    pub fn new(bus: Arc<DynBus>, state: Arc<Container![Send + Sync]>) -> Self {
        Self { bus, state }
    }
    /// get the message bus from the context
    pub fn get_bus_as_ref(&self) -> &DynBus {
        &self.bus
    }
    /// get the shared state of type `T` from the context
    pub fn get_state<T: Send + Sync + 'static>(&self) -> Result<&T, SvcError> {
        match self.state.try_get() {
            Some(state) => Ok(state),
            None => {
                let type_name = std::any::type_name::<T>();
                let error_msg = format!(
                    "Requested data type '{}' not shared via with_shared_data",
                    type_name
                );
                error!("{}", error_msg);
                Err(SvcError::Internal { details: error_msg })
            }
        }
    }
}

/// Service Request received via the message bus
pub type Request<'a> = ReceivedRawMessage<'a>;

#[async_trait]
#[clonable]
/// Trait which must be implemented by each subscriber with the handler
/// which processes the messages and a filter to match message types
pub trait ServiceSubscriber: Clone + Send + Sync {
    /// async handler which processes the messages
    async fn handler(&self, args: Arguments<'_>) -> Result<(), SvcError>;
    /// filter which identifies which messages may be routed to the handler
    fn filter(&self) -> Vec<MessageId>;
}

impl Service {
    /// Setup default service connecting to `server` on subject `channel`
    pub fn builder(server: String, channel: impl Into<Channel>) -> Self {
        Self {
            server,
            server_connected: false,
            channel: channel.into(),
            ..Default::default()
        }
    }

    /// Connect to the provided message bus server immediately
    /// Useful for when dealing with async shared data which might required the
    /// message bus before the builder is complete
    pub async fn connect_message_bus(
        mut self,
        no_min_timeouts: bool,
        client: impl Into<Option<BusClient>>,
    ) -> Self {
        self.message_bus_init(no_min_timeouts, client).await;
        self
    }

    async fn message_bus_init(
        &mut self,
        no_min_timeouts: bool,
        client: impl Into<Option<BusClient>>,
    ) {
        if !self.server_connected {
            let timeout_opts = if no_min_timeouts {
                TimeoutOptions::new_no_retries().with_req_timeout(None)
            } else {
                TimeoutOptions::new_no_retries()
            };
            // todo: parse connection options when nats has better support
            mbus_api::message_bus_init_options(client, self.server.clone(), timeout_opts).await;
            self.server_connected = true;
            self.no_min_timeouts = no_min_timeouts;
        }
    }

    /// Setup default `channel` where `with_subscription` will listen on
    pub fn with_channel(mut self, channel: impl Into<Channel>) -> Self {
        self.channel = channel.into();
        self
    }

    /// Add a new service-wide shared state which can be retried in the handlers
    /// (more than one type of data can be added).
    /// The type must be `Send + Sync + 'static`.
    ///
    /// Example:
    /// # async fn main() {
    /// Service::builder(cli_args.url, Channel::Registry)
    ///         .with_shared_state(NodeStore::default())
    ///         .with_shared_state(More {})
    ///         .with_subscription(ServiceHandler::<Register>::default())
    ///         .run().await;
    ///
    /// # async fn handler(&self, args: Arguments<'_>) -> Result<(), SvcError> {
    ///    let store: &NodeStore = args.context.get_state()?;
    ///    let more: &More = args.context.get_state()?;
    /// # Ok(())
    /// # }
    pub fn with_shared_state<T: Send + Sync + 'static>(self, state: T) -> Self {
        let type_name = std::any::type_name::<T>();
        tracing::debug!("Adding shared type: {}", type_name);
        if !self.shared_state.set(state) {
            panic!(
                "Shared state for type '{}' has already been set!",
                type_name
            );
        }
        self
    }
    /// Get the shared state of type `T` added with `with_shared_state`
    pub fn get_shared_state<T: Send + Sync + 'static>(&self) -> &T {
        match self.shared_state.try_get() {
            Some(state) => state,
            None => {
                let type_name = std::any::type_name::<T>();
                let error_msg = format!(
                    "Requested data type '{}' not shared via with_shared_data",
                    type_name
                );
                panic!("{}", error_msg);
            }
        }
    }

    /// Add a default liveness endpoint which can be used to probe
    /// the service for liveness on the current selected channel.
    ///
    /// Example:
    /// # async fn main() {
    /// Service::builder(cli_args.url, ChannelVs::Node)
    ///         .with_default_liveness()
    ///         .with_subscription(ServiceHandler::<GetNodes>::default())
    ///         .run().await;
    ///
    /// # async fn alive() -> bool {
    ///    Liveness{}.request().await.is_ok()
    /// # }
    pub fn with_default_liveness(self) -> Self {
        #[derive(Clone, Default)]
        struct ServiceHandler<T> {
            data: std::marker::PhantomData<T>,
        }

        #[async_trait]
        impl ServiceSubscriber for ServiceHandler<Liveness> {
            async fn handler(&self, args: Arguments<'_>) -> Result<(), SvcError> {
                let request: ReceivedMessage<Liveness> = args.request.try_into()?;
                Ok(request.reply(()).await?)
            }
            fn filter(&self) -> Vec<MessageId> {
                vec![Liveness::default().id()]
            }
        }

        self.with_subscription(ServiceHandler::<Liveness>::default())
    }

    /// Configure `self` through a configure closure
    pub fn configure<F>(self, configure: F) -> Self
    where
        F: FnOnce(Service) -> Service,
    {
        configure(self)
    }

    /// Configure `self` through an async configure closure
    pub async fn configure_async<F, Fut>(self, configure: F) -> Self
    where
        F: FnOnce(Service) -> Fut,
        Fut: Future<Output = Service>,
    {
        configure(self).await
    }

    /// Add a new subscriber on the default channel
    pub fn with_subscription(self, service_subscriber: impl ServiceSubscriber + 'static) -> Self {
        let channel = self.channel.clone();
        self.with_subscription_channel(channel, service_subscriber)
    }

    /// Add a new subscriber on the given `channel`
    pub fn with_subscription_channel(
        mut self,
        channel: Channel,
        service_subscriber: impl ServiceSubscriber + 'static,
    ) -> Self {
        match self.subscriptions.get_mut(&channel.to_string()) {
            Some(entry) => {
                entry.push(Box::from(service_subscriber));
            }
            None => {
                self.subscriptions
                    .insert(channel.to_string(), vec![Box::from(service_subscriber)]);
            }
        };
        self
    }

    async fn run_channel(
        bus: DynBus,
        channel: Channel,
        subscriptions: Vec<Box<dyn ServiceSubscriber>>,
        state: std::sync::Arc<Container![Send + Sync]>,
    ) -> Result<(), ServiceError> {
        let bus = Arc::new(bus);
        let handle = bus.subscribe(channel.clone()).await.context(Subscribe {
            channel: channel.clone(),
        })?;

        // Gated access to a subscription. This means we can concurrently handle CreateVolume and
        // GetVolume but we handle CreateVolume one at a time.
        let concurrency: usize = std::env::var("MAX_CONCURRENT_RPC")
            .ok()
            .and_then(|i| i.parse().ok())
            .unwrap_or(3usize);
        let gated_subs = Arc::new(
            subscriptions
                .into_iter()
                .map(|i| (tokio::sync::Semaphore::new(concurrency), i))
                .collect::<Vec<_>>(),
        );

        let mut signal_term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        let mut signal_int =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt());
        loop {
            let state = state.clone();
            let bus = bus.clone();
            let gated_subs = gated_subs.clone();

            let message = match (signal_term.as_mut(), signal_int.as_mut()) {
                (Ok(term), Ok(int)) => {
                    tokio::select! {
                        _evt = term.recv() => {
                             opentelemetry::global::force_flush_tracer_provider();
                            return Ok(())
                        },
                        _evt = int.recv() => {
                             opentelemetry::global::force_flush_tracer_provider();
                            return Ok(())
                        },
                        message = handle.next() => {
                            message
                        }
                    }
                }
                (Ok(signal), Err(_)) | (Err(_), Ok(signal)) => {
                    tokio::select! {
                        _evt = signal.recv() => {
                             opentelemetry::global::force_flush_tracer_provider();
                            return Ok(())
                        },
                        message = handle.next() => {
                            message
                        }
                    }
                }
                _ => handle.next().await,
            }
            .context(GetMessage {
                channel: channel.clone(),
            })?;

            tokio::spawn(async move {
                let context = Context::new(bus, state);
                let mut req = Request::from(&message);
                req.set_context(context.get_state().ok());

                let args = Arguments::new(context, Arc::new(req));
                if args.request.channel() != Channel::v0(ChannelVs::Registry) {
                    debug!("Processing message: {{ {} }}", args.request);
                }

                if let Err(error) = Self::process_message(args, &gated_subs).await {
                    error!("Error processing message: {}", error.full_string());
                }
            });
        }
    }

    async fn process_message(
        arguments: Arguments<'_>,
        subscriptions: &Arc<Vec<(tokio::sync::Semaphore, Box<dyn ServiceSubscriber>)>>,
    ) -> Result<(), ServiceError> {
        let channel = arguments.request.channel();
        let id = &arguments.request.id().context(GetMessageId {
            channel: channel.clone(),
        })?;

        let mut subscription = Err(ServiceError::FindSubscription {
            channel: channel.clone(),
            id: id.clone(),
        });
        for sub in subscriptions.iter() {
            if sub.1.filter().iter().any(|find_id| find_id == id) {
                subscription = Ok(sub);
                break;
            }
        }
        let subscription = subscription?;
        let _guard = subscription.0.acquire().await?;

        match subscription
            .1
            .handler(arguments.clone())
            .with_context(arguments.request.context())
            .await
        {
            Ok(_) => Ok(()),
            Err(error) => {
                let result = ServiceError::HandleMessage {
                    channel,
                    id: id.clone(),
                    details: error.full_string(),
                };
                // respond back to the sender with an error, ignore the outcome
                arguments
                    .request
                    .respond::<(), _>(Err(error.into()))
                    .await
                    // ignore the outcome, since we're already in error
                    .ok();
                Err(result)
            }
        }
    }

    /// Runs the server which services all subscribers asynchronously until all
    /// subscribers are closed
    ///
    /// subscribers are sorted according to the channel they subscribe on
    /// each channel benefits from a tokio thread which routes messages
    /// accordingly todo: only one subscriber per message id supported at
    /// the moment
    pub async fn run(mut self) {
        let mut threads = vec![];

        self.message_bus_init(self.no_min_timeouts, BusClient::CoreAgent)
            .await;
        let bus = mbus_api::bus();

        for subscriptions in self.subscriptions.iter() {
            let bus = bus.clone();
            let channel = subscriptions.0.clone();
            let subscriptions = subscriptions.1.clone();
            let state = self.shared_state.clone();

            let handle = tokio::spawn(async move {
                Self::run_channel(bus, channel.parse().unwrap(), subscriptions, state).await
            });

            threads.push(handle);
        }

        join_all(threads)
            .await
            .iter()
            .for_each(|result| match result {
                Err(error) => error!("Failed to wait for thread: {:?}", error),
                Ok(Err(error)) => {
                    error!("Error running channel thread: {:?}", error)
                }
                _ => {}
            });
    }
}
