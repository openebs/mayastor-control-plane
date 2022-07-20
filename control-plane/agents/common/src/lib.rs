#![warn(missing_docs)]
//! Control Plane Agents library with emphasis on the message bus interaction
//! including errors.
//!
//! It's meant to facilitate the creation of agents with a helper builder to
//! subscribe handlers for different message identifiers.

use std::{convert::Into, sync::Arc};

use futures::Future;

use snafu::Snafu;
use state::Container;
use tracing::error;

use crate::errors::SvcError;
use common_lib::{
    transport_api,
    transport_api::{BusClient, DynBus, TimeoutOptions},
    types::Channel,
};

/// Agent level errors
pub mod errors;
/// Version 0 of the message bus types
pub mod v0;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ServiceError {
    #[snafu(display("Channel '{}' has been closed.", channel.to_string()))]
    GetMessage { channel: Channel },
    #[snafu(display("GrpcServer error"))]
    GrpcServer { source: tonic::transport::Error },
}

/// Runnable service with N subscriptions which listen on a given
/// message bus channel on a specific ID
pub struct Service {
    server: Option<String>,
    server_connected: bool,
    no_min_timeouts: bool,
    channel: Channel,
    shared_state: std::sync::Arc<Container![Send + Sync]>,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            server: None,
            server_connected: false,
            channel: Default::default(),
            shared_state: std::sync::Arc::new(<Container![Send + Sync]>::new()),
            no_min_timeouts: !utils::ENABLE_MIN_TIMEOUTS,
        }
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

impl Service {
    /// Setup default service connecting to `server` on subject `channel`
    pub fn builder(server: Option<String>, channel: impl Into<Channel>) -> Self {
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
            if let Some(server) = self.server.clone() {
                transport_api::message_bus_init_options(client, server, timeout_opts).await;
                self.server_connected = true;
            }
            self.no_min_timeouts = no_min_timeouts;
        }
    }

    /// Setup default `channel` where `with_subscription` will listen on
    #[must_use]
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
    #[must_use]
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

    /// Configure `self` through a configure closure
    #[must_use]
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
}
