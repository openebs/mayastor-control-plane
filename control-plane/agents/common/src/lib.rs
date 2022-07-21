#![warn(missing_docs)]
//! Control Plane Agents library with emphasis on the message bus interaction
//! including errors.
//!
//! It's meant to facilitate the creation of agents with a helper builder to
//! subscribe handlers for different message identifiers.

use crate::errors::SvcError;
use common_lib::transport_api::DynClient;
use futures::Future;
use snafu::Snafu;
use state::Container;
use std::sync::Arc;
use tracing::error;

/// Agent level errors
pub mod errors;
/// Version 0 of the message bus types
pub mod v0;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ServiceError {
    #[snafu(display("GrpcServer error"))]
    GrpcServer { source: tonic::transport::Error },
}

/// Runnable service with N subscriptions which listen on a given
/// message bus channel on a specific ID
pub struct Service {
    shared_state: std::sync::Arc<Container![Send + Sync]>,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            shared_state: std::sync::Arc::new(<Container![Send + Sync]>::new()),
        }
    }
}

/// Service handling context
/// the message bus which triggered the service callback
#[derive(Clone)]
pub struct Context {
    client: Arc<DynClient>,
    state: Arc<Container![Send + Sync]>,
}

impl Context {
    /// create a new context
    pub fn new(client: Arc<DynClient>, state: Arc<Container![Send + Sync]>) -> Self {
        Self { client, state }
    }
    /// get the client opts from the context.
    pub fn client_opts(&self) -> &DynClient {
        &self.client
    }
    /// get the shared state of type `T` from the context
    pub fn state<T: Send + Sync + 'static>(&self) -> Result<&T, SvcError> {
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
    pub fn builder() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Add a new service-wide shared state which can be retried in the handlers
    /// (more than one type of data can be added).
    /// The type must be `Send + Sync + 'static`.
    ///
    /// Example:
    /// # async fn main() {
    /// Service::builder()
    ///         .with_shared_state(NodeStore::default())
    ///         .with_shared_state(More {})
    ///         .configure(configure)
    ///         .run().await;
    ///
    /// # async fn configure(builder: Service) -> Service {
    /// #  builder
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
    pub fn shared_state<T: Send + Sync + 'static>(&self) -> &T {
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
