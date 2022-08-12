#![warn(missing_docs)]
//! Control Plane Agents library with emphasis on the message bus interaction
//! including errors.
//!
//! It's meant to facilitate the creation of agents with a helper builder to
//! subscribe handlers for different message identifiers.

use futures::Future;
use snafu::Snafu;
use state::Container;
use std::sync::Arc;

/// Agent level errors
pub mod errors;
/// message translation to agent types from rpc v0,v1 types
pub mod msg_translation;

#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum ServiceError {
    #[snafu(display("GrpcServer error"))]
    GrpcServer { source: tonic::transport::Error },
}

/// Runnable service with N subscriptions which listen on a given
/// message bus channel on a specific ID
pub struct Service {
    shared_state: Arc<Container![Send + Sync]>,
    tonic_server: tonic::transport::Server,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            shared_state: Arc::new(<Container![Send + Sync]>::new()),
            tonic_server: tonic::transport::Server::builder(),
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

    /// Get the configured tonic Server.
    pub fn tonic_server(self) -> tonic::transport::Server {
        self.tonic_server
    }

    /// Get a shutdown_signal as a oneshot channel when the process receives either TERM or INT.
    /// When received the opentel traces are also immediately flushed.
    pub fn shutdown_signal() -> tokio::sync::oneshot::Receiver<()> {
        let mut signal_term =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        let mut signal_int =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()).unwrap();
        let (stop_sender, stop_receiver) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            tokio::select! {
                _term = signal_term.recv() => {tracing::info!("SIGTERM received")},
                _int = signal_int.recv() => {tracing::info!("SIGINT received")},
            }
            opentelemetry::global::force_flush_tracer_provider();
            if stop_sender.send(()).is_err() {
                // should we panic here?
                tracing::warn!("Failed to stop the tonic server");
            }
        });
        stop_receiver
    }
}
