#![warn(missing_docs)]

use common::ServiceError;
use futures::{future::join_all, FutureExt};
use grpc::operations::{
    pool::server::PoolServer, replica::server::ReplicaServer, volume::server::VolumeServer,
};
use http::Uri;
use tracing::error;

/// the gprc service that encapsulates the base_service and the server for rpc
pub struct Service {
    base_service: common::Service,
    tonic_grpc_server: tonic::transport::Server,
}

impl Service {
    /// creates a new Service with the base_service and tonic server builder
    pub fn new(base_service: common::Service) -> Self {
        Self {
            base_service,
            tonic_grpc_server: tonic::transport::Server::builder(),
        }
    }

    /// launch each of the services and the grpc server
    pub async fn run(mut self) {
        let grpc_addr = self.base_service.get_shared_state::<Uri>().clone();
        let pool_service = self.base_service.get_shared_state::<PoolServer>().clone();
        let replica_service = self
            .base_service
            .get_shared_state::<ReplicaServer>()
            .clone();
        let volume_service = self.base_service.get_shared_state::<VolumeServer>().clone();

        let tonic_router = self
            .tonic_grpc_server
            .add_service(pool_service.into_grpc_server())
            .add_service(replica_service.into_grpc_server())
            .add_service(volume_service.into_grpc_server());

        let mut threads = self.base_service.mbus_handles().await;

        let tonic_thread = tokio::spawn(async move {
            tonic_router
                .serve_with_shutdown(
                    grpc_addr.authority().unwrap().to_string().parse().unwrap(),
                    Self::shutdown_signal().map(|_| ()),
                )
                .await
                .map_err(|source| ServiceError::GrpcServer { source })
        });

        threads.push(tonic_thread);

        join_all(threads)
            .await
            .iter()
            .for_each(|result| match result {
                Err(error) => error!("Failed to wait for thread: {:?}", error),
                Ok(Err(error)) => {
                    error!(error=?error, "Error running service thread");
                }
                _ => {}
            });
    }

    /// Get a shutdown_signal as a oneshot channel when the process receives either TERM or INT.
    /// When received the opentel traces are also immediately flushed.
    fn shutdown_signal() -> tokio::sync::oneshot::Receiver<()> {
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
