#![warn(missing_docs)]

use common::ServiceError;
use futures::FutureExt;
use grpc::{
    operations::{
        nexus::server::NexusServer, node::server::NodeServer, pool::server::PoolServer,
        registration::server::RegistrationServer, registry::server::RegistryServer,
        replica::server::ReplicaServer, volume::server::VolumeServer, watch::server::WatchServer,
    },
    tracing::OpenTelServer,
};
use http::Uri;
use tracing::error;

/// the gprc service that encapsulates the base_service and the server for rpc
pub(crate) struct Service {
    base_service: common::Service,
}

impl Service {
    /// Creates a new Service with the base_service.
    pub(crate) fn new(base_service: common::Service) -> Self {
        Self { base_service }
    }

    /// Launch the tonic server with the required services.
    /// todo: allow the base server to handle this through the configure calls.
    pub(crate) async fn run(self) {
        let grpc_addr = self.base_service.shared_state::<Uri>().clone();
        let pool_service = self.base_service.shared_state::<PoolServer>().clone();
        let replica_service = self.base_service.shared_state::<ReplicaServer>().clone();
        let volume_service = self.base_service.shared_state::<VolumeServer>().clone();
        let node_service = self.base_service.shared_state::<NodeServer>().clone();
        let registration_service = self
            .base_service
            .shared_state::<RegistrationServer>()
            .clone();
        let registry_service = self.base_service.shared_state::<RegistryServer>().clone();
        let nexus_service = self.base_service.shared_state::<NexusServer>().clone();
        let watch_service = self.base_service.shared_state::<WatchServer>().clone();

        let tonic_router = self
            .base_service
            .tonic_server()
            .layer(OpenTelServer::new())
            .add_service(pool_service.into_grpc_server())
            .add_service(replica_service.into_grpc_server())
            .add_service(volume_service.into_grpc_server())
            .add_service(node_service.into_grpc_server())
            .add_service(registration_service.into_grpc_server())
            .add_service(registry_service.into_grpc_server())
            .add_service(nexus_service.into_grpc_server())
            .add_service(watch_service.into_grpc_server());

        let result = tonic_router
            .serve_with_shutdown(
                grpc_addr.authority().unwrap().to_string().parse().unwrap(),
                common::Service::shutdown_signal().map(|_| ()),
            )
            .await
            .map_err(|source| ServiceError::GrpcServer { source });

        if let Err(error) = result {
            error!(error=?error, "Error running service thread");
        }
    }
}
