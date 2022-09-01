mod service;
use crate::controller::registry::Registry;
use agents::Service;
use grpc::operations::registry::server::RegistryServer;
use std::sync::Arc;

/// Configure the registry service
pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.shared_state::<Registry>().clone();
    let registry_service = RegistryServer::new(Arc::new(service::Service::new(registry)));
    builder.with_service(registry_service.into_grpc_server())
}
