mod service;
use crate::core::registry::Registry;
use common::Service;
use grpc::operations::registry::server::RegistryServer;
use std::sync::Arc;

/// Configure the registry service
pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    let registry_service = RegistryServer::new(Arc::new(service::Service::new(registry)));
    builder.with_shared_state(registry_service)
}
