use crate::controller::registry::Registry;
use agents::Service;
use grpc::operations::frontend_node::server::FrontendNodeServer;
use std::sync::Arc;

mod registry;
mod service;
mod spec;

pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let frontend_server = FrontendNodeServer::new(new_service);
    builder
        .with_service(frontend_server.clone().into_v1_grpc_server())
        .with_service(frontend_server.into_v1_registration_grpc_server())
}
