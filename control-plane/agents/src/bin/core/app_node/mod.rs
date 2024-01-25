use crate::controller::registry::Registry;
use agents::Service;
use grpc::operations::app_node::server::AppNodeServer;
use std::sync::Arc;

mod registry;
mod service;
mod specs;

pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let app_node_server = AppNodeServer::new(new_service);
    builder
        .with_service(app_node_server.clone().into_v1_grpc_server())
        .with_service(app_node_server.into_v1_registration_grpc_server())
}
