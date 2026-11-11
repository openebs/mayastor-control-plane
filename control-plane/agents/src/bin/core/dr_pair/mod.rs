use crate::controller::registry::Registry;
use agents::Service;
use grpc::operations::dr_pair::server::DrPairServer;
use std::sync::Arc;

mod service;
mod specs;

pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    builder.with_service(DrPairServer::new(new_service).into_grpc_server())
}
