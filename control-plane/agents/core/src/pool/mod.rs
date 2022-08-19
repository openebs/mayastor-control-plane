mod pool_operations;
mod registry;
mod replica_operations;
mod service;
mod specs;

use super::controller::registry::Registry;
use std::sync::Arc;

use common::Service;
use grpc::operations::{pool::server::PoolServer, replica::server::ReplicaServer};

/// Configure the Service and return the builder.
pub(crate) fn configure(builder: Service) -> Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let pool_service = PoolServer::new(new_service.clone());
    let replica_service = ReplicaServer::new(new_service);
    builder
        .with_shared_state(pool_service)
        .with_shared_state(replica_service)
}
