mod registry;
pub mod service;
pub mod specs;

use super::core::registry::Registry;
use std::sync::Arc;

use common::Service;
use grpc::operations::{pool::server::PoolServer, replica::server::ReplicaServer};

pub(crate) async fn configure(builder: Service) -> Service {
    let registry = builder.get_shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let pool_service = PoolServer::new(new_service.clone());
    let replica_service = ReplicaServer::new(new_service);
    builder
        .with_shared_state(pool_service)
        .with_shared_state(replica_service)
}

/// Pool Agent's Tests
#[cfg(test)]
mod tests;
