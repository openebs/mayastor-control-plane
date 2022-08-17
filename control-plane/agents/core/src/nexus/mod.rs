use super::controller::registry::Registry;
use grpc::operations::nexus::server::NexusServer;
use std::sync::Arc;

mod operations;
mod registry;
/// Nexus Scheduling helpers.
pub(crate) mod scheduling;
mod service;
mod specs;

/// Configure the Nexus Service and return the builder.
pub(crate) fn configure(builder: common::Service) -> common::Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let nexus_service = NexusServer::new(new_service);
    builder.with_shared_state(nexus_service)
}

/// Nexus Agent's Tests
#[cfg(test)]
mod tests;
