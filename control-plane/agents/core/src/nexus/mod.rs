use super::controller::registry::Registry;
use grpc::operations::nexus::server::NexusServer;
use std::sync::Arc;

pub(crate) mod registry;
pub(crate) mod scheduling;
mod service;
pub mod specs;

pub(crate) fn configure(builder: common::Service) -> common::Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let nexus_service = NexusServer::new(new_service);
    builder.with_shared_state(nexus_service)
}

/// Nexus Agent's Tests
#[cfg(test)]
mod tests;
