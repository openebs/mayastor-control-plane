use crate::controller::registry::Registry;
use grpc::operations::volume::server::VolumeServer;
use std::sync::Arc;

mod registry;
mod scheduling;
mod service;
pub mod specs;

pub(crate) fn configure(builder: common::Service) -> common::Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let volume_service = VolumeServer::new(new_service);
    builder.with_shared_state(volume_service)
}

/// Volume Agent's Tests
#[cfg(test)]
mod tests;
