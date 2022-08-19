mod service;
mod watches;

use super::controller::registry::Registry;
use grpc::operations::watch::server::WatchServer;
use std::sync::Arc;

/// Configure the Service and return the builder.
pub(crate) fn configure(builder: common::Service) -> common::Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let watch_service = WatchServer::new(new_service);
    builder.with_shared_state(watch_service)
}
