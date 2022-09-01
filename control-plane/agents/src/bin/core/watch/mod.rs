mod service;
mod watches;

use super::controller::registry::Registry;
use grpc::operations::watch::server::WatchServer;
use std::sync::Arc;

/// Configure the Service and return the builder.
pub(crate) fn configure(builder: agents::Service) -> agents::Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let watch_service = WatchServer::new(new_service);
    builder.with_service(watch_service.into_grpc_server())
}
