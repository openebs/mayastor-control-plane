use crate::controller::registry::Registry;
use grpc::operations::volume::server::VolumeServer;
use std::sync::Arc;

mod clone_operations;
mod operations;
mod operations_helper;
mod registry;
mod scheduling;
mod service;
mod snapshot_helpers;
mod snapshot_operations;
mod specs;

pub(crate) use operations::MoveReplicaRequest;
pub(crate) use snapshot_operations::DestroyVolumeSnapshotRequest;

/// Configure the Service and return the builder.
pub(crate) fn configure(builder: agents::Service) -> agents::Service {
    let registry = builder.shared_state::<Registry>().clone();
    let new_service = Arc::new(service::Service::new(registry));
    let volume_service = VolumeServer::new(new_service);
    builder.with_service(volume_service.into_grpc_server())
}
