//! Common modules used by the different core services

/// gRPC helpers
pub(crate) mod grpc;
/// reconciliation logic
pub(crate) mod reconciler;
/// registry with node and all its resources
pub(crate) mod registry;
/// Resources and their helper interfaces.
pub mod resources;
/// helpers for node/pool/replica scheduling
pub(crate) mod scheduling;
/// registry with all the resource states
pub(crate) mod states;
/// generic task pollers (eg used by the reconcilers)
mod task_poller;
/// helper wrappers over the resources
pub(crate) mod wrapper;
