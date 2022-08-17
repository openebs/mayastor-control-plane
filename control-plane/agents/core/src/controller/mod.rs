//! Common modules used by the different core services

/// gRPC helpers
pub(crate) mod grpc;
pub(crate) mod operations;
/// reconciliation logic
pub(crate) mod reconciler;
/// registry with node and all its resources
pub(crate) mod registry;
/// generic resources
mod resource_map;
/// helpers for node/pool/replica scheduling
pub(crate) mod scheduling;
/// registry with all the resource specs
pub(crate) mod specs;
/// registry with all the resource states
pub(crate) mod states;
/// generic task pollers (eg used by the reconcilers)
mod task_poller;
/// helper wrappers over the resources
pub(crate) mod wrapper;

/// Core tests
#[cfg(test)]
mod tests;
