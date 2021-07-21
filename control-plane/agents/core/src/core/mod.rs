//! Common modules used by the different core services

/// gRPC helpers
pub mod grpc;
/// registry with node and all its resources
pub mod registry;
/// generic resources
mod resource_map;
/// helpers for node/pool/replica scheduling
pub(crate) mod scheduling;
/// registry with all the resource specs
pub mod specs;
/// registry with all the resource states
pub mod states;
/// helper wrappers over the resources
pub mod wrapper;

/// Core tests
#[cfg(test)]
mod tests;
