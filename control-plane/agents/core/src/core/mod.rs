//! Common modules used by the different core services

/// gRPC helpers
pub mod grpc;
/// registry with node and all its resources
pub mod registry;
/// registry with all the resource specs
pub mod specs;
/// helper wrappers over the resources
pub mod wrapper;

/// Core tests
#[cfg(test)]
mod tests;
