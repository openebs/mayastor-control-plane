#![allow(clippy::derive_partial_eq_without_eq)]

/// All the control-plane resources which are persisted in the persistent store.
pub mod store;
/// All the "transport" types which allow control-plane components to interact with each other
/// through the grpc transport interface.
/// Note, grpc is not a representative name, as it is simply the current implementation but the
/// transport interfaces are agnostic of any particular "rpc" medium.
pub mod transport;

/// Reexport the openapi.
pub mod openapi;
