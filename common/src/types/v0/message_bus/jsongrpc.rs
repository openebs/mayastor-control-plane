use super::*;

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

bus_impl_string_id!(
    JsonGrpcParams,
    "Parameters to be passed to a JSON gRPC method"
);
bus_impl_string_id!(JsonGrpcMethod, "JSON gRPC method");

/// Generic JSON gRPC request
#[derive(Serialize, Deserialize, Default, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct JsonGrpcRequest {
    /// id of the mayastor instance
    pub node: NodeId,
    /// JSON gRPC method to call
    pub method: JsonGrpcMethod,
    /// parameters to be passed to the above method
    pub params: JsonGrpcParams,
}
