use crate::{context::Context, jsongrpc};
use common_lib::{
    transport_api::ReplyError,
    types::v0::transport::{JsonGrpcMethod, JsonGrpcParams, JsonGrpcRequest, NodeId},
};

/// All jsongrpc operations to be a part of the JsonGrpcOperations trait
#[tonic::async_trait]
pub trait JsonGrpcOperations: Send + Sync {
    /// make a json grpc call
    async fn call(
        &self,
        req: &dyn JsonGrpcRequestInfo,
        ctx: Option<Context>,
    ) -> Result<serde_json::Value, ReplyError>;
    /// Liveness probe for jsongrpc service
    async fn probe(&self, ctx: Option<Context>) -> Result<bool, ReplyError>;
}

/// Trait to be implemented for json_grpc_call operation
pub trait JsonGrpcRequestInfo: Send + Sync + std::fmt::Debug {
    /// id of the io-engine instance
    fn node(&self) -> NodeId;
    /// JSON gRPC method to call
    fn method(&self) -> JsonGrpcMethod;
    /// parameters to be passed to the above method
    fn params(&self) -> JsonGrpcParams;
}

impl JsonGrpcRequestInfo for JsonGrpcRequest {
    fn node(&self) -> NodeId {
        self.node.clone()
    }

    fn method(&self) -> JsonGrpcMethod {
        self.method.clone()
    }

    fn params(&self) -> JsonGrpcParams {
        self.params.clone()
    }
}

impl JsonGrpcRequestInfo for jsongrpc::JsonGrpcRequest {
    fn node(&self) -> NodeId {
        self.node_id.clone().into()
    }

    fn method(&self) -> JsonGrpcMethod {
        self.json_grpc_method.clone().into()
    }

    fn params(&self) -> JsonGrpcParams {
        self.json_grpc_params.clone().into()
    }
}

impl From<&dyn JsonGrpcRequestInfo> for JsonGrpcRequest {
    fn from(data: &dyn JsonGrpcRequestInfo) -> Self {
        Self {
            node: data.node(),
            method: data.method(),
            params: data.params(),
        }
    }
}

impl From<&dyn JsonGrpcRequestInfo> for jsongrpc::JsonGrpcRequest {
    fn from(data: &dyn JsonGrpcRequestInfo) -> Self {
        Self {
            node_id: data.node().to_string(),
            json_grpc_method: data.method().to_string(),
            json_grpc_params: data.params().to_string(),
        }
    }
}
