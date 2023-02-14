// clippy warning caused by the instrument macro
#![allow(clippy::unit_arg)]

use crate::CORE_CLIENT;
use ::rpc::io_engine::{JsonRpcReply, JsonRpcRequest};
use agents::errors::{JsonRpcDeserialise, NodeNotOnline, SvcError};
use grpc::{
    context::Context,
    operations::{
        jsongrpc::traits::{JsonGrpcOperations, JsonGrpcRequestInfo},
        node::traits::NodeOperations,
    },
};
use rpc::io_engine::json_rpc_client::JsonRpcClient;
use serde_json::Value;
use snafu::{OptionExt, ResultExt};
use stor_port::{
    transport_api::ReplyError,
    types::v0::transport::{ApiVersion, Filter, JsonGrpcRequest, Node, NodeId},
};

#[derive(Clone, Default)]
pub(super) struct JsonGrpcSvc {}

/// JSON gRPC service implementation
impl JsonGrpcSvc {
    /// create a new jsongrpc service
    pub(super) fn new() -> Self {
        Self {}
    }

    /// Generic JSON gRPC call issued to the IoEngine using the JsonRpcClient.
    pub(super) async fn json_grpc_call(
        &self,
        request: &JsonGrpcRequest,
    ) -> Result<serde_json::Value, SvcError> {
        let response = match CORE_CLIENT
            .get()
            .expect("Client is not initialised")
            .node() // get node client
            .get(Filter::Node(request.clone().node), None)
            .await
        {
            Ok(response) => response,
            Err(err) => {
                return Err(SvcError::GetNode {
                    node: request.node.to_string(),
                    source: err,
                })
            }
        };
        let node = node(request.clone().node, response.into_inner().get(0))?;
        let node = node.state().context(NodeNotOnline {
            node: request.node.to_owned(),
        })?;

        let mut api_versions = node.api_versions.clone().unwrap_or_default();
        api_versions.sort();

        // todo: use the cli argument timeouts
        let response = match api_versions.last().unwrap_or(&ApiVersion::V1) {
            ApiVersion::V0 => {
                let mut client = JsonRpcClient::connect(format!("http://{}", node.grpc_endpoint))
                    .await
                    .unwrap();
                let response: JsonRpcReply = client
                    .json_rpc_call(JsonRpcRequest {
                        method: request.method.to_string(),
                        params: request.params.to_string(),
                    })
                    .await
                    .map_err(|error| SvcError::JsonRpc {
                        method: request.method.to_string(),
                        params: request.params.to_string(),
                        error: error.to_string(),
                    })?
                    .into_inner();
                response.result
            }
            ApiVersion::V1 => {
                let mut client =
                    rpc::v1::json::JsonRpcClient::connect(format!("http://{}", node.grpc_endpoint))
                        .await
                        .unwrap();
                let response: rpc::v1::json::JsonRpcResponse = client
                    .json_rpc_call(rpc::v1::json::JsonRpcRequest {
                        method: request.method.to_string(),
                        params: request.params.to_string(),
                    })
                    .await
                    .map_err(|error| SvcError::JsonRpc {
                        method: request.method.to_string(),
                        params: request.params.to_string(),
                        error: error.to_string(),
                    })?
                    .into_inner();
                response.result
            }
        };

        serde_json::from_str(&response).context(JsonRpcDeserialise)
    }
}

#[tonic::async_trait]
impl JsonGrpcOperations for JsonGrpcSvc {
    async fn call(
        &self,
        req: &dyn JsonGrpcRequestInfo,
        _ctx: Option<Context>,
    ) -> Result<Value, ReplyError> {
        let req = req.into();
        let service = self.clone();
        let response = Context::spawn(async move { service.json_grpc_call(&req).await }).await??;
        Ok(response)
    }
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        return Ok(true);
    }
}

/// returns node from node option and returns an error on non existence
fn node(node_id: NodeId, node: Option<&Node>) -> Result<Node, SvcError> {
    match node {
        Some(node) => Ok(node.clone()),
        None => Err(SvcError::NodeNotFound { node_id }),
    }
}
