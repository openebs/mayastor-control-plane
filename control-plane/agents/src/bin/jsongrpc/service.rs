// clippy warning caused by the instrument macro
#![allow(clippy::unit_arg)]

use crate::CORE_CLIENT;
use ::rpc::io_engine::{JsonRpcReply, JsonRpcRequest};
use agents::errors::{JsonRpcDeserialise, NodeNotOnline, SvcError};
use common_lib::{
    transport_api::ReplyError,
    types::v0::transport::{Filter, JsonGrpcRequest, Node, NodeId},
};
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
        // todo: use the cli argument timeouts
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

        serde_json::from_str(&response.result).context(JsonRpcDeserialise)
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
