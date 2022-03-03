// clippy warning caused by the instrument macro
#![allow(clippy::unit_arg)]

use crate::CORE_CLIENT;
use ::rpc::mayastor::{JsonRpcReply, JsonRpcRequest};
use common::errors::{JsonRpcDeserialise, NodeNotOnline, SvcError};
use common_lib::types::v0::message_bus::{Filter, JsonGrpcRequest, Node, NodeId};
use grpc::operations::node::traits::NodeOperations;
use rpc::mayastor::json_rpc_client::JsonRpcClient;
use snafu::{OptionExt, ResultExt};

#[derive(Clone, Default)]
pub(super) struct JsonGrpcSvc {}

/// JSON gRPC service implementation
impl JsonGrpcSvc {
    /// Generic JSON gRPC call issued to Mayastor using the JsonRpcClient.
    pub(super) async fn json_grpc_call(
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
                return Err(SvcError::BusGetNode {
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

        Ok(serde_json::from_str(&response.result).context(JsonRpcDeserialise)?)
    }
}

/// returns node from node option and returns an error on non existence
fn node(node_id: NodeId, node: Option<&Node>) -> Result<Node, SvcError> {
    match node {
        Some(node) => Ok(node.clone()),
        None => Err(SvcError::NodeNotFound { node_id }),
    }
}
