// clippy warning caused by the instrument macro
#![allow(clippy::unit_arg)]

use ::rpc::mayastor::{JsonRpcReply, JsonRpcRequest};
use common::errors::{BusGetNode, JsonRpcDeserialise, NodeNotOnline, SvcError};
use common_lib::{
    mbus_api::message_bus::v0::{MessageBus, *},
    types::v0::message_bus::JsonGrpcRequest,
};
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
        let node = MessageBus::get_node(&request.node)
            .await
            .context(BusGetNode {
                node: request.node.clone(),
            })?;
        let node = node.state().context(NodeNotOnline {
            node: request.node.to_owned(),
        })?;
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
