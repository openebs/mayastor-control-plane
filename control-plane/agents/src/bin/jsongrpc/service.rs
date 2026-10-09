// clippy warning caused by the instrument macro
#![allow(clippy::unit_arg)]

use crate::CORE_CLIENT;
use ::rpc::io_engine::{JsonRpcReply, JsonRpcRequest};
use agents::errors::{GrpcConnect, GrpcConnectUri, JsonRpcDeserialise, NodeNotOnline, SvcError};
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
pub(super) struct JsonGrpcSvc {
    /// Reject io-engines that do not advertise gRPC TLS support instead of
    /// connecting over plaintext.
    grpc_tls_enforced: bool,
}

/// JSON gRPC service implementation
impl JsonGrpcSvc {
    /// create a new jsongrpc service
    pub(super) fn new(grpc_tls_enforced: bool) -> Self {
        Self { grpc_tls_enforced }
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
            .get(Filter::Node(request.clone().node), false, None)
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
        let node = node(request.clone().node, response.into_inner().first())?;
        // Whether the io-engine serves its gRPC over (auto-)TLS, as advertised in its node
        // features. When enabled we must connect with TLS to match the io-engine server.
        let grpc_tls = node
            .spec()
            .and_then(|spec| spec.features().as_ref())
            .or_else(|| node.state().and_then(|state| state.features.as_ref()))
            .and_then(|features| features.grpc_tls)
            .unwrap_or(false);
        let node = node.state().context(NodeNotOnline {
            node: request.node.to_owned(),
        })?;

        // When TLS is enforced, refuse to fall back to a plaintext connection for an io-engine
        // that doesn't advertise gRPC TLS support, rather than talking to it in the clear.
        if self.grpc_tls_enforced && !grpc_tls {
            return Err(SvcError::GrpcTlsRequired {
                node_id: node.id.to_string(),
                endpoint: node.grpc_endpoint.to_string(),
            });
        }

        let mut api_versions = node.api_versions.clone().unwrap_or_default();
        api_versions.sort();

        // The io-engine endpoint always carries an `http` scheme; with TLS the handshake is
        // performed by the auto-TLS connector rather than by tonic (see `grpc::tls::io_connect`).
        let uri = http::uri::Uri::builder()
            .scheme("http")
            .authority(node.grpc_endpoint.to_string())
            .path_and_query("")
            .build()
            .context(GrpcConnectUri {
                node_id: node.id.to_string(),
                uri: node.grpc_endpoint.to_string(),
            })?;
        let channel = grpc::tls::io_connect(tonic::transport::Endpoint::from(uri), grpc_tls)
            .await
            .context(GrpcConnect {
                node_id: node.id.to_string(),
                endpoint: node.grpc_endpoint.to_string(),
            })?;

        // todo: use the cli argument timeouts
        let response = match api_versions.last().unwrap_or(&ApiVersion::V1) {
            ApiVersion::V0 => {
                let mut client = JsonRpcClient::new(channel);
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
                let mut client = rpc::v1::json::JsonRpcClient::new(channel);
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
