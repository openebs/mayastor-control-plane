//! Provides a REST interface to interact with JSON gRPC methods.
//! These methods are typically used to control SPDK directly.

use super::*;
use common_lib::types::v0::message_bus::JsonGrpcRequest;
use grpc::operations::jsongrpc::traits::JsonGrpcOperations;
use serde_json::Value;

#[async_trait::async_trait]
impl apis::actix_server::JsonGrpc for RestApi {
    // A PUT request is required so that method parameters can be passed in the
    // body.
    //
    // # Example
    // To create a malloc bdev:
    // ```
    //  curl -X PUT "https://localhost:8080/v0/nodes/io-engine/jsongrpc/bdev_malloc_create" \
    //  -H "accept: application/json" -H "Content-Type: application/json" \
    //  -d '{"block_size": 512, "num_blocks": 64, "name": "Malloc0"}'
    // ```
    async fn put_node_jsongrpc(
        Path((node, method)): Path<(String, String)>,
        Body(body): Body<Value>,
    ) -> Result<Value, RestError<RestJsonError>> {
        let result = json_grpc()?
            .call(
                &JsonGrpcRequest {
                    node: node.into(),
                    method: method.into(),
                    params: body.to_string().into(),
                },
                None,
            )
            .await?;
        Ok(result)
    }
}
