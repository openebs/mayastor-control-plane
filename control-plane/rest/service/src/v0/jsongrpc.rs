//! Provides a REST interface to interact with JSON gRPC methods.
//! These methods are typically used to control SPDK directly.

use super::*;
use actix_web::web::Path;
use common_lib::types::v0::message_bus::JsonGrpcRequest;
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};
use serde_json::Value;

#[async_trait::async_trait]
impl apis::JsonGrpcApi for RestApi {
    // A PUT request is required so that method parameters can be passed in the
    // body.
    //
    // # Example
    // To create a malloc bdev:
    // ```
    //  curl -X PUT "https://localhost:8080/v0/nodes/mayastor/jsongrpc/bdev_malloc_create" \
    //  -H "accept: application/json" -H "Content-Type: application/json" \
    //  -d '{"block_size": 512, "num_blocks": 64, "name": "Malloc0"}'
    // ```
    async fn put_node_jsongrpc(
        web::Path((node, method)): Path<(String, String)>,
        web::Json(body): Json<Value>,
    ) -> Result<Json<Value>, RestError<RestJsonError>> {
        let result = MessageBus::json_grpc_call(JsonGrpcRequest {
            node: node.into(),
            method: method.into(),
            params: body.to_string().into(),
        })
        .await?;
        Ok(Json(result))
    }
}
