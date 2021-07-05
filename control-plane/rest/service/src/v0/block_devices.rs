use super::*;
use actix_web::web::Path;
use common_lib::types::v0::message_bus::GetBlockDevices;
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};

#[async_trait::async_trait]
impl apis::BlockDevicesApi for RestApi {
    // Get block devices takes a query parameter 'all' which is used to determine
    // whether to return all found devices or only those that are usable.
    // Omitting the query parameter will result in all block devices being shown.
    //
    // # Examples
    // Get only usable block devices with query parameter:
    //      curl -X GET "https://localhost:8080/v0/nodes/mayastor/block_devices?all=false" \
    //      -H  "accept: application/json"
    //
    // Get all block devices with query parameter:
    //      curl -X GET "https://localhost:8080/v0/nodes/mayastor/block_devices?all=true" \
    //      -H  "accept: application/json" -k
    //
    // Get all block devices without query parameter:
    //      curl -X GET "https://localhost:8080/v0/nodes/mayastor/block_devices" \
    //      -H  "accept: application/json" -k
    //
    async fn get_node_block_devices(
        Path(node): Path<String>,
        all: Option<bool>,
    ) -> Result<Json<Vec<models::BlockDevice>>, RestError<RestJsonError>> {
        let devices = MessageBus::get_block_devices(GetBlockDevices {
            node: node.into(),
            all: all.unwrap_or(true),
        })
        .await?;
        Ok(Json(
            devices.into_inner().into_iter().map(From::from).collect(),
        ))
    }
}
