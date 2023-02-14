use super::*;
use grpc::operations::node::traits::NodeOperations;
use stor_port::types::v0::transport::GetBlockDevices;

fn client() -> impl NodeOperations {
    core_grpc().node()
}

#[async_trait::async_trait]
impl apis::actix_server::BlockDevices for RestApi {
    // Get block devices takes a query parameter 'all' which is used to determine
    // whether to return all found devices or only those that are usable.
    // Omitting the query parameter will result in all block devices being shown.
    //
    // # Examples
    // Get only usable block devices with query parameter:
    //      curl -X GET "https://localhost:8080/v0/nodes/io-engine/block_devices?all=false" \
    //      -H  "accept: application/json"
    //
    // Get all block devices with query parameter:
    //      curl -X GET "https://localhost:8080/v0/nodes/io-engine/block_devices?all=true" \
    //      -H  "accept: application/json" -k
    //
    // Get all block devices without query parameter:
    //      curl -X GET "https://localhost:8080/v0/nodes/io-engine/block_devices" \
    //      -H  "accept: application/json" -k
    //
    async fn get_node_block_devices(
        Path(node): Path<String>,
        Query(all): Query<Option<bool>>,
    ) -> Result<Vec<models::BlockDevice>, RestError<RestJsonError>> {
        let devices = client()
            .get_block_devices(
                &GetBlockDevices {
                    node: node.into(),
                    all: all.unwrap_or(true),
                },
                None,
            )
            .await?;
        Ok(devices.into_inner().into_iter().map(From::from).collect())
    }
}
