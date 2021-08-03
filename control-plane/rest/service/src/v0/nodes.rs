use super::*;
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};

#[async_trait::async_trait]
impl apis::Nodes for RestApi {
    async fn get_node(Path(id): Path<String>) -> Result<models::Node, RestError<RestJsonError>> {
        let node = MessageBus::get_node(&id.into()).await?;
        Ok(node.into())
    }

    async fn get_nodes() -> Result<Vec<models::Node>, RestError<RestJsonError>> {
        let nodes = MessageBus::get_nodes().await?;
        Ok(nodes.into_vec())
    }
}
