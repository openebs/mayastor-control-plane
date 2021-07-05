use super::*;
use actix_web::web::Path;
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};

#[async_trait::async_trait]
impl apis::NodesApi for RestApi {
    async fn get_node(
        Path(id): Path<String>,
    ) -> Result<Json<models::Node>, RestError<RestJsonError>> {
        let node = MessageBus::get_node(&id.into()).await?;
        Ok(Json(node.into()))
    }

    async fn get_nodes() -> Result<Json<Vec<models::Node>>, RestError<RestJsonError>> {
        let nodes = MessageBus::get_nodes().await?;
        Ok(Json(nodes.iter().map(models::Node::from).collect()))
    }
}
