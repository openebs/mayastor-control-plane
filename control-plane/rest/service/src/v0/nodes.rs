use super::*;
use common_lib::types::v0::message_bus::{Node, NodeId};
use mbus_api::message_bus::v0::{MessageBus, MessageBusTrait};

pub(super) fn configure(cfg: &mut actix_web::web::ServiceConfig) {
    cfg.service(get_nodes).service(get_node);
}

#[get("/nodes")]
async fn get_nodes() -> Result<web::Json<Vec<Node>>, RestError> {
    RestRespond::result(MessageBus::get_nodes().await).map_err(RestError::from)
}
#[get("/nodes/{id}")]
async fn get_node(web::Path(node_id): web::Path<NodeId>) -> Result<web::Json<Node>, RestError> {
    RestRespond::result(MessageBus::get_node(&node_id).await)
}
