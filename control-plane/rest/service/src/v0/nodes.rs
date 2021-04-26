use super::*;

pub(super) fn configure(cfg: &mut paperclip::actix::web::ServiceConfig) {
    cfg.service(get_nodes).service(get_node);
}

#[get("/nodes", tags(Nodes))]
async fn get_nodes() -> Result<web::Json<Vec<Node>>, RestClusterError> {
    RestRespond::result(MessageBus::get_nodes().await).map_err(RestClusterError::from)
}
#[get("/nodes/{id}", tags(Nodes))]
async fn get_node(web::Path(node_id): web::Path<NodeId>) -> Result<web::Json<Node>, RestError> {
    RestRespond::result(MessageBus::get_node(&node_id).await)
}
