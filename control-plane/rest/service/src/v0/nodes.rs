use super::*;
use grpc::operations::node::traits::NodeOperations;

fn client() -> impl NodeOperations {
    core_grpc().node()
}

#[async_trait::async_trait]
impl apis::actix_server::Nodes for RestApi {
    async fn get_node(Path(id): Path<String>) -> Result<models::Node, RestError<RestJsonError>> {
        let node = node(
            id.clone(),
            client()
                .get(Filter::Node(id.into()), None)
                .await?
                .into_inner()
                .get(0),
        )?;
        Ok(node.into())
    }

    async fn get_nodes() -> Result<Vec<models::Node>, RestError<RestJsonError>> {
        let nodes = client().get(Filter::None, None).await?;
        Ok(nodes.into_inner().into_vec())
    }

    async fn put_node_cordon(
        Path((id, label)): Path<(String, String)>,
    ) -> Result<models::Node, RestError<RestJsonError>> {
        let node = client().cordon(id.into(), label).await?;
        Ok(node.into())
    }

    async fn delete_node_cordon(
        Path((id, label)): Path<(String, String)>,
    ) -> Result<models::Node, RestError<RestJsonError>> {
        let node = client().uncordon(id.into(), label).await?;
        Ok(node.into())
    }
}

/// returns node from node option and returns an error on non existence
fn node(node_id: String, node: Option<&Node>) -> Result<Node, ReplyError> {
    match node {
        Some(node) => Ok(node.clone()),
        None => Err(ReplyError {
            kind: ReplyErrorKind::NotFound,
            resource: ResourceKind::Node,
            source: "Requested node was not found".to_string(),
            extra: format!("Node id : {}", node_id),
        }),
    }
}
