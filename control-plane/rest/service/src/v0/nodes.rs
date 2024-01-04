use std::collections::HashMap;

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
                .get(Filter::Node(id.into()), false, None)
                .await?
                .into_inner()
                .get(0),
        )?;
        Ok(node.into())
    }

    async fn get_nodes(
        Query(node_id): Query<Option<String>>,
    ) -> Result<Vec<models::Node>, RestError<RestJsonError>> {
        match node_id {
            Some(node_id) => {
                let nodes = client()
                    .get(Filter::Node(node_id.into()), true, None)
                    .await?;
                Ok(nodes.into_inner().into_vec())
            }
            None => {
                let nodes = client().get(Filter::None, false, None).await?;
                Ok(nodes.into_inner().into_vec())
            }
        }
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

    async fn put_node_drain(
        Path((id, label)): Path<(String, String)>,
    ) -> Result<models::Node, RestError<RestJsonError>> {
        let node = client().drain(id.into(), label).await?;
        Ok(node.into())
    }

    async fn put_node_label(
        Path((id, label)): Path<(String, String)>,
        Query(overwrite): Query<Option<bool>>,
    ) -> Result<models::Node, RestError<RestJsonError>> {
        let mut label_map = HashMap::new();
        let mut iter = label.split('=');
        let overwrite = overwrite.unwrap_or(false);

        if let (Some(key), Some(value)) = (iter.next(), iter.next()) {
            label_map.insert(key.trim().to_string(), value.trim().to_string());
        } else {
            return Err(RestError::from(ReplyError {
                kind: ReplyErrorKind::Internal,
                resource: ResourceKind::Node,
                source: "put_node_label".to_string(),
                extra: format!("invalid label for node resource {}", label),
            }));
        }
        let node = client().label(id.into(), label_map, overwrite).await?;
        Ok(node.into())
    }

    async fn delete_node_label(
        Path((id, label)): Path<(String, String)>,
    ) -> Result<models::Node, RestError<RestJsonError>> {
        let node = client().unlabel(id.into(), label).await?;
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
            extra: format!("Node id : {node_id}"),
        }),
    }
}
