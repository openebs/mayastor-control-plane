use super::*;
use grpc::operations::node::traits::NodeOperations;
// use rest_client::versions::v0::models::NodeDeleteResult;
// use stor_port::types::v0::transport::DestroyNode;

fn client() -> impl NodeOperations {
    core_grpc().node()
}

#[async_trait::async_trait]
impl apis::actix_server::Nodes for RestApi {
    async fn del_node(
        Path(_id): Path<String>,
        Body(body): Body<Option<models::DeleteNodeBody>>,
    ) -> Result<models::NodeDeleteResult, RestError<RestJsonError>> {
        let _body = body.unwrap_or_default();
        // let request = DestroyNode::new(id.into())
        //     .with_purge(body.purge.unwrap_or(false))
        //     .with_accept(body.accept.unwrap_or(false))
        //     .with_accept_volume_loss(body.accept_volume_loss.unwrap_or(false))
        //     .with_accept_snapshot_loss(body.accept_snapshot_loss.unwrap_or(false));

        // let result = client().delete(&request).await?;
        // Ok(result.into())
        Ok(models::NodeDeleteResult::default())
    }

    async fn get_node(Path(id): Path<String>) -> Result<models::Node, RestError<RestJsonError>> {
        let node = node(
            id.clone(),
            client()
                .get(Filter::Node(id.into()), false, None)
                .await?
                .into_inner()
                .first(),
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
        Path((id, key, value)): Path<(String, String, String)>,
        Query(overwrite): Query<Option<bool>>,
    ) -> Result<models::Node, RestError<RestJsonError>> {
        let overwrite = overwrite.unwrap_or(false);
        let node = client()
            .label(id.into(), [(key, value)].into(), overwrite)
            .await?;
        Ok(node.into())
    }

    async fn delete_node_label(
        Path((id, label_key)): Path<(String, String)>,
    ) -> Result<models::Node, RestError<RestJsonError>> {
        let node = client().unlabel(id.into(), label_key).await?;
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
