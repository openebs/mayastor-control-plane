use super::*;
use grpc::operations::{
    app_node::traits::AppNodeOperations, MaxEntries, Pagination, StartingToken,
};
use rest_client::versions::v0::models::{AppNode, AppNodes, RegisterAppNode};
use std::net::AddrParseError;
use stor_port::types::v0::transport::DeregisterAppNode;

fn client() -> impl AppNodeOperations {
    core_grpc().app_node()
}

#[async_trait::async_trait]
impl apis::actix_server::AppNodes for RestApi {
    async fn deregister_app_node(
        Path(app_node_id): Path<String>,
    ) -> Result<(), RestError<RestJsonError>> {
        client()
            .deregister_app_node(
                &DeregisterAppNode {
                    id: app_node_id.into(),
                },
                None,
            )
            .await?;
        Ok(())
    }

    async fn get_app_node(
        Path(app_node_id): Path<String>,
    ) -> Result<AppNode, RestError<RestJsonError>> {
        let app_node = client()
            .get(Filter::AppNode(app_node_id.into()), None)
            .await?;
        Ok(app_node.into())
    }

    async fn get_app_nodes(
        Query((max_entries, starting_token)): Query<(isize, Option<isize>)>,
    ) -> Result<AppNodes, RestError<RestJsonError>> {
        let starting_token = starting_token.unwrap_or_default();

        // If max entries is 0, pagination is disabled. All app nodes will be returned in a
        // single call.
        let pagination = if max_entries > 0 {
            Some(Pagination::new(
                max_entries as MaxEntries,
                starting_token as StartingToken,
            ))
        } else {
            None
        };
        let app_nodes = client().list(pagination, None).await?;
        Ok(models::AppNodes {
            entries: app_nodes.entries.into_iter().map(|e| e.into()).collect(),
            next_token: app_nodes.next_token.map(|t| t as isize),
        })
    }

    async fn register_app_node(
        Path(app_node_id): Path<String>,
        Body(register_app_node): Body<RegisterAppNode>,
    ) -> Result<(), RestError<RestJsonError>> {
        client()
            .register_app_node(
                &stor_port::types::v0::transport::RegisterAppNode {
                    id: app_node_id.into(),
                    endpoint: register_app_node.endpoint.parse().map_err(
                        |error: AddrParseError| {
                            ReplyError::invalid_argument(
                                ResourceKind::AppNode,
                                "app_node.endpoint",
                                error.to_string(),
                            )
                        },
                    )?,
                    labels: register_app_node.labels,
                },
                None,
            )
            .await?;
        Ok(())
    }
}
