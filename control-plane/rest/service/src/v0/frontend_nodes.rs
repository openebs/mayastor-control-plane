use crate::v0::{core_grpc, Body, Path, Query, RestApi, RestError, RestJsonError};
use grpc::operations::{
    frontend_node::traits::FrontendNodeOperations, MaxEntries, Pagination, StartingToken,
};
use rest_client::versions::v0::{
    apis, models,
    models::{FrontendNode, FrontendNodes, RegisterFrontendNode},
};
use std::net::AddrParseError;
use stor_port::{
    transport_api::{ReplyError, ResourceKind},
    types::v0::transport::{DeregisterFrontendNode, Filter},
};

fn client() -> impl FrontendNodeOperations {
    core_grpc().frontend_node()
}

#[async_trait::async_trait]
impl apis::actix_server::FrontendNodes for RestApi {
    async fn deregister_frontend_node(
        Path(frontend_node_id): Path<String>,
    ) -> Result<(), RestError<RestJsonError>> {
        client()
            .deregister_frontend_node(
                &DeregisterFrontendNode {
                    id: frontend_node_id.into(),
                },
                None,
            )
            .await?;
        Ok(())
    }

    async fn get_frontend_node(
        Path(frontend_node_id): Path<String>,
    ) -> Result<FrontendNode, RestError<RestJsonError>> {
        let frontend_node = client()
            .get(Filter::FrontendNode(frontend_node_id.into()), None)
            .await?;
        Ok(frontend_node.into())
    }

    async fn get_frontend_nodes(
        Query((max_entries, starting_token)): Query<(isize, Option<isize>)>,
    ) -> Result<FrontendNodes, RestError<RestJsonError>> {
        let starting_token = starting_token.unwrap_or_default();

        // If max entries is 0, pagination is disabled. All frontend nodes will be returned in a
        // single call.
        let pagination = if max_entries > 0 {
            Some(Pagination::new(
                max_entries as MaxEntries,
                starting_token as StartingToken,
            ))
        } else {
            None
        };
        let frontend_nodes = client().list(Filter::None, pagination, None).await?;
        Ok(models::FrontendNodes {
            entries: frontend_nodes
                .entries
                .into_iter()
                .map(|e| e.into())
                .collect(),
            next_token: frontend_nodes.next_token.map(|t| t as isize),
        })
    }

    async fn register_frontend_node(
        Path(frontend_node_id): Path<String>,
        Body(register_frontend_node): Body<RegisterFrontendNode>,
    ) -> Result<(), RestError<RestJsonError>> {
        client()
            .register_frontend_node(
                &stor_port::types::v0::transport::RegisterFrontendNode {
                    id: frontend_node_id.into(),
                    endpoint: register_frontend_node.endpoint.parse().map_err(
                        |error: AddrParseError| {
                            ReplyError::invalid_argument(
                                ResourceKind::FrontendNode,
                                "frontend_node.endpoint",
                                error.to_string(),
                            )
                        },
                    )?,
                    labels: register_frontend_node.labels,
                },
                None,
            )
            .await?;
        Ok(())
    }
}
