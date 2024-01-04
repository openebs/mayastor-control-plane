use crate::{error::SvcError, registry::Registry};
use pstor_proxy::types::{
    frontend_node::{DeregisterFrontendNode, FrontendNodes, RegisterFrontendNode},
    misc::{Filter, Pagination},
};

#[derive(Debug, Clone)]
pub(crate) struct Service {
    registry: Registry,
}

impl Service {
    pub(crate) fn new(registry: Registry) -> Self {
        Self { registry }
    }

    /// Register a frontend node.
    pub(crate) async fn register_frontend_node(
        &self,
        frontend_node_spec: &RegisterFrontendNode,
    ) -> Result<(), SvcError> {
        self.registry
            .register_frontend_node(frontend_node_spec)
            .await
    }

    /// Deregister a frontend node.
    pub(crate) async fn deregister_frontend_node(
        &self,
        frontend_node_spec: &DeregisterFrontendNode,
    ) -> Result<(), SvcError> {
        self.registry
            .deregister_frontend_node(frontend_node_spec)
            .await
    }

    /// Register one or multiple frontend node.
    pub(super) async fn get_frontend_nodes(
        &self,
        filter: Filter,
        ignore_notfound: bool,
        pagination: Option<Pagination>,
    ) -> Result<FrontendNodes, SvcError> {
        // The last result can only ever be false if using pagination.
        let mut last_result = true;

        let filtered_frontend_nodes = match filter {
            Filter::None => match &pagination {
                Some(p) => {
                    let paginated_frontend_nodes = self.registry.paginated_frontend_nodes(p).await;
                    last_result = paginated_frontend_nodes.last();
                    paginated_frontend_nodes.result()
                }
                None => self.registry.frontend_nodes().await,
            },
            Filter::FrontendNode(frontend_node_id) => {
                match self.registry.frontend_node(&frontend_node_id).await {
                    Ok(frontend_node) => Ok(vec![frontend_node]),
                    Err(SvcError::FrontendNodeNotFound { .. }) if ignore_notfound => Ok(vec![]),
                    Err(error) => Err(error),
                }?
            }
        };

        Ok(FrontendNodes {
            entries: filtered_frontend_nodes,
            next_token: match last_result {
                true => None,
                false => pagination.map(|p| p.starting_token() + p.max_entries()),
            },
        })
    }
}
