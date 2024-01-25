use crate::controller::registry::Registry;
use agents::errors::SvcError;
use grpc::operations::{PaginatedResult, Pagination};
use stor_port::{
    transport_api::ResourceKind,
    types::v0::transport::{AppNode, AppNodeId, AppNodeState},
};

impl Registry {
    /// Returns all app nodes from the registry.
    pub(super) fn app_nodes(&self) -> Vec<AppNode> {
        let app_node_specs = self.specs().app_node_specs();
        let mut app_nodes = Vec::with_capacity(app_node_specs.len());
        for spec in app_node_specs {
            let state = AppNodeState::from(spec.clone());
            app_nodes.push(AppNode::new(spec, Some(state)));
        }
        app_nodes
    }
    /// Returns the app nodes from the registry that match the given pagination.
    pub(super) fn paginated_app_nodes(&self, pagination: &Pagination) -> PaginatedResult<AppNode> {
        let app_node_specs = self.specs().paginated_app_node_specs(pagination);
        let mut app_nodes = Vec::with_capacity(app_node_specs.len());
        let last = app_node_specs.last();
        for spec in app_node_specs.result() {
            let state = AppNodeState::from(spec.clone());
            app_nodes.push(AppNode::new(spec, Some(state)));
        }
        PaginatedResult::new(app_nodes, last)
    }
    /// Gets the app node from the registry with the given id.
    pub(crate) fn app_node(&self, id: &AppNodeId) -> Result<AppNode, SvcError> {
        let Some(spec) = self.specs().app_node_spec(id) else {
            return Err(SvcError::NotFound {
                kind: ResourceKind::AppNode,
                id: id.to_string(),
            });
        };
        let app_node_state = AppNodeState::from(spec.clone());
        Ok(AppNode::new(spec, Some(app_node_state)))
    }
}
