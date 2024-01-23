use crate::controller::registry::Registry;
use agents::errors::SvcError;
use grpc::operations::{PaginatedResult, Pagination};
use stor_port::{
    transport_api::ResourceKind,
    types::v0::transport::{FrontendNode, FrontendNodeId, FrontendNodeState},
};

impl Registry {
    /// Returns all frontend nodes from the registry.
    pub(super) fn frontend_nodes(&self) -> Vec<FrontendNode> {
        let frontend_node_specs = self.specs().frontend_node_specs();
        let mut frontend_nodes = Vec::with_capacity(frontend_node_specs.len());
        for spec in frontend_node_specs {
            let state = FrontendNodeState::from(spec.clone());
            frontend_nodes.push(FrontendNode::new(spec, Some(state)));
        }
        frontend_nodes
    }
    /// Returns the frontend nodes from the registry that match the given pagination.
    pub(super) fn paginated_frontend_nodes(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<FrontendNode> {
        let frontend_node_specs = self.specs().paginated_frontend_node_specs(pagination);
        let mut frontend_nodes = Vec::with_capacity(frontend_node_specs.len());
        let last = frontend_node_specs.last();
        for spec in frontend_node_specs.result() {
            let state = FrontendNodeState::from(spec.clone());
            frontend_nodes.push(FrontendNode::new(spec, Some(state)));
        }
        PaginatedResult::new(frontend_nodes, last)
    }
    /// Gets the frontend node from the registry with the given id.
    pub(crate) fn frontend_node(&self, id: &FrontendNodeId) -> Result<FrontendNode, SvcError> {
        let Some(spec) = self.specs().get_frontend_node_spec(id) else {
            return Err(SvcError::NotFound {
                kind: ResourceKind::FrontendNode,
                id: id.to_string(),
            });
        };
        let frontend_node_state = FrontendNodeState::from(spec.clone());
        Ok(FrontendNode::new(spec, Some(frontend_node_state)))
    }
}
