use crate::controller::resources::operations_helper::{ResourceSpecs, ResourceSpecsLocked};
use grpc::operations::{PaginatedResult, Pagination};
use stor_port::types::v0::store::frontend_node::FrontendNodeSpec;

impl ResourceSpecs {
    /// Get all frontend node specs.
    pub(crate) fn frontend_node_specs(&self) -> Vec<FrontendNodeSpec> {
        self.frontend_nodes
            .values()
            .map(|v| v.lock().clone())
            .collect()
    }
    /// Get a subset of frontend node specs based on the pagination argument.
    pub(crate) fn paginated_frontend_node_specs(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<FrontendNodeSpec> {
        let mut last_result = false;
        let num_frontend_nodes = self.frontend_nodes.len() as u64;
        let max_entries = pagination.max_entries();
        let offset = std::cmp::min(pagination.starting_token(), num_frontend_nodes);

        let length = match offset + max_entries >= num_frontend_nodes {
            true => {
                last_result = true;
                num_frontend_nodes - offset
            }
            false => pagination.max_entries(),
        };

        PaginatedResult::new(self.frontend_nodes.paginate(offset, length), last_result)
    }
}
impl ResourceSpecsLocked {
    /// Get a subset of frontend nodes based on the pagination argument.
    pub(crate) fn paginated_frontend_node_specs(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<FrontendNodeSpec> {
        let specs = self.read();
        specs.paginated_frontend_node_specs(pagination)
    }
    /// Get all frontend node specs.
    pub(crate) fn frontend_node_specs(&self) -> Vec<FrontendNodeSpec> {
        let specs = self.read();
        specs.frontend_node_specs()
    }
}
