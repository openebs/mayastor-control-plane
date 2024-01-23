use crate::controller::{
    registry::Registry,
    resources::operations_helper::{ResourceSpecs, ResourceSpecsLocked},
};
use agents::errors::SvcError;
use grpc::operations::{
    frontend_node::traits::FrontendNodeRegisterInfo, PaginatedResult, Pagination,
};
use stor_port::{
    pstor::ObjectKey,
    types::v0::{
        store::frontend_node::{FrontendNodeSpec, FrontendNodeSpecKey},
        transport::{DeregisterFrontendNode, FrontendNodeId, RegisterFrontendNode},
    },
};

impl ResourceSpecs {
    /// Get a copy of all frontend node specs.
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

    /// Get a copy of all frontend node specs.
    pub(crate) fn frontend_node_specs(&self) -> Vec<FrontendNodeSpec> {
        let specs = self.read();
        specs.frontend_node_specs()
    }

    /// Get a frontend node spec using id.
    pub(crate) fn get_frontend_node_spec(&self, id: &FrontendNodeId) -> Option<FrontendNodeSpec> {
        let specs = self.read();
        specs.frontend_nodes.get(id).map(|spec| spec.lock().clone())
    }

    /// Remove the frontend node spec from registry.
    fn remove_frontend_node_spec(&self, id: &FrontendNodeId) {
        let mut specs = self.write();
        specs.frontend_nodes.remove(id);
    }

    /// Create a frontend node spec for the incoming request from csi instance.
    pub(crate) async fn register_frontend_node_spec(
        &self,
        registry: &Registry,
        req: &RegisterFrontendNode,
    ) -> Result<(), SvcError> {
        let (changed, spec_to_persist) = {
            let mut specs = self.write();
            match specs.frontend_nodes.get(&req.frontend_node_id()) {
                Some(frontend_node_rsc) => {
                    let mut frontend_node_spec = frontend_node_rsc.lock();
                    let changed = frontend_node_spec.endpoint != req.grpc_endpoint()
                        || frontend_node_spec.labels != req.labels();

                    frontend_node_spec.endpoint = req.grpc_endpoint();
                    (changed, frontend_node_spec.clone())
                }
                None => {
                    let frontend_node = FrontendNodeSpec::new(
                        req.frontend_node_id().clone(),
                        req.grpc_endpoint(),
                        req.labels.clone(),
                    );
                    specs.frontend_nodes.insert(frontend_node.clone());
                    (true, frontend_node)
                }
            }
        };
        if changed {
            registry.store_obj(&spec_to_persist).await?;
        }
        Ok(())
    }

    /// Delete a frontend node spec from the registry for the incoming request from csi instance.
    pub(crate) async fn deregister_frontend_node_spec(
        &self,
        registry: &Registry,
        req: &DeregisterFrontendNode,
    ) -> Result<(), SvcError> {
        registry
            .delete_kv(
                &<DeregisterFrontendNode as Into<FrontendNodeSpecKey>>::into(req.clone()).key(),
            )
            .await?;
        self.remove_frontend_node_spec(&req.id);
        Ok(())
    }
}
