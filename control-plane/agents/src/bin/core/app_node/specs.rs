use crate::controller::{
    registry::Registry,
    resources::operations_helper::{ResourceSpecs, ResourceSpecsLocked},
};
use agents::errors::SvcError;
use grpc::operations::{app_node::traits::AppNodeRegisterInfo, PaginatedResult, Pagination};
use stor_port::{
    pstor::ObjectKey,
    types::v0::{
        store::app_node::{AppNodeSpec, AppNodeSpecKey},
        transport::{AppNodeId, DeregisterAppNode, RegisterAppNode},
    },
};

impl ResourceSpecs {
    /// Get a copy of all app node specs.
    pub(crate) fn app_node_specs(&self) -> Vec<AppNodeSpec> {
        self.app_nodes.values().map(|v| v.lock().clone()).collect()
    }
    /// Get a subset of app node specs based on the pagination argument.
    pub(crate) fn paginated_app_node_specs(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<AppNodeSpec> {
        let mut last_result = false;
        let num_app_nodes = self.app_nodes.len() as u64;
        let max_entries = pagination.max_entries();
        let offset = std::cmp::min(pagination.starting_token(), num_app_nodes);

        let length = match offset + max_entries >= num_app_nodes {
            true => {
                last_result = true;
                num_app_nodes - offset
            }
            false => pagination.max_entries(),
        };

        PaginatedResult::new(self.app_nodes.paginate(offset, length), last_result)
    }
}
impl ResourceSpecsLocked {
    /// Get a subset of app nodes based on the pagination argument.
    pub(crate) fn paginated_app_node_specs(
        &self,
        pagination: &Pagination,
    ) -> PaginatedResult<AppNodeSpec> {
        let specs = self.read();
        specs.paginated_app_node_specs(pagination)
    }

    /// Get a copy of all app node specs.
    pub(crate) fn app_node_specs(&self) -> Vec<AppNodeSpec> {
        let specs = self.read();
        specs.app_node_specs()
    }

    /// Get an app node spec using id.
    pub(crate) fn app_node_spec(&self, id: &AppNodeId) -> Option<AppNodeSpec> {
        let specs = self.read();
        specs.app_nodes.get(id).map(|spec| spec.lock().clone())
    }

    /// Remove the app node spec from registry.
    fn remove_app_node_spec(&self, id: &AppNodeId) {
        let mut specs = self.write();
        specs.app_nodes.remove(id);
    }

    /// Create an app node spec for the incoming request from csi instance.
    pub(crate) async fn register_app_node(
        &self,
        registry: &Registry,
        req: &RegisterAppNode,
    ) -> Result<(), SvcError> {
        let (changed, spec_to_persist) = {
            let mut specs = self.write();
            match specs.app_nodes.get(&req.app_node_id()) {
                Some(app_node_rsc) => {
                    let mut app_node_spec = app_node_rsc.lock();
                    let changed = app_node_spec.endpoint != req.grpc_endpoint()
                        || app_node_spec.labels != req.labels();

                    app_node_spec.endpoint = req.grpc_endpoint();
                    (changed, app_node_spec.clone())
                }
                None => {
                    let app_node = AppNodeSpec::new(
                        req.app_node_id().clone(),
                        req.grpc_endpoint(),
                        req.labels.clone(),
                    );
                    specs.app_nodes.insert(app_node.clone());
                    (true, app_node)
                }
            }
        };
        if changed {
            registry.store_obj(&spec_to_persist).await?;
        }
        Ok(())
    }

    /// Delete an app node spec from the registry for the incoming request from csi instance.
    pub(crate) async fn deregister_app_node(
        &self,
        registry: &Registry,
        req: &DeregisterAppNode,
    ) -> Result<(), SvcError> {
        registry
            .delete_kv(&<DeregisterAppNode as Into<AppNodeSpecKey>>::into(req.clone()).key())
            .await?;
        self.remove_app_node_spec(&req.id);
        Ok(())
    }
}
