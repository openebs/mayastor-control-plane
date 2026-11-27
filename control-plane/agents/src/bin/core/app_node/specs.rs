use crate::controller::{
    registry::Registry,
    resources::operations_helper::{ResourceSpecs, ResourceSpecsLocked},
};
use agents::errors::SvcError;
use grpc::operations::{PaginatedResult, Pagination};
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

    /// An RPC may time out after etcd commits. Only confirmed cache entries can skip a write.
    fn mark_app_node_write(&self, id: &AppNodeId) {
        let mut specs = self.write();
        if specs.app_nodes.get(id).is_some() {
            specs.unconfirmed_app_nodes.insert(id.clone());
        }
    }

    /// Create an app node spec for the incoming request from csi instance.
    pub(crate) async fn register_app_node(
        &self,
        registry: &Registry,
        req: &RegisterAppNode,
    ) -> Result<(), SvcError> {
        let _registration = registry.registration_guard(req.id.as_str()).await;
        let labels = {
            let specs = self.read();
            if let Some(node) = specs.app_nodes.get(&req.id) {
                let spec = node.lock();
                if spec.endpoint == req.endpoint
                    && spec.labels == req.labels
                    && spec.transport_caps == req.transport_caps
                    && !specs.unconfirmed_app_nodes.contains(&req.id)
                {
                    return Ok(());
                }
                spec.labels.clone()
            } else {
                req.labels.clone()
            }
        };
        let spec = AppNodeSpec::new(
            req.id.clone(),
            req.endpoint,
            labels,
            req.transport_caps.clone(),
        );
        self.mark_app_node_write(&req.id);
        registry.store_obj(&spec).await?;
        let mut specs = self.write();
        specs.app_nodes.insert(spec);
        specs.unconfirmed_app_nodes.remove(&req.id);
        Ok(())
    }

    /// Delete an app node spec from the registry for the incoming request from csi instance.
    pub(crate) async fn deregister_app_node(
        &self,
        registry: &Registry,
        req: &DeregisterAppNode,
    ) -> Result<(), SvcError> {
        let _registration = registry.registration_guard(req.id.as_str()).await;
        self.mark_app_node_write(&req.id);
        registry
            .delete_kv(&AppNodeSpecKey::from(&req.id).key())
            .await?;
        let mut specs = self.write();
        specs.app_nodes.remove(&req.id);
        specs.unconfirmed_app_nodes.remove(&req.id);
        Ok(())
    }
}
