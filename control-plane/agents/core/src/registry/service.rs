use crate::{core, core::specs::ResourceSpecsLocked};
use common::errors::SvcError;
use common_lib::{
    transport_api::ReplyError,
    types::v0::transport::{GetSpecs, GetStates, Specs, States},
};
use grpc::{
    context::Context,
    operations::registry::traits::{GetSpecsInfo, GetStatesInfo, RegistryOperations},
};

/// Registry Service
#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: core::registry::Registry,
}

#[tonic::async_trait]
impl RegistryOperations for Service {
    async fn get_specs(
        &self,
        get_specs: &dyn GetSpecsInfo,
        _ctx: Option<Context>,
    ) -> Result<Specs, ReplyError> {
        let req = get_specs.into();
        let specs = self.get_specs(&req).await?;
        Ok(specs)
    }

    async fn get_states(
        &self,
        get_states: &dyn GetStatesInfo,
        _ctx: Option<Context>,
    ) -> Result<States, ReplyError> {
        let req = get_states.into();
        let states = self.get_states(&req).await?;
        Ok(states)
    }
}

impl Service {
    /// Invoke a new Registry Service
    pub(super) fn new(registry: core::registry::Registry) -> Self {
        Self { registry }
    }

    fn specs(&self) -> &ResourceSpecsLocked {
        self.registry.specs()
    }

    /// Get specs from the registry
    pub(crate) async fn get_specs(&self, _request: &GetSpecs) -> Result<Specs, SvcError> {
        let specs = self.specs().write();
        Ok(Specs {
            volumes: specs.get_volumes(),
            nexuses: specs.get_nexuses(),
            replicas: specs.get_replicas(),
            pools: specs.get_pools(),
        })
    }

    /// Get state information for all resources.
    pub(crate) async fn get_states(&self, _request: &GetStates) -> Result<States, SvcError> {
        let mut nexuses = vec![];
        let mut pools = vec![];
        let mut replicas = vec![];

        // Aggregate the state information from each node.
        let nodes = self.registry.nodes().read().await;
        for (_node_id, locked_node_wrapper) in nodes.iter() {
            let node_wrapper = locked_node_wrapper.read().await;
            nexuses.extend(node_wrapper.nexus_states());
            pools.extend(node_wrapper.pool_states());
            replicas.extend(node_wrapper.replica_states());
        }

        Ok(States {
            nexuses,
            pools,
            replicas,
        })
    }
}
