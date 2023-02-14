use crate::{controller, controller::resources::operations_helper::ResourceSpecsLocked};
use agents::errors::SvcError;
use grpc::{
    context::Context,
    operations::registry::traits::{GetSpecsInfo, GetStatesInfo, RegistryOperations},
};
use stor_port::{
    transport_api::ReplyError,
    types::v0::transport::{GetSpecs, GetStates, Specs, States},
};

/// Registry Service
#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: controller::registry::Registry,
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
    pub(super) fn new(registry: controller::registry::Registry) -> Self {
        Self { registry }
    }

    fn specs(&self) -> &ResourceSpecsLocked {
        self.registry.specs()
    }

    /// Get specs from the registry
    pub(crate) async fn get_specs(&self, _request: &GetSpecs) -> Result<Specs, SvcError> {
        let specs = self.specs().write();
        Ok(Specs {
            volumes: specs.volumes(),
            nexuses: specs.nexuses(),
            replicas: specs.replicas(),
            pools: specs.pools(),
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
            nexuses.extend(node_wrapper.nexus_states_cloned());
            pools.extend(node_wrapper.pool_states_cloned());
            replicas.extend(node_wrapper.replica_states_cloned());
        }

        Ok(States {
            nexuses,
            pools,
            replicas,
        })
    }
}
