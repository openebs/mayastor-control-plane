use crate::{core, core::specs::ResourceSpecsLocked};
use common::errors::SvcError;
use common_lib::{
    mbus_api::ReplyError,
    types::v0::message_bus::{GetSpecs, Specs},
};
use grpc::{
    context::Context,
    operations::registry::traits::{GetSpecsInfo, RegistryOperations},
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
        get_spec: &dyn GetSpecsInfo,
        _ctx: Option<Context>,
    ) -> Result<Specs, ReplyError> {
        let req = get_spec.into();
        let specs = self.get_specs(&req).await?;
        Ok(specs)
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
}
