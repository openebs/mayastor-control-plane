use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceLifecycle, ResourceOffspring, ResourceSharing},
        operations_helper::ResourceSpecsLocked,
        OperationGuardArc,
    },
};
use agents::errors::SvcError;
use common_lib::{
    transport_api::{v0::Nexuses, ReplyError},
    types::v0::{
        store::nexus::NexusSpec,
        transport::{
            AddNexusChild, Child, CreateNexus, DestroyNexus, Filter, GetNexuses, Nexus,
            RemoveNexusChild, ShareNexus, UnshareNexus,
        },
    },
};
use grpc::{
    context::Context,
    operations::nexus::traits::{
        AddNexusChildInfo, CreateNexusInfo, DestroyNexusInfo, NexusOperations,
        RemoveNexusChildInfo, ShareNexusInfo, UnshareNexusInfo,
    },
};

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

#[tonic::async_trait]
impl NexusOperations for Service {
    async fn create(
        &self,
        nexus: &dyn CreateNexusInfo,
        _ctx: Option<Context>,
    ) -> Result<Nexus, ReplyError> {
        let req = nexus.into();
        let service = self.clone();
        let nexus = Context::spawn(async move { service.create_nexus(&req).await }).await??;
        Ok(nexus)
    }

    async fn get(&self, filter: Filter, _ctx: Option<Context>) -> Result<Nexuses, ReplyError> {
        let req = GetNexuses { filter };
        let nexuses = self.get_nexuses(&req).await?;
        Ok(nexuses)
    }

    async fn destroy(
        &self,
        req: &dyn DestroyNexusInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let destroy_nexus = req.into();
        let service = self.clone();
        Context::spawn(async move { service.destroy_nexus(&destroy_nexus).await }).await??;
        Ok(())
    }

    async fn share(
        &self,
        req: &dyn ShareNexusInfo,
        _ctx: Option<Context>,
    ) -> Result<String, ReplyError> {
        let share_nexus = req.into();
        let service = self.clone();
        let response =
            Context::spawn(async move { service.share_nexus(&share_nexus).await }).await??;
        Ok(response)
    }

    async fn unshare(
        &self,
        req: &dyn UnshareNexusInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let unshare_nexus = req.into();
        let service = self.clone();
        Context::spawn(async move { service.unshare_nexus(&unshare_nexus).await }).await??;
        Ok(())
    }

    async fn add_nexus_child(
        &self,
        req: &dyn AddNexusChildInfo,
        _ctx: Option<Context>,
    ) -> Result<Child, ReplyError> {
        let add_nexus_child = req.into();
        let service = self.clone();
        let child = Context::spawn(async move { service.add_nexus_child(&add_nexus_child).await })
            .await??;
        Ok(child)
    }

    async fn remove_nexus_child(
        &self,
        req: &dyn RemoveNexusChildInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let unshare_nexus = req.into();
        let service = self.clone();
        Context::spawn(async move { service.remove_nexus_child(&unshare_nexus).await }).await??;
        Ok(())
    }
}
impl Service {
    /// Return new `Self`.
    pub(super) fn new(registry: Registry) -> Self {
        Self { registry }
    }
    fn specs(&self) -> &ResourceSpecsLocked {
        self.registry.specs()
    }

    /// Get nexuses according to the filter
    #[tracing::instrument(level = "info", skip(self), err)]
    pub(super) async fn get_nexuses(&self, request: &GetNexuses) -> Result<Nexuses, SvcError> {
        let filter = request.filter.clone();
        let nexuses = match filter {
            Filter::None => self.registry.node_opt_nexuses(None).await?,
            Filter::Node(node_id) => self.registry.node_nexuses(&node_id).await?,
            Filter::NodeNexus(node_id, nexus_id) => {
                let nexus = self.registry.node_nexus(&node_id, &nexus_id).await?;
                vec![nexus]
            }
            Filter::Nexus(nexus_id) => {
                let nexus = self.registry.nexus(&nexus_id).await?;
                vec![nexus]
            }
            _ => return Err(SvcError::InvalidFilter { filter }),
        };
        Ok(Nexuses(nexuses))
    }

    /// Create nexus using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.uuid))]
    pub(super) async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError> {
        OperationGuardArc::<NexusSpec>::create(&self.registry, request)
            .await
            .map(|(_, nexus)| nexus)
    }

    /// Destroy a nexus using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.uuid))]
    pub(super) async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError> {
        let mut nexus = self.specs().nexus_opt(&request.uuid).await?;
        nexus.as_mut().destroy(&self.registry, request).await
    }

    /// Share a nexus using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.uuid))]
    pub(super) async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError> {
        let mut nexus = self.specs().nexus_opt(&request.uuid).await?;
        nexus.as_mut().share(&self.registry, request).await
    }

    /// Unshare a nexus using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.uuid))]
    pub(super) async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<(), SvcError> {
        let mut nexus = self.specs().nexus_opt(&request.uuid).await?;
        nexus.as_mut().unshare(&self.registry, request).await
    }

    /// Add a nexus child using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.nexus))]
    pub(super) async fn add_nexus_child(&self, request: &AddNexusChild) -> Result<Child, SvcError> {
        let mut nexus = self.specs().nexus_opt(&request.nexus).await?;
        nexus.as_mut().add_child(&self.registry, request).await
    }

    /// Remove a nexus child using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.nexus))]
    pub(super) async fn remove_nexus_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        let mut nexus = self.specs().nexus_opt(&request.nexus).await?;
        nexus.as_mut().remove_child(&self.registry, request).await
    }
}
