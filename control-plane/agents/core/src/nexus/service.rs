use crate::core::{registry::Registry, specs::ResourceSpecsLocked};
use common::errors::SvcError;
use common_lib::{
    mbus_api::{v0::Nexuses, ReplyError},
    types::v0::{
        message_bus::{
            AddNexusChild, Child, CreateNexus, DestroyNexus, Filter, GetNexuses, Nexus,
            RemoveNexusChild, ShareNexus, UnshareNexus,
        },
        store::OperationMode,
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
            Filter::None => self.registry.get_node_opt_nexuses(None).await?,
            Filter::Node(node_id) => self.registry.get_node_nexuses(&node_id).await?,
            Filter::NodeNexus(node_id, nexus_id) => {
                let nexus = self.registry.get_node_nexus(&node_id, &nexus_id).await?;
                vec![nexus]
            }
            Filter::Nexus(nexus_id) => {
                let nexus = self.registry.get_nexus(&nexus_id).await?;
                vec![nexus]
            }
            _ => return Err(SvcError::InvalidFilter { filter }),
        };
        Ok(Nexuses(nexuses))
    }

    /// Create nexus
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.uuid))]
    pub(super) async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError> {
        self.specs()
            .create_nexus(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Destroy nexus
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.uuid))]
    pub(super) async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError> {
        self.specs()
            .destroy_nexus(&self.registry, request, true, OperationMode::Exclusive)
            .await
    }

    /// Share nexus
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.uuid))]
    pub(super) async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError> {
        self.specs()
            .share_nexus(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Unshare nexus
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.uuid))]
    pub(super) async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<(), SvcError> {
        self.specs()
            .unshare_nexus(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Add nexus child
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.nexus))]
    pub(super) async fn add_nexus_child(&self, request: &AddNexusChild) -> Result<Child, SvcError> {
        self.specs()
            .add_nexus_child(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Remove nexus child
    #[tracing::instrument(level = "info", skip(self), err, fields(nexus.uuid = %request.nexus))]
    pub(super) async fn remove_nexus_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        self.specs()
            .remove_nexus_child(&self.registry, request, OperationMode::Exclusive)
            .await
    }
}
