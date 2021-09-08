use crate::core::{registry::Registry, specs::ResourceSpecsLocked};
use common::errors::SvcError;
use common_lib::{
    mbus_api::message_bus::v0::Nexuses,
    types::v0::{
        message_bus::{
            AddNexusChild, Child, CreateNexus, DestroyNexus, Filter, GetNexuses, Nexus,
            RemoveNexusChild, ShareNexus, UnshareNexus,
        },
        store::OperationMode,
    },
};

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
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
