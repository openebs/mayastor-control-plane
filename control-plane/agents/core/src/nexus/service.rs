use crate::core::registry::Registry;
use common::errors::SvcError;
use mbus_api::message_bus::v0::Nexuses;
use types::v0::message_bus::mbus::{
    AddNexusChild, Child, CreateNexus, DestroyNexus, Filter, GetNexuses, Nexus, RemoveNexusChild,
    ShareNexus, UnshareNexus,
};

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self { registry }
    }

    /// Get nexuses according to the filter
    #[tracing::instrument(level = "debug", err)]
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
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_nexus(&self, request: &CreateNexus) -> Result<Nexus, SvcError> {
        self.registry
            .specs
            .create_nexus(&self.registry, request)
            .await
    }

    /// Destroy nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_nexus(&self, request: &DestroyNexus) -> Result<(), SvcError> {
        self.registry
            .specs
            .destroy_nexus(&self.registry, request, true)
            .await
    }

    /// Share nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn share_nexus(&self, request: &ShareNexus) -> Result<String, SvcError> {
        self.registry
            .specs
            .share_nexus(&self.registry, request)
            .await
    }

    /// Unshare nexus
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unshare_nexus(&self, request: &UnshareNexus) -> Result<(), SvcError> {
        self.registry
            .specs
            .unshare_nexus(&self.registry, request)
            .await
    }

    /// Add nexus child
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn add_nexus_child(&self, request: &AddNexusChild) -> Result<Child, SvcError> {
        self.registry
            .specs
            .add_nexus_child(&self.registry, request)
            .await
    }

    /// Remove nexus child
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn remove_nexus_child(
        &self,
        request: &RemoveNexusChild,
    ) -> Result<(), SvcError> {
        self.registry
            .specs
            .remove_nexus_child(&self.registry, request)
            .await
    }
}
