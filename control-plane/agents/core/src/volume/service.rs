use crate::core::registry::Registry;
use common::errors::SvcError;
use mbus_api::v0::{
    AddNexusChild,
    Child,
    CreateNexus,
    CreateVolume,
    DestroyNexus,
    DestroyVolume,
    Filter,
    GetNexuses,
    GetVolumes,
    Nexus,
    Nexuses,
    RemoveNexusChild,
    ShareNexus,
    UnshareNexus,
    Volume,
    Volumes,
};

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self {
            registry,
        }
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
            _ => {
                return Err(SvcError::InvalidFilter {
                    filter,
                })
            }
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

    /// Get volumes
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_volumes(&self, request: &GetVolumes) -> Result<Volumes, SvcError> {
        let nexuses = self.get_nexuses(&Default::default()).await?.0;
        let nexus_specs = self.registry.specs.get_created_nexus_specs().await;
        let volumes = nexuses
            .iter()
            .map(|nexus| {
                let uuid = nexus_specs
                    .iter()
                    .find(|nexus_spec| nexus_spec.uuid == nexus.uuid)
                    .map(|nexus_spec| nexus_spec.owner.clone())
                    .flatten();
                if let Some(uuid) = uuid {
                    Some(Volume {
                        uuid,
                        size: nexus.size,
                        // ANA not supported so derive volume state from the
                        // single Nexus
                        state: nexus.state.clone(),
                        children: vec![nexus.clone()],
                    })
                } else {
                    None
                }
            })
            .flatten()
            .collect::<Vec<_>>();

        let volumes = match &request.filter {
            Filter::None => volumes,
            Filter::NodeVolume(node, volume) => volumes
                .iter()
                .filter(|volume_iter| {
                    volume_iter.children.iter().any(|c| &c.node == node)
                        && &volume_iter.uuid == volume
                })
                .cloned()
                .collect(),
            Filter::Volume(volume) => volumes
                .iter()
                .filter(|volume_iter| &volume_iter.uuid == volume)
                .cloned()
                .collect(),
            filter => {
                return Err(SvcError::InvalidFilter {
                    filter: filter.clone(),
                })
            }
        };
        Ok(Volumes(volumes))
    }

    /// Create volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_volume(&self, request: &CreateVolume) -> Result<Volume, SvcError> {
        self.registry
            .specs
            .create_volume(&self.registry, request)
            .await
    }

    /// Destroy volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_volume(&self, request: &DestroyVolume) -> Result<(), SvcError> {
        self.registry
            .specs
            .destroy_volume(&self.registry, request)
            .await
    }
}
