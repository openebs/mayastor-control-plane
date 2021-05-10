use crate::core::registry::Registry;
use common::errors::SvcError;
use mbus_api::v0::{CreateVolume, DestroyVolume, Filter, GetVolumes, Volume, Volumes};

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self { registry }
    }

    /// Get volumes
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_volumes(&self, request: &GetVolumes) -> Result<Volumes, SvcError> {
        let nexuses = self.registry.get_node_opt_nexuses(None).await?;
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
