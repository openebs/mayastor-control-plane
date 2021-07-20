use crate::core::registry::Registry;
use common::errors::SvcError;
use common_lib::{
    mbus_api::message_bus::v0::Volumes,
    types::v0::message_bus::{
        CreateVolume, DestroyVolume, Filter, GetVolumes, PublishVolume, SetVolumeReplica,
        ShareVolume, UnpublishVolume, UnshareVolume, Volume,
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

    /// Get volumes
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_volumes(&self, request: &GetVolumes) -> Result<Volumes, SvcError> {
        let volumes = self.registry.get_volumes_status().await;

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

    /// Share volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn share_volume(&self, request: &ShareVolume) -> Result<String, SvcError> {
        self.registry
            .specs
            .share_volume(&self.registry, request)
            .await
    }

    /// Unshare volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unshare_volume(&self, request: &UnshareVolume) -> Result<(), SvcError> {
        self.registry
            .specs
            .unshare_volume(&self.registry, request)
            .await
    }

    /// Publish volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn publish_volume(&self, request: &PublishVolume) -> Result<String, SvcError> {
        self.registry
            .specs
            .publish_volume(&self.registry, request)
            .await
    }

    /// Unpublish volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unpublish_volume(&self, request: &UnpublishVolume) -> Result<(), SvcError> {
        self.registry
            .specs
            .unpublish_volume(&self.registry, request)
            .await
    }

    /// Set volume replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn set_volume_replica(
        &self,
        request: &SetVolumeReplica,
    ) -> Result<Volume, SvcError> {
        self.registry
            .specs
            .set_volume_replica(&self.registry, request)
            .await
    }
}
