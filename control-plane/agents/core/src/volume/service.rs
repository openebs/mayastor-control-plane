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
        let volumes = self.registry.get_volumes().await;

        // The filter criteria is matched against the volume state.
        let filtered_volumes = match &request.filter {
            Filter::None => volumes,
            Filter::Volume(volume_id) => vec![self.registry.get_volume(volume_id).await?],
            filter => {
                return Err(SvcError::InvalidFilter {
                    filter: filter.clone(),
                })
            }
        };

        Ok(Volumes(filtered_volumes))
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
    pub(super) async fn publish_volume(&self, request: &PublishVolume) -> Result<Volume, SvcError> {
        self.registry
            .specs
            .publish_volume(&self.registry, request)
            .await
    }

    /// Unpublish volume
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unpublish_volume(
        &self,
        request: &UnpublishVolume,
    ) -> Result<Volume, SvcError> {
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
