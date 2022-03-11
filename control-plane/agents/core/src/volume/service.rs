use crate::core::{registry::Registry, specs::ResourceSpecsLocked};
use common::errors::SvcError;
use common_lib::{
    mbus_api::message_bus::v0::Volumes,
    types::v0::{
        message_bus::{
            CreateVolume, DestroyVolume, Filter, GetVolumes, PublishVolume, SetVolumeReplica,
            ShareVolume, UnpublishVolume, UnshareVolume, Volume,
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

    /// Get volumes
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid))]
    pub(super) async fn get_volumes(&self, request: &GetVolumes) -> Result<Volumes, SvcError> {
        // The filter criteria is matched against the volume state.
        let filtered_volumes = match &request.filter {
            Filter::None => self.registry.get_volumes().await,
            Filter::Volume(volume_id) => {
                tracing::Span::current().record("volume.uuid", &volume_id.as_str());
                vec![self.registry.get_volume(volume_id).await?]
            }
            filter => {
                return Err(SvcError::InvalidFilter {
                    filter: filter.clone(),
                })
            }
        };

        Ok(Volumes(filtered_volumes))
    }

    /// Create volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn create_volume(&self, request: &CreateVolume) -> Result<Volume, SvcError> {
        self.specs()
            .create_volume(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Destroy volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn destroy_volume(&self, request: &DestroyVolume) -> Result<(), SvcError> {
        self.specs()
            .destroy_volume(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Share volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn share_volume(&self, request: &ShareVolume) -> Result<String, SvcError> {
        self.specs()
            .share_volume(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Unshare volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn unshare_volume(&self, request: &UnshareVolume) -> Result<(), SvcError> {
        self.specs()
            .unshare_volume(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Publish volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn publish_volume(&self, request: &PublishVolume) -> Result<Volume, SvcError> {
        self.specs()
            .publish_volume(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Unpublish volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn unpublish_volume(
        &self,
        request: &UnpublishVolume,
    ) -> Result<Volume, SvcError> {
        self.specs()
            .unpublish_volume(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Set volume replica
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn set_volume_replica(
        &self,
        request: &SetVolumeReplica,
    ) -> Result<Volume, SvcError> {
        self.specs()
            .set_volume_replica(&self.registry, request, OperationMode::Exclusive)
            .await
    }
}
