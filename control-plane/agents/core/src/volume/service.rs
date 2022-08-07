use crate::core::{registry::Registry, specs::ResourceSpecsLocked};
use common::{errors, errors::SvcError};
use common_lib::{
    transport_api::{v0::Volumes, ReplyError},
    types::v0::{
        store::{volume::VolumeSpec, OperationMode},
        transport::{
            CreateVolume, DestroyVolume, Filter, GetVolumes, PublishVolume, SetVolumeReplica,
            ShareVolume, UnpublishVolume, UnshareVolume, Volume, VolumeId,
        },
    },
};
use grpc::{
    context::Context,
    operations::{
        volume::traits::{
            CreateVolumeInfo, DestroyVolumeInfo, PublishVolumeInfo, SetVolumeReplicaInfo,
            ShareVolumeInfo, UnpublishVolumeInfo, UnshareVolumeInfo, VolumeOperations,
        },
        Pagination,
    },
};
use parking_lot::Mutex;
use snafu::OptionExt;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

#[tonic::async_trait]
impl VolumeOperations for Service {
    async fn create(
        &self,
        req: &dyn CreateVolumeInfo,
        _ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let create_volume = req.into();
        let service = self.clone();
        let volume =
            Context::spawn(async move { service.create_volume(&create_volume).await }).await??;
        Ok(volume)
    }

    async fn get(
        &self,
        filter: Filter,
        pagination: Option<Pagination>,
        _ctx: Option<Context>,
    ) -> Result<Volumes, ReplyError> {
        let req = GetVolumes { filter };
        let volumes = self.get_volumes(&req, pagination).await?;
        Ok(volumes)
    }

    async fn destroy(
        &self,
        req: &dyn DestroyVolumeInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let destroy_volume = req.into();
        let service = self.clone();
        Context::spawn(async move { service.destroy_volume(&destroy_volume).await }).await??;
        Ok(())
    }

    async fn share(
        &self,
        req: &dyn ShareVolumeInfo,
        _ctx: Option<Context>,
    ) -> Result<String, ReplyError> {
        let share_volume = req.into();
        let service = self.clone();
        let response =
            Context::spawn(async move { service.share_volume(&share_volume).await }).await??;
        Ok(response)
    }

    async fn unshare(
        &self,
        req: &dyn UnshareVolumeInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let unshare_volume = req.into();
        let service = self.clone();
        Context::spawn(async move { service.unshare_volume(&unshare_volume).await }).await??;
        Ok(())
    }

    async fn publish(
        &self,
        req: &dyn PublishVolumeInfo,
        _ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let publish_volume = req.into();
        let service = self.clone();
        let volume =
            Context::spawn(async move { service.publish_volume(&publish_volume).await }).await??;
        Ok(volume)
    }

    async fn unpublish(
        &self,
        req: &dyn UnpublishVolumeInfo,
        _ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let unpublish_volume = req.into();
        let service = self.clone();
        let volume =
            Context::spawn(async move { service.unpublish_volume(&unpublish_volume).await })
                .await??;
        Ok(volume)
    }

    async fn set_replica(
        &self,
        req: &dyn SetVolumeReplicaInfo,
        _ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let set_volume_replica = req.into();
        let service = self.clone();
        let volume =
            Context::spawn(async move { service.set_volume_replica(&set_volume_replica).await })
                .await??;
        Ok(volume)
    }

    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        return Ok(true);
    }
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
    pub(super) async fn get_volumes(
        &self,
        request: &GetVolumes,
        pagination: Option<Pagination>,
    ) -> Result<Volumes, SvcError> {
        // The last result can only ever be false if using pagination.
        let mut last_result = true;

        // The filter criteria is matched against the volume state.
        let filtered_volumes = match &request.filter {
            Filter::None => match &pagination {
                Some(p) => {
                    let paginated_volumes = self.registry.get_paginated_volume(p).await;
                    last_result = paginated_volumes.last();
                    paginated_volumes.result()
                }
                None => self.registry.get_volumes().await,
            },
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

        Ok(Volumes {
            entries: filtered_volumes,
            next_token: match last_result {
                true => None,
                false => pagination.map(|p| p.starting_token() + p.max_entries()),
            },
        })
    }

    /// Get the protected VolumeSpec for the given volume `id`, if any exists
    pub(crate) fn get_volume(&self, volume: &VolumeId) -> Result<Arc<Mutex<VolumeSpec>>, SvcError> {
        self.specs()
            .get_locked_volume(volume)
            .context(errors::VolumeNotFound {
                vol_id: volume.to_string(),
            })
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
        let volume = self.get_volume(&request.uuid)?;
        self.specs()
            .destroy_volume(&volume, &self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Share volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn share_volume(&self, request: &ShareVolume) -> Result<String, SvcError> {
        let volume = self.get_volume(&request.uuid)?;
        self.specs()
            .share_volume(&volume, &self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Unshare volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn unshare_volume(&self, request: &UnshareVolume) -> Result<(), SvcError> {
        let volume = self.get_volume(&request.uuid)?;
        self.specs()
            .unshare_volume(&volume, &self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Publish volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn publish_volume(&self, request: &PublishVolume) -> Result<Volume, SvcError> {
        let volume = self.get_volume(&request.uuid)?;
        self.specs()
            .publish_volume(&volume, &self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Unpublish volume
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn unpublish_volume(
        &self,
        request: &UnpublishVolume,
    ) -> Result<Volume, SvcError> {
        let volume = self.get_volume(&request.uuid)?;
        self.specs()
            .unpublish_volume(&volume, &self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Set volume replica
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn set_volume_replica(
        &self,
        request: &SetVolumeReplica,
    ) -> Result<Volume, SvcError> {
        let volume = self.get_volume(&request.uuid)?;
        self.specs()
            .set_volume_replica(&volume, &self.registry, request, OperationMode::Exclusive)
            .await
    }
}
