use crate::controller::{
    registry::Registry,
    resources::{
        operations::{
            ResourceLifecycle, ResourcePublishing, ResourceReplicas, ResourceSharing,
            ResourceShutdownOperations,
        },
        operations_helper::ResourceSpecsLocked,
        OperationGuardArc,
    },
};
use agents::errors::SvcError;
use common_lib::{
    transport_api::{v0::Volumes, ReplyError},
    types::v0::{
        store::volume::VolumeSpec,
        transport::{
            CreateVolume, DestroyShutdownTargets, DestroyVolume, Filter, PublishVolume,
            RepublishVolume, SetVolumeReplica, ShareVolume, UnpublishVolume, UnshareVolume, Volume,
        },
    },
};
use grpc::{
    context::Context,
    operations::{
        volume::traits::{
            CreateVolumeInfo, DestroyShutdownTargetsInfo, DestroyVolumeInfo, PublishVolumeInfo,
            RepublishVolumeInfo, SetVolumeReplicaInfo, ShareVolumeInfo, UnpublishVolumeInfo,
            UnshareVolumeInfo, VolumeOperations,
        },
        Pagination,
    },
};

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
        ignore_notfound: bool,
        pagination: Option<Pagination>,
        _ctx: Option<Context>,
    ) -> Result<Volumes, ReplyError> {
        let volumes = self
            .get_volumes(filter, ignore_notfound, pagination)
            .await?;
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

    async fn republish(
        &self,
        req: &dyn RepublishVolumeInfo,
        _ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let republish_volume = req.into();
        let service = self.clone();
        let volume =
            Context::spawn(async move { service.republish_volume(&republish_volume).await })
                .await??;
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

    async fn destroy_shutdown_target(
        &self,
        req: &dyn DestroyShutdownTargetsInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let destroy_volume = req.into();
        let service = self.clone();
        Context::spawn(async move { service.destroy_shutdown_target(&destroy_volume).await })
            .await??;
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

    /// Get volumes
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid))]
    pub(super) async fn get_volumes(
        &self,
        filter: Filter,
        ignore_notfound: bool,
        pagination: Option<Pagination>,
    ) -> Result<Volumes, SvcError> {
        // The last result can only ever be false if using pagination.
        let mut last_result = true;

        // The filter criteria is matched against a volume state.
        let filtered_volumes = match filter {
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
                match self.registry.get_volume(&volume_id).await {
                    Ok(volume) => Ok(vec![volume]),
                    Err(SvcError::VolumeNotFound { .. }) if ignore_notfound => Ok(vec![]),
                    Err(error) => Err(error),
                }?
            }
            filter => return Err(SvcError::InvalidFilter { filter }),
        };

        Ok(Volumes {
            entries: filtered_volumes,
            next_token: match last_result {
                true => None,
                false => pagination.map(|p| p.starting_token() + p.max_entries()),
            },
        })
    }

    /// Create a volume using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn create_volume(&self, request: &CreateVolume) -> Result<Volume, SvcError> {
        OperationGuardArc::<VolumeSpec>::create(&self.registry, request).await?;
        self.registry.get_volume(&request.uuid).await
    }

    /// Destroy a volume using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn destroy_volume(&self, request: &DestroyVolume) -> Result<(), SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.destroy(&self.registry, request).await?;
        Ok(())
    }

    /// Destroy the shutdown targets associate with the volume.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn destroy_shutdown_target(
        &self,
        request: &DestroyShutdownTargets,
    ) -> Result<(), SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume
            .remove_shutdown_targets(&self.registry, request)
            .await
    }

    /// Share a volume using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn share_volume(&self, request: &ShareVolume) -> Result<String, SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.share(&self.registry, request).await
    }

    /// Unshare a volume using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn unshare_volume(&self, request: &UnshareVolume) -> Result<(), SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.unshare(&self.registry, request).await
    }

    /// Publish a volume using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn publish_volume(&self, request: &PublishVolume) -> Result<Volume, SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.publish(&self.registry, request).await
    }

    /// Republish a volume by shutting down the older target first.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn republish_volume(
        &self,
        request: &RepublishVolume,
    ) -> Result<Volume, SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.republish(&self.registry, request).await
    }

    /// Unpublish a volume using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn unpublish_volume(
        &self,
        request: &UnpublishVolume,
    ) -> Result<Volume, SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.unpublish(&self.registry, request).await?;
        self.registry.get_volume(&request.uuid).await
    }

    /// Set volume replica
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn set_volume_replica(
        &self,
        request: &SetVolumeReplica,
    ) -> Result<Volume, SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.set_replica(&self.registry, request).await?;
        self.registry.get_volume(&request.uuid).await
    }
}
