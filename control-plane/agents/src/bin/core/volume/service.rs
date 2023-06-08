use crate::{
    controller::{
        registry::Registry,
        resources::{
            operations::{
                ResourceLifecycle, ResourceLifecycleWithLifetime, ResourcePublishing,
                ResourceReplicas, ResourceSharing, ResourceShutdownOperations,
                ResourceSnapshotting,
            },
            operations_helper::{OperationSequenceGuard, ResourceSpecsLocked},
            OperationGuardArc,
        },
    },
    volume::snapshot_operations::DestroyVolumeSnapshotRequest,
};
use agents::errors::SvcError;
use grpc::{
    context::Context,
    operations::{
        volume::traits::{
            CreateVolumeInfo, CreateVolumeSnapshot, CreateVolumeSnapshotInfo, DeleteVolumeSnapshot,
            DeleteVolumeSnapshotInfo, DestroyShutdownTargetsInfo, DestroyVolumeInfo,
            PublishVolumeInfo, RepublishVolumeInfo, SetVolumeReplicaInfo, ShareVolumeInfo,
            UnpublishVolumeInfo, UnshareVolumeInfo, VolumeOperations, VolumeSnapshot,
            VolumeSnapshots,
        },
        Pagination,
    },
};
use stor_port::{
    transport_api::{v0::Volumes, ReplyError},
    types::v0::{
        store::{snapshots::volume::VolumeSnapshotUserSpec, volume::VolumeSpec},
        transport::{
            CreateVolume, DestroyShutdownTargets, DestroyVolume, Filter, PublishVolume,
            RepublishVolume, SetVolumeReplica, ShareVolume, UnpublishVolume, UnshareVolume, Volume,
        },
    },
};

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
    create_volume_limiter: std::sync::Arc<tokio::sync::Semaphore>,
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

    async fn create_snapshot(
        &self,
        request: &dyn CreateVolumeSnapshotInfo,
        _ctx: Option<Context>,
    ) -> Result<VolumeSnapshot, ReplyError> {
        let service = self.clone();
        let request = request.info();
        let snapshot =
            Context::spawn(async move { service.create_snapshot(request).await }).await??;
        Ok(snapshot)
    }

    async fn delete_snapshot(
        &self,
        request: &dyn DeleteVolumeSnapshotInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let service = self.clone();
        let request = request.info();
        Context::spawn(async move { service.delete_snapshot(request).await }).await??;
        Ok(())
    }

    async fn get_snapshots(
        &self,
        filter: Filter,
        ignore_notfound: bool,
        pagination: Option<Pagination>,
        _ctx: Option<Context>,
    ) -> Result<VolumeSnapshots, ReplyError> {
        let snapshots = self
            .get_snapshots(filter, ignore_notfound, pagination)
            .await?;
        Ok(snapshots)
    }
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self {
            create_volume_limiter: std::sync::Arc::new(tokio::sync::Semaphore::new(
                registry.create_volume_limit(),
            )),
            registry,
        }
    }
    async fn create_volume_permit(&self) -> Result<tokio::sync::SemaphorePermit, SvcError> {
        tokio::time::timeout(
            // if we take too long waiting for our turn just abort..
            std::time::Duration::from_secs(10),
            self.create_volume_limiter.acquire(),
        )
        .await
        .map_err(|_| SvcError::ServiceBusy {})?
        .map_err(|_| SvcError::ServiceShutdown {})
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
                    let paginated_volumes = self.registry.paginated_volumes(p).await;
                    last_result = paginated_volumes.last();
                    paginated_volumes.result()
                }
                None => self.registry.volumes().await,
            },
            Filter::Volume(volume_id) => {
                tracing::Span::current().record("volume.uuid", volume_id.as_str());
                match self.registry.volume(&volume_id).await {
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
        let _permit = self.create_volume_permit().await?;
        OperationGuardArc::<VolumeSpec>::create(&self.registry, request).await?;
        self.registry.volume(&request.uuid).await
    }

    /// Destroy a volume using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn destroy_volume(&self, request: &DestroyVolume) -> Result<(), SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.destroy(&self.registry, request).await?;
        Ok(())
    }

    /// Destroy the shutdown targets associate with the volume.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid()))]
    pub(super) async fn destroy_shutdown_target(
        &self,
        request: &DestroyShutdownTargets,
    ) -> Result<(), SvcError> {
        let mut volume = self.specs().volume(request.uuid()).await?;
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
        self.registry.volume(&request.uuid).await
    }

    /// Set volume replica.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.uuid))]
    pub(super) async fn set_volume_replica(
        &self,
        request: &SetVolumeReplica,
    ) -> Result<Volume, SvcError> {
        let mut volume = self.specs().volume(&request.uuid).await?;
        volume.set_replica(&self.registry, request).await?;
        self.registry.volume(&request.uuid).await
    }

    /// Create a volume snapshot.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = %request.source_id, snapshot.source_uuid = %request.source_id, snapshot.uuid = %request.snap_id))]
    async fn create_snapshot(
        &self,
        request: CreateVolumeSnapshot,
    ) -> Result<VolumeSnapshot, SvcError> {
        let mut volume = self.specs().volume(&request.source_id).await?;
        let snapshot = volume
            .create_snap(
                &self.registry,
                &VolumeSnapshotUserSpec::new(volume.uuid(), request.snap_id),
            )
            .await?;
        let state = self.registry.snapshot_state(snapshot.as_ref()).await;
        Ok(VolumeSnapshot::new(snapshot.as_ref(), state))
    }

    /// Delete a volume snapshot.
    #[tracing::instrument(level = "info", skip(self), err, fields(volume.uuid = ?request.source_id, snapshot.source_uuid = ?request.source_id, snapshot.uuid = %request.snap_id))]
    async fn delete_snapshot(&self, request: DeleteVolumeSnapshot) -> Result<(), SvcError> {
        // Fetch the snapshot spec.
        let snapshot = self.specs().volume_snapshot_rsc(request.snap_id()).ok_or(
            SvcError::VolSnapshotNotFound {
                snap_id: request.snap_id().to_string(),
                source_id: request.source_id().as_ref().map(|id| id.to_string()),
            },
        )?;
        let source_id = snapshot.lock().spec().source_id().clone();

        // Fetch the volume using the snapshot source.
        let result = match request.source_id() {
            None => self.specs().volume(&source_id).await,
            Some(vol_id) => {
                if &source_id == vol_id {
                    self.specs().volume(vol_id).await
                } else {
                    // Source id did not match, different snapshot.
                    Err(SvcError::InvalidSnapshotSource {
                        snap_id: request.snap_id().to_string(),
                        invalid_source_id: vol_id.to_string(),
                        correct_source_id: source_id.to_string(),
                    })
                }
            }
        };

        // Execute the destroy.
        match result {
            Ok(mut volume) => {
                volume
                    .destroy_snap(
                        &self.registry,
                        &DestroyVolumeSnapshotRequest::new(
                            snapshot,
                            Some(volume.uuid().clone()),
                            request.snap_id,
                        ),
                    )
                    .await
            }
            Err(SvcError::VolumeNotFound { .. }) => {
                let mut snapshot_guard = snapshot.operation_guard_wait().await?;
                snapshot_guard
                    .destroy(
                        &self.registry,
                        &DestroyVolumeSnapshotRequest::new(snapshot, None, request.snap_id),
                    )
                    .await
            }
            Err(error) => Err(error),
        }?;

        Ok(())
    }

    /// Get snapshots.
    pub(super) async fn get_snapshots(
        &self,
        filter: Filter,
        ignore_notfound: bool,
        pagination: Option<Pagination>,
    ) -> Result<VolumeSnapshots, SvcError> {
        // The last result can only ever be false if using pagination.
        let mut last_result = true;
        // The filter criteria is matched to figure out whether we need to fetch a single
        // or multiple snapshots.
        let filtered_snaps = match filter {
            Filter::None => match &pagination {
                Some(p) => {
                    let paginated_snaps = self.registry.paginated_snapshots(p, None).await;
                    last_result = paginated_snaps.last();
                    paginated_snaps.result()
                }
                None => self.registry.volume_snapshots_all().await,
            },

            Filter::Volume(volume_id) => match &pagination {
                Some(p) => {
                    let paginated_snaps =
                        self.registry.paginated_snapshots(p, Some(&volume_id)).await;
                    last_result = paginated_snaps.last();
                    paginated_snaps.result()
                }
                None => self.registry.volume_snapshots(&volume_id).await,
            },

            Filter::VolumeSnapshot(volume_id, snap_id) => {
                // Get a single snapshot.
                match self.registry.snapshot(Some(&volume_id), &snap_id).await {
                    Ok(snapshot) => Ok(vec![snapshot]),
                    Err(SvcError::NotFound { .. }) if ignore_notfound => Ok(vec![]),
                    Err(error) => Err(error),
                }?
            }
            Filter::Snapshot(snap_id) => {
                // Get a single snapshot.
                match self.registry.snapshot(None, &snap_id).await {
                    Ok(snapshot) => Ok(vec![snapshot]),
                    Err(SvcError::NotFound { .. }) if ignore_notfound => Ok(vec![]),
                    Err(error) => Err(error),
                }?
            }
            filter => return Err(SvcError::InvalidFilter { filter }),
        };

        Ok(VolumeSnapshots {
            entries: filtered_snaps,
            next_token: match last_result {
                true => None,
                false => pagination.map(|p| p.starting_token() + p.max_entries()),
            },
        })
    }
}
