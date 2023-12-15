use crate::{
    common::{SnapshotFilter, VolumeFilter, VolumeSnapshotFilter},
    context::{Client, Context, TracedChannel},
    operations::{
        volume::{
            traits::{
                CreateSnapshotVolumeInfo, CreateVolumeInfo, CreateVolumeSnapshotInfo,
                DestroyShutdownTargetsInfo, DestroyVolumeInfo, PublishVolumeInfo,
                RepublishVolumeInfo, ResizeVolumeInfo, SetVolumeReplicaInfo, ShareVolumeInfo,
                UnpublishVolumeInfo, UnshareVolumeInfo, VolumeOperations, VolumeSnapshot,
                VolumeSnapshots,
            },
            traits_snapshots::DestroyVolumeSnapshotInfo,
        },
        Pagination,
    },
    volume::{
        create_snapshot_reply, create_snapshot_volume_reply, create_volume_reply,
        get_snapshots_reply, get_snapshots_request, get_volumes_reply, get_volumes_request,
        publish_volume_reply, republish_volume_reply, set_volume_replica_reply, share_volume_reply,
        unpublish_volume_reply, volume_grpc_client::VolumeGrpcClient, GetSnapshotsRequest,
        GetVolumesRequest, ProbeRequest,
    },
};

use stor_port::{
    transport_api::{v0::Volumes, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::transport::{Filter, MessageIdVs, Volume},
};

use std::{convert::TryFrom, ops::Deref};
use tonic::transport::Uri;

/// RPC Volume Client
#[derive(Clone)]
pub struct VolumeClient {
    inner: Client<VolumeGrpcClient<TracedChannel>>,
}

impl VolumeClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, VolumeGrpcClient::new).await;
        Self { inner: client }
    }
}

impl Deref for VolumeClient {
    type Target = Client<VolumeGrpcClient<TracedChannel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Implement volume operations supported by the Volume RPC client.
/// This converts the client side data into a RPC request.
#[tonic::async_trait]
impl VolumeOperations for VolumeClient {
    #[tracing::instrument(name = "VolumeClient::create", level = "debug", skip(self), err)]
    async fn create(
        &self,
        request: &dyn CreateVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::CreateVolume);
        let response = self.client().create_volume(req).await?.into_inner();
        match response.reply {
            Some(create_volume_reply) => match create_volume_reply {
                create_volume_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                create_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    #[tracing::instrument(name = "VolumeClient::get", level = "debug", skip(self), err)]
    async fn get(
        &self,
        filter: Filter,
        ignore_notfound: bool,
        pagination: Option<Pagination>,
        ctx: Option<Context>,
    ) -> Result<Volumes, ReplyError> {
        let req: GetVolumesRequest = match filter {
            Filter::Volume(volume_id) => GetVolumesRequest {
                filter: Some(get_volumes_request::Filter::Volume(VolumeFilter {
                    volume_id: volume_id.to_string(),
                })),
                pagination: pagination.map(|p| p.into()),
                ignore_notfound,
            },
            _ => GetVolumesRequest {
                filter: None,
                pagination: pagination.map(|p| p.into()),
                ignore_notfound,
            },
        };
        let req = self.request(req, ctx, MessageIdVs::GetVolumes);
        let response = self.client().get_volumes(req).await?.into_inner();
        match response.reply {
            Some(get_volumes_reply) => match get_volumes_reply {
                get_volumes_reply::Reply::Volumes(volumes) => Ok(Volumes::try_from(volumes)?),
                get_volumes_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    #[tracing::instrument(name = "VolumeClient::destroy", level = "debug", skip(self), err)]
    async fn destroy(
        &self,
        request: &dyn DestroyVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::DestroyVolume);
        let response = self.client().destroy_volume(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(name = "VolumeClient::share", level = "debug", skip(self), err)]
    async fn share(
        &self,
        request: &dyn ShareVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<String, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::ShareVolume);
        let response = self.client().share_volume(req).await?.into_inner();
        match response.reply {
            Some(share_volume_reply) => match share_volume_reply {
                share_volume_reply::Reply::Response(message) => Ok(message),
                share_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    #[tracing::instrument(name = "VolumeClient::unshare", level = "debug", skip(self), err)]
    async fn unshare(
        &self,
        request: &dyn UnshareVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::UnshareVolume);
        let response = self.client().unshare_volume(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(name = "VolumeClient::publish", level = "debug", skip(self), err)]
    async fn publish(
        &self,
        request: &dyn PublishVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::PublishVolume);
        let response = self.client().publish_volume(req).await?.into_inner();
        match response.reply {
            Some(publish_volume_reply) => match publish_volume_reply {
                publish_volume_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                publish_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    #[tracing::instrument(name = "VolumeClient::republish", level = "debug", skip(self), err)]
    async fn republish(
        &self,
        request: &dyn RepublishVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::RepublishVolume);
        let response = self.client().republish_volume(req).await?.into_inner();
        match response.reply {
            Some(republish_volume_reply) => match republish_volume_reply {
                republish_volume_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                republish_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    #[tracing::instrument(name = "VolumeClient::unpublish", level = "debug", skip(self), err)]
    async fn unpublish(
        &self,
        request: &dyn UnpublishVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::UnpublishVolume);
        let response = self.client().unpublish_volume(req).await?.into_inner();
        match response.reply {
            Some(unpublish_volume_reply) => match unpublish_volume_reply {
                unpublish_volume_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                unpublish_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    #[tracing::instrument(name = "VolumeClient::set_replica", level = "debug", skip(self), err)]
    async fn set_replica(
        &self,
        request: &dyn SetVolumeReplicaInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::SetVolumeReplica);
        let response = self.client().set_volume_replica(req).await?.into_inner();
        match response.reply {
            Some(set_volume_replica_reply) => match set_volume_replica_reply {
                set_volume_replica_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                set_volume_replica_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    #[tracing::instrument(name = "VolumeClient::probe", level = "debug", skip(self))]
    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        match self.client().probe(ProbeRequest {}).await {
            Ok(resp) => Ok(resp.into_inner().ready),
            Err(e) => Err(e.into()),
        }
    }

    async fn destroy_shutdown_target(
        &self,
        request: &dyn DestroyShutdownTargetsInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::DestroyNexus);
        let response = self
            .client()
            .destroy_shutdown_target(req)
            .await?
            .into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(name = "VolumeClient::create_snapshot", level = "debug", skip(self))]
    async fn create_snapshot(
        &self,
        request: &dyn CreateVolumeSnapshotInfo,
        ctx: Option<Context>,
    ) -> Result<VolumeSnapshot, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::CreateVolumeSnapshot);
        let response = self.client().create_snapshot(req).await?.into_inner();
        match response.reply {
            Some(reply) => match reply {
                create_snapshot_reply::Reply::Snapshot(snap) => Ok(VolumeSnapshot::try_from(snap)?),
                create_snapshot_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::VolumeSnapshot)),
        }
    }

    #[tracing::instrument(name = "VolumeClient::delete_snapshot", level = "debug", skip(self))]
    async fn destroy_snapshot(
        &self,
        request: &dyn DestroyVolumeSnapshotInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::DestroyVolumeSnapshot);
        let response = self.client().destroy_snapshot(req).await?.into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    #[tracing::instrument(name = "VolumeClient::get_snapshots", level = "debug", skip(self))]
    async fn get_snapshots(
        &self,
        filter: Filter,
        ignore_notfound: bool,
        pagination: Option<Pagination>,
        ctx: Option<Context>,
    ) -> Result<VolumeSnapshots, ReplyError> {
        let req: GetSnapshotsRequest = match filter {
            Filter::Volume(volume_id) => GetSnapshotsRequest {
                filter: Some(get_snapshots_request::Filter::Volume(VolumeFilter {
                    volume_id: volume_id.to_string(),
                })),
                pagination: pagination.map(|p| p.into()),
                ignore_notfound,
            },
            Filter::VolumeSnapshot(volume_id, snap_id) => GetSnapshotsRequest {
                filter: Some(get_snapshots_request::Filter::VolumeSnapshot(
                    VolumeSnapshotFilter {
                        volume_id: volume_id.to_string(),
                        snapshot_id: snap_id.to_string(),
                    },
                )),
                pagination: None,
                ignore_notfound,
            },
            Filter::Snapshot(snap_id) => GetSnapshotsRequest {
                filter: Some(get_snapshots_request::Filter::Snapshot(SnapshotFilter {
                    snapshot_id: snap_id.to_string(),
                })),
                pagination: None,
                ignore_notfound,
            },
            _ => GetSnapshotsRequest {
                filter: None,
                pagination: pagination.map(|p| p.into()),
                ignore_notfound,
            },
        };
        let req = self.request(req, ctx, MessageIdVs::GetVolumeSnapshots);
        let response = self.client().get_snapshots(req).await?.into_inner();
        match response.reply {
            Some(reply) => match reply {
                get_snapshots_reply::Reply::Response(snaps) => {
                    Ok(VolumeSnapshots::try_from(snaps)?)
                }
                get_snapshots_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::VolumeSnapshot)),
        }
    }

    #[tracing::instrument(
        name = "VolumeClient::create_snapshot_volume",
        level = "debug",
        skip(self),
        err
    )]
    async fn create_snapshot_volume(
        &self,
        request: &dyn CreateSnapshotVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(request, ctx, MessageIdVs::CreateSnapshotVolume);
        let response = self
            .client()
            .create_snapshot_volume(req)
            .await?
            .into_inner();
        match response.reply {
            Some(reply) => match reply {
                create_snapshot_volume_reply::Reply::Volume(volume) => {
                    Ok(Volume::try_from(volume)?)
                }
                create_snapshot_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    async fn resize(
        &self,
        _req: &dyn ResizeVolumeInfo,
        _ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        unimplemented!()
    }
}
