use crate::{
    common::VolumeFilter,
    grpc_opts::{Client, Context},
    operations::volume::traits::{
        CreateVolumeInfo, DestroyVolumeInfo, PublishVolumeInfo, SetVolumeReplicaInfo,
        ShareVolumeInfo, UnpublishVolumeInfo, UnshareVolumeInfo, VolumeOperations,
    },
    volume::{
        create_volume_reply, get_volumes_reply, get_volumes_request, publish_volume_reply,
        set_volume_replica_reply, share_volume_reply, unpublish_volume_reply,
        volume_grpc_client::VolumeGrpcClient, GetVolumesRequest, ProbeRequest,
    },
};
use common_lib::{
    mbus_api::{v0::Volumes, ReplyError, ResourceKind, TimeoutOptions},
    types::v0::message_bus::{Filter, MessageIdVs, Volume},
};
use std::{convert::TryFrom, ops::Deref};
use tonic::transport::{Channel, Uri};

/// RPC Pool Client
#[derive(Clone)]
pub struct VolumeClient {
    inner: Client<VolumeGrpcClient<Channel>>,
}

impl VolumeClient {
    /// creates a new base tonic endpoint with the timeout options and the address
    pub async fn new<O: Into<Option<TimeoutOptions>>>(addr: Uri, opts: O) -> Self {
        let client = Client::new(addr, opts, VolumeGrpcClient::new).await;
        Self { inner: client }
    }
}

impl Deref for VolumeClient {
    type Target = Client<VolumeGrpcClient<Channel>>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

/// Implement pool operations supported by the Pool RPC client.
/// This converts the client side data into a RPC request.
#[tonic::async_trait]
impl VolumeOperations for VolumeClient {
    async fn create(
        &self,
        create_volume_req: &dyn CreateVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(create_volume_req, ctx, MessageIdVs::CreateVolume);
        let response = self.client().clone().create_volume(req).await?.into_inner();
        match response.reply {
            Some(create_volume_reply) => match create_volume_reply {
                create_volume_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                create_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    async fn get(&self, filter: Filter, ctx: Option<Context>) -> Result<Volumes, ReplyError> {
        let req: GetVolumesRequest = match filter {
            Filter::Volume(volume_id) => GetVolumesRequest {
                filter: Some(get_volumes_request::Filter::Volume(VolumeFilter {
                    volume_id: volume_id.to_string(),
                })),
            },
            _ => GetVolumesRequest { filter: None },
        };
        let req = self.request(req, ctx, MessageIdVs::GetVolumes);
        let response = self.client().clone().get_volumes(req).await?.into_inner();
        match response.reply {
            Some(get_volumes_reply) => match get_volumes_reply {
                get_volumes_reply::Reply::Volumes(volumes) => Ok(Volumes::try_from(volumes)?),
                get_volumes_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    async fn destroy(
        &self,
        destroy_volume_req: &dyn DestroyVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(destroy_volume_req, ctx, MessageIdVs::DestroyVolume);
        let response = self
            .client()
            .clone()
            .destroy_volume(req)
            .await?
            .into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    async fn share(
        &self,
        share_volume_req: &dyn ShareVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<String, ReplyError> {
        let req = self.request(share_volume_req, ctx, MessageIdVs::ShareVolume);
        let response = self.client().clone().share_volume(req).await?.into_inner();
        match response.reply {
            Some(share_volume_reply) => match share_volume_reply {
                share_volume_reply::Reply::Response(message) => Ok(message),
                share_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    async fn unshare(
        &self,
        unshare_volume_req: &dyn UnshareVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = self.request(unshare_volume_req, ctx, MessageIdVs::UnshareVolume);
        let response = self
            .client()
            .clone()
            .unshare_volume(req)
            .await?
            .into_inner();
        match response.error {
            None => Ok(()),
            Some(err) => Err(err.into()),
        }
    }

    async fn publish(
        &self,
        publish_volume_req: &dyn PublishVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(publish_volume_req, ctx, MessageIdVs::PublishVolume);
        let response = self
            .client()
            .clone()
            .publish_volume(req)
            .await?
            .into_inner();
        match response.reply {
            Some(publish_volume_reply) => match publish_volume_reply {
                publish_volume_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                publish_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    async fn unpublish(
        &self,
        unpublish_volume_req: &dyn UnpublishVolumeInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(unpublish_volume_req, ctx, MessageIdVs::UnpublishVolume);
        let response = self
            .client()
            .clone()
            .unpublish_volume(req)
            .await?
            .into_inner();
        match response.reply {
            Some(unpublish_volume_reply) => match unpublish_volume_reply {
                unpublish_volume_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                unpublish_volume_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    async fn set_volume_replica(
        &self,
        set_volume_replica_req: &dyn SetVolumeReplicaInfo,
        ctx: Option<Context>,
    ) -> Result<Volume, ReplyError> {
        let req = self.request(set_volume_replica_req, ctx, MessageIdVs::SetVolumeReplica);
        let response = self
            .client()
            .clone()
            .set_volume_replica(req)
            .await?
            .into_inner();
        match response.reply {
            Some(set_volume_replica_reply) => match set_volume_replica_reply {
                set_volume_replica_reply::Reply::Volume(volume) => Ok(Volume::try_from(volume)?),
                set_volume_replica_reply::Reply::Error(err) => Err(err.into()),
            },
            None => Err(ReplyError::invalid_response(ResourceKind::Volume)),
        }
    }

    async fn probe(&self, _ctx: Option<Context>) -> Result<bool, ReplyError> {
        match self.client().clone().probe(ProbeRequest {}).await {
            Ok(resp) => Ok(resp.into_inner().ready),
            Err(e) => Err(e.into()),
        }
    }
}
