use crate::{
    misc::traits::ValidateRequestTypes,
    operations::volume::traits::VolumeOperations,
    volume::{
        create_volume_reply, get_volumes_reply, publish_volume_reply, set_volume_replica_reply,
        share_volume_reply, unpublish_volume_reply,
        volume_grpc_server::{VolumeGrpc, VolumeGrpcServer},
        CreateVolumeReply, CreateVolumeRequest, DestroyVolumeReply, DestroyVolumeRequest,
        GetVolumesReply, GetVolumesRequest, ProbeRequest, ProbeResponse, PublishVolumeReply,
        PublishVolumeRequest, SetVolumeReplicaReply, SetVolumeReplicaRequest, ShareVolumeReply,
        ShareVolumeRequest, UnpublishVolumeReply, UnpublishVolumeRequest, UnshareVolumeReply,
        UnshareVolumeRequest,
    },
};
use common_lib::types::v0::message_bus::Filter;
use std::{convert::TryFrom, sync::Arc};
use tonic::Response;

/// RPC Volume Server
#[derive(Clone)]
pub struct VolumeServer {
    /// Service which executes the operations.
    service: Arc<dyn VolumeOperations>,
}

impl VolumeServer {
    /// returns a new volumeserver with the service implementing Volume operations
    pub fn new(service: Arc<dyn VolumeOperations>) -> Self {
        Self { service }
    }
    /// coverts the volumeserver to its corresponding grpc server type
    pub fn into_grpc_server(self) -> VolumeGrpcServer<VolumeServer> {
        VolumeGrpcServer::new(self)
    }
}

/// Implementation of the RPC methods.
#[tonic::async_trait]
impl VolumeGrpc for VolumeServer {
    async fn create_volume(
        &self,
        request: tonic::Request<CreateVolumeRequest>,
    ) -> Result<tonic::Response<CreateVolumeReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.create(&req, None).await {
            Ok(volume) => Ok(Response::new(CreateVolumeReply {
                reply: Some(create_volume_reply::Reply::Volume(volume.into())),
            })),
            Err(err) => Ok(Response::new(CreateVolumeReply {
                reply: Some(create_volume_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn destroy_volume(
        &self,
        request: tonic::Request<DestroyVolumeRequest>,
    ) -> Result<tonic::Response<DestroyVolumeReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.destroy(&req, None).await {
            Ok(()) => Ok(Response::new(DestroyVolumeReply { error: None })),
            Err(e) => Ok(Response::new(DestroyVolumeReply {
                error: Some(e.into()),
            })),
        }
    }
    async fn get_volumes(
        &self,
        request: tonic::Request<GetVolumesRequest>,
    ) -> Result<tonic::Response<GetVolumesReply>, tonic::Status> {
        let req: GetVolumesRequest = request.into_inner();
        let filter = match req.filter {
            Some(filter) => match Filter::try_from(filter) {
                Ok(filter) => filter,
                Err(err) => {
                    return Ok(Response::new(GetVolumesReply {
                        reply: Some(get_volumes_reply::Reply::Error(err.into())),
                    }))
                }
            },
            None => Filter::None,
        };
        match self.service.get(filter, None).await {
            Ok(volumes) => Ok(Response::new(GetVolumesReply {
                reply: Some(get_volumes_reply::Reply::Volumes(volumes.into())),
            })),
            Err(err) => Ok(Response::new(GetVolumesReply {
                reply: Some(get_volumes_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn publish_volume(
        &self,
        request: tonic::Request<PublishVolumeRequest>,
    ) -> Result<tonic::Response<PublishVolumeReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.publish(&req, None).await {
            Ok(volume) => Ok(Response::new(PublishVolumeReply {
                reply: Some(publish_volume_reply::Reply::Volume(volume.into())),
            })),
            Err(err) => Ok(Response::new(PublishVolumeReply {
                reply: Some(publish_volume_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn unpublish_volume(
        &self,
        request: tonic::Request<UnpublishVolumeRequest>,
    ) -> Result<tonic::Response<UnpublishVolumeReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.unpublish(&req, None).await {
            Ok(volume) => Ok(Response::new(UnpublishVolumeReply {
                reply: Some(unpublish_volume_reply::Reply::Volume(volume.into())),
            })),
            Err(err) => Ok(Response::new(UnpublishVolumeReply {
                reply: Some(unpublish_volume_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn share_volume(
        &self,
        request: tonic::Request<ShareVolumeRequest>,
    ) -> Result<tonic::Response<ShareVolumeReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.share(&req, None).await {
            Ok(message) => Ok(Response::new(ShareVolumeReply {
                reply: Some(share_volume_reply::Reply::Response(message)),
            })),
            Err(err) => Ok(Response::new(ShareVolumeReply {
                reply: Some(share_volume_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn unshare_volume(
        &self,
        request: tonic::Request<UnshareVolumeRequest>,
    ) -> Result<tonic::Response<UnshareVolumeReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.unshare(&req, None).await {
            Ok(()) => Ok(Response::new(UnshareVolumeReply { error: None })),
            Err(e) => Ok(Response::new(UnshareVolumeReply {
                error: Some(e.into()),
            })),
        }
    }
    async fn set_volume_replica(
        &self,
        request: tonic::Request<SetVolumeReplicaRequest>,
    ) -> Result<tonic::Response<SetVolumeReplicaReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.set_volume_replica(&req, None).await {
            Ok(volume) => Ok(Response::new(SetVolumeReplicaReply {
                reply: Some(set_volume_replica_reply::Reply::Volume(volume.into())),
            })),
            Err(err) => Ok(Response::new(SetVolumeReplicaReply {
                reply: Some(set_volume_replica_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn probe(
        &self,
        _request: tonic::Request<ProbeRequest>,
    ) -> Result<tonic::Response<ProbeResponse>, tonic::Status> {
        match self.service.probe(None).await {
            Ok(resp) => Ok(Response::new(ProbeResponse { ready: resp })),
            Err(_) => Ok(Response::new(ProbeResponse { ready: false })),
        }
    }
}
