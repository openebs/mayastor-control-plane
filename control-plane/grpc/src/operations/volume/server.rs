use crate::{
    misc::traits::ValidateRequestTypes,
    operations::{volume::traits::VolumeOperations, Pagination},
    volume::{
        create_snapshot_reply, create_volume_reply, get_snapshots_reply, get_volumes_reply,
        publish_volume_reply, republish_volume_reply, set_volume_replica_reply, share_volume_reply,
        unpublish_volume_reply,
        volume_grpc_server::{VolumeGrpc, VolumeGrpcServer},
        CreateSnapshotReply, CreateSnapshotRequest, CreateVolumeReply, CreateVolumeRequest,
        DestroyShutdownTargetReply, DestroyShutdownTargetRequest, DestroySnapshotReply,
        DestroySnapshotRequest, DestroyVolumeReply, DestroyVolumeRequest, GetSnapshotsReply,
        GetSnapshotsRequest, GetVolumesReply, GetVolumesRequest, ProbeRequest, ProbeResponse,
        PublishVolumeReply, PublishVolumeRequest, RepublishVolumeReply, RepublishVolumeRequest,
        SetVolumeReplicaReply, SetVolumeReplicaRequest, ShareVolumeReply, ShareVolumeRequest,
        UnpublishVolumeReply, UnpublishVolumeRequest, UnshareVolumeReply, UnshareVolumeRequest,
    },
};
use std::{convert::TryFrom, sync::Arc};
use stor_port::types::v0::transport::Filter;
use tonic::{Request, Response, Status};

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

    async fn destroy_shutdown_target(
        &self,
        request: Request<DestroyShutdownTargetRequest>,
    ) -> Result<Response<DestroyShutdownTargetReply>, Status> {
        let req = request.into_inner().validated()?;
        match self.service.destroy_shutdown_target(&req, None).await {
            Ok(()) => Ok(Response::new(DestroyShutdownTargetReply { error: None })),
            Err(e) => Ok(Response::new(DestroyShutdownTargetReply {
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

        let pagination: Option<Pagination> = req.pagination.map(|p| p.into());
        match self
            .service
            .get(filter, req.ignore_notfound, pagination, None)
            .await
        {
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
    async fn republish_volume(
        &self,
        request: tonic::Request<RepublishVolumeRequest>,
    ) -> Result<tonic::Response<RepublishVolumeReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.republish(&req, None).await {
            Ok(volume) => Ok(Response::new(RepublishVolumeReply {
                reply: Some(republish_volume_reply::Reply::Volume(volume.into())),
            })),
            Err(err) => Ok(Response::new(RepublishVolumeReply {
                reply: Some(republish_volume_reply::Reply::Error(err.into())),
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
        match self.service.set_replica(&req, None).await {
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

    async fn create_snapshot(
        &self,
        request: tonic::Request<CreateSnapshotRequest>,
    ) -> Result<tonic::Response<CreateSnapshotReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.create_snapshot(&req, None).await {
            Ok(snapshot) => Ok(Response::new(CreateSnapshotReply {
                reply: Some(create_snapshot_reply::Reply::Snapshot(snapshot.try_into()?)),
            })),
            Err(err) => Ok(Response::new(CreateSnapshotReply {
                reply: Some(create_snapshot_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn destroy_snapshot(
        &self,
        request: tonic::Request<DestroySnapshotRequest>,
    ) -> Result<tonic::Response<DestroySnapshotReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.destroy_snapshot(&req, None).await {
            Ok(()) => Ok(Response::new(DestroySnapshotReply { error: None })),
            Err(e) => Ok(Response::new(DestroySnapshotReply {
                error: Some(e.into()),
            })),
        }
    }

    async fn get_snapshots(
        &self,
        request: tonic::Request<GetSnapshotsRequest>,
    ) -> Result<tonic::Response<GetSnapshotsReply>, tonic::Status> {
        let req: GetSnapshotsRequest = request.into_inner();
        let filter = match req.filter {
            Some(filter) => match Filter::try_from(filter) {
                Ok(filter) => filter,
                Err(err) => {
                    return Ok(Response::new(GetSnapshotsReply {
                        reply: Some(get_snapshots_reply::Reply::Error(err.into())),
                    }))
                }
            },
            None => Filter::None,
        };

        let pagination: Option<Pagination> = req.pagination.map(|p| p.into());
        match self
            .service
            .get_snapshots(filter, req.ignore_notfound, pagination, None)
            .await
        {
            Ok(snapshots) => Ok(Response::new(GetSnapshotsReply {
                reply: Some(get_snapshots_reply::Reply::Response(snapshots.try_into()?)),
            })),
            Err(error) => Ok(Response::new(GetSnapshotsReply {
                reply: Some(get_snapshots_reply::Reply::Error(error.into())),
            })),
        }
    }
}
