use crate::{
    dr,
    dr::{cluster_pair_reply, cluster_pairs_reply, dr_grpc_server, volume_reply, volumes_reply},
    operations::dr::traits::{volume_id, DrOperations},
};
use std::sync::Arc;
use stor_port::{
    transport_api::ReplyError,
    types::v0::transport::{
        DestroyDrClusterPair, DisableDrVolume, DrVolume, GetDrClusterPair, GetDrVolume,
    },
};
use tonic::Response;

/// RPC Dr Server
#[derive(Clone)]
pub struct DrServer {
    /// Service which executes the operations.
    service: Arc<dyn DrOperations>,
}

impl DrServer {
    /// Returns a new Dr Server with the service implementing the dr operations.
    pub fn new(service: Arc<dyn DrOperations>) -> Self {
        Self { service }
    }
    /// Converts the Dr Server into its corresponding grpc server type.
    pub fn into_grpc_server(self) -> dr_grpc_server::DrGrpcServer<DrServer> {
        dr_grpc_server::DrGrpcServer::new(self)
    }
}

/// Implementation of the RPC methods.
/// A single-pair reply, carrying either the pair or why there is none.
///
/// Errors travel in the payload rather than as a `tonic::Status`, so the kind and resource
/// survive the hop instead of being flattened into a string.
fn pair_reply(
    result: Result<
        stor_port::types::v0::transport::DrClusterPair,
        stor_port::transport_api::ReplyError,
    >,
) -> dr::ClusterPairReply {
    dr::ClusterPairReply {
        reply: Some(match result {
            Ok(pair) => cluster_pair_reply::Reply::Pair(pair.into()),
            Err(error) => cluster_pair_reply::Reply::Error(error.into()),
        }),
    }
}

/// A single-volume reply, carrying either the entry or why there is none.
fn volume_reply(result: Result<DrVolume, ReplyError>) -> dr::VolumeReply {
    dr::VolumeReply {
        reply: Some(match result {
            Ok(volume) => volume_reply::Reply::Volume(volume.into()),
            Err(error) => volume_reply::Reply::Error(error.into()),
        }),
    }
}

#[tonic::async_trait]
impl dr_grpc_server::DrGrpc for DrServer {
    async fn probe(
        &self,
        _request: tonic::Request<dr::ProbeRequest>,
    ) -> Result<tonic::Response<dr::ProbeResponse>, tonic::Status> {
        match self.service.probe(None).await {
            Ok(ready) => Ok(Response::new(dr::ProbeResponse { ready })),
            Err(_) => Ok(Response::new(dr::ProbeResponse { ready: false })),
        }
    }

    async fn create_cluster_pair(
        &self,
        request: tonic::Request<dr::CreateClusterPairRequest>,
    ) -> Result<tonic::Response<dr::ClusterPairReply>, tonic::Status> {
        let result = match request.into_inner().try_into() {
            Ok(req) => self.service.create_pair(&req, None).await,
            Err(error) => Err(error),
        };
        Ok(Response::new(pair_reply(result)))
    }

    async fn get_cluster_pair(
        &self,
        request: tonic::Request<dr::GetClusterPairRequest>,
    ) -> Result<tonic::Response<dr::ClusterPairReply>, tonic::Status> {
        let req = GetDrClusterPair {
            id: request.into_inner().id.into(),
        };
        Ok(Response::new(pair_reply(
            self.service.get_pair(&req, None).await,
        )))
    }

    async fn list_cluster_pairs(
        &self,
        _request: tonic::Request<dr::ListClusterPairsRequest>,
    ) -> Result<tonic::Response<dr::ClusterPairsReply>, tonic::Status> {
        let reply = match self.service.list_pairs(None).await {
            Ok(pairs) => cluster_pairs_reply::Reply::Pairs(dr::ClusterPairs {
                entries: pairs.entries.into_iter().map(Into::into).collect(),
            }),
            Err(error) => cluster_pairs_reply::Reply::Error(error.into()),
        };
        Ok(Response::new(dr::ClusterPairsReply { reply: Some(reply) }))
    }

    async fn destroy_cluster_pair(
        &self,
        request: tonic::Request<dr::DestroyClusterPairRequest>,
    ) -> Result<tonic::Response<dr::DestroyClusterPairReply>, tonic::Status> {
        let req = DestroyDrClusterPair {
            id: request.into_inner().id.into(),
        };
        Ok(Response::new(dr::DestroyClusterPairReply {
            error: self
                .service
                .destroy_pair(&req, None)
                .await
                .err()
                .map(Into::into),
        }))
    }

    async fn enable_volume(
        &self,
        request: tonic::Request<dr::EnableVolumeRequest>,
    ) -> Result<tonic::Response<dr::VolumeReply>, tonic::Status> {
        let result = match request.into_inner().try_into() {
            Ok(req) => self.service.enable_volume(&req, None).await,
            Err(error) => Err(error),
        };
        Ok(Response::new(volume_reply(result)))
    }

    async fn get_volume(
        &self,
        request: tonic::Request<dr::GetVolumeRequest>,
    ) -> Result<tonic::Response<dr::VolumeReply>, tonic::Status> {
        let result = match volume_id("volume", &request.into_inner().volume) {
            Ok(volume) => self.service.get_volume(&GetDrVolume { volume }, None).await,
            Err(error) => Err(error),
        };
        Ok(Response::new(volume_reply(result)))
    }

    async fn list_volumes(
        &self,
        _request: tonic::Request<dr::ListVolumesRequest>,
    ) -> Result<tonic::Response<dr::VolumesReply>, tonic::Status> {
        let reply = match self.service.list_volumes(None).await {
            Ok(volumes) => volumes_reply::Reply::Volumes(dr::Volumes {
                entries: volumes.entries.into_iter().map(Into::into).collect(),
            }),
            Err(error) => volumes_reply::Reply::Error(error.into()),
        };
        Ok(Response::new(dr::VolumesReply { reply: Some(reply) }))
    }

    async fn disable_volume(
        &self,
        request: tonic::Request<dr::DisableVolumeRequest>,
    ) -> Result<tonic::Response<dr::DisableVolumeReply>, tonic::Status> {
        let result = match volume_id("volume", &request.into_inner().volume) {
            Ok(volume) => {
                self.service
                    .disable_volume(&DisableDrVolume { volume }, None)
                    .await
            }
            Err(error) => Err(error),
        };
        Ok(Response::new(dr::DisableVolumeReply {
            error: result.err().map(Into::into),
        }))
    }
}
