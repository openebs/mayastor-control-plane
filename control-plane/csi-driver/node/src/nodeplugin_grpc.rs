//! The io_engine node plugin gRPC service
//! This provides access to functionality that needs to be executed on the same
//! node as a IoEngine CSI node plugin, but it is not possible to do so within
//! the CSI framework. This service must be deployed on all nodes the
//! IoEngine CSI node plugin is deployed.
use crate::{nodeplugin_svc, shutdown_event::Shutdown};
use io_engine_node_plugin::{
    io_engine_node_plugin_server::{IoEngineNodePlugin, IoEngineNodePluginServer},
    FindVolumeReply, FindVolumeRequest, FreezeFsReply, FreezeFsRequest, UnfreezeFsReply,
    UnfreezeFsRequest, VolumeType,
};
use nodeplugin_svc::{find_volume, freeze_volume, unfreeze_volume, ServiceError, TypeOfMount};
use tonic::{transport::Server, Code, Request, Response, Status};

pub mod io_engine_node_plugin {
    #![allow(clippy::derive_partial_eq_without_eq)]
    #![allow(clippy::upper_case_acronyms)]
    tonic::include_proto!("ioenginenodeplugin");
}

#[derive(Debug, Default)]
pub struct IoEngineNodePluginSvc {}

impl From<ServiceError> for Status {
    fn from(err: ServiceError) -> Self {
        match err {
            ServiceError::VolumeNotFound { .. } => Status::new(Code::NotFound, err.to_string()),
            ServiceError::FsfreezeFailed { .. } => Status::new(Code::Internal, err.to_string()),
            ServiceError::InvalidVolumeId { .. } => {
                Status::new(Code::InvalidArgument, err.to_string())
            }
            ServiceError::InternalFailure { .. } => Status::new(Code::Internal, err.to_string()),
            ServiceError::IoError { .. } => Status::new(Code::Unknown, err.to_string()),
            ServiceError::InconsistentMountFs { .. } => Status::new(Code::Unknown, err.to_string()),
            ServiceError::BlockDeviceMount { .. } => {
                Status::new(Code::FailedPrecondition, err.to_string())
            }
        }
    }
}

#[tonic::async_trait]
impl IoEngineNodePlugin for IoEngineNodePluginSvc {
    async fn freeze_fs(
        &self,
        request: Request<FreezeFsRequest>,
    ) -> Result<Response<FreezeFsReply>, Status> {
        let volume_id = request.into_inner().volume_id;
        debug!("freeze_fs({})", volume_id);
        freeze_volume(&volume_id).await?;
        Ok(Response::new(FreezeFsReply {}))
    }

    async fn unfreeze_fs(
        &self,
        request: Request<UnfreezeFsRequest>,
    ) -> Result<Response<UnfreezeFsReply>, Status> {
        let volume_id = request.into_inner().volume_id;
        debug!("unfreeze_fs({})", volume_id);
        unfreeze_volume(&volume_id).await?;
        Ok(Response::new(UnfreezeFsReply {}))
    }

    async fn find_volume(
        &self,
        request: Request<FindVolumeRequest>,
    ) -> Result<Response<FindVolumeReply>, Status> {
        let volume_id = request.into_inner().volume_id;
        debug!("find_volume({})", volume_id);
        match find_volume(&volume_id).await? {
            TypeOfMount::FileSystem => Ok(Response::new(FindVolumeReply {
                volume_type: VolumeType::Filesystem as i32,
            })),
            TypeOfMount::RawBlock => Ok(Response::new(FindVolumeReply {
                volume_type: VolumeType::Rawblock as i32,
            })),
        }
    }
}

pub struct IoEngineNodePluginGrpcServer {}

impl IoEngineNodePluginGrpcServer {
    pub async fn run(endpoint: std::net::SocketAddr) -> Result<(), ()> {
        info!(
            "IoEngine node plugin gRPC server configured at address {:?}",
            endpoint
        );
        if let Err(e) = Server::builder()
            .add_service(IoEngineNodePluginServer::new(IoEngineNodePluginSvc {}))
            .serve_with_shutdown(endpoint, Shutdown::wait())
            .await
        {
            error!("gRPC server failed with error: {}", e);
            return Err(());
        }
        Ok(())
    }
}
