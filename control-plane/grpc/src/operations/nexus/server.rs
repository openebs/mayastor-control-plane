use crate::{
    misc::traits::ValidateRequestTypes,
    nexus::{
        add_nexus_child_reply, create_nexus_reply, get_nexuses_reply,
        nexus_grpc_server::{NexusGrpc, NexusGrpcServer},
        rebuild_history_reply, share_nexus_reply, AddNexusChildReply, AddNexusChildRequest,
        CreateNexusReply, CreateNexusRequest, DestroyNexusReply, DestroyNexusRequest,
        GetNexusesReply, GetNexusesRequest, RebuildHistoryReply, RebuildHistoryRequest,
        RemoveNexusChildReply, RemoveNexusChildRequest, ShareNexusReply, ShareNexusRequest,
        UnshareNexusReply, UnshareNexusRequest,
    },
    operations::nexus::traits::NexusOperations,
};
use std::{convert::TryFrom, sync::Arc};
use stor_port::types::v0::transport::Filter;
use tonic::Response;

/// RPC Nexus Server
#[derive(Clone)]
pub struct NexusServer {
    /// Service which executes the operations.
    service: Arc<dyn NexusOperations>,
}

impl NexusServer {
    /// returns a new nexus server with the service implementing nexus operations
    pub fn new(service: Arc<dyn NexusOperations>) -> Self {
        Self { service }
    }
    /// coverts the nexus server to its corresponding grpc server type
    pub fn into_grpc_server(self) -> NexusGrpcServer<NexusServer> {
        NexusGrpcServer::new(self)
    }
}

/// Implementation of the RPC methods.
#[tonic::async_trait]
impl NexusGrpc for NexusServer {
    async fn create_nexus(
        &self,
        request: tonic::Request<CreateNexusRequest>,
    ) -> Result<tonic::Response<CreateNexusReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.create(&req, None).await {
            Ok(nexus) => Ok(Response::new(CreateNexusReply {
                reply: Some(create_nexus_reply::Reply::Nexus(nexus.into())),
            })),
            Err(err) => Ok(Response::new(CreateNexusReply {
                reply: Some(create_nexus_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn destroy_nexus(
        &self,
        request: tonic::Request<DestroyNexusRequest>,
    ) -> Result<tonic::Response<DestroyNexusReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.destroy(&req, None).await {
            Ok(()) => Ok(Response::new(DestroyNexusReply { error: None })),
            Err(e) => Ok(Response::new(DestroyNexusReply {
                error: Some(e.into()),
            })),
        }
    }

    async fn share_nexus(
        &self,
        request: tonic::Request<ShareNexusRequest>,
    ) -> Result<tonic::Response<ShareNexusReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.share(&req, None).await {
            Ok(message) => Ok(Response::new(ShareNexusReply {
                reply: Some(share_nexus_reply::Reply::Response(message)),
            })),
            Err(err) => Ok(Response::new(ShareNexusReply {
                reply: Some(share_nexus_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn unshare_nexus(
        &self,
        request: tonic::Request<UnshareNexusRequest>,
    ) -> Result<tonic::Response<UnshareNexusReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.unshare(&req, None).await {
            Ok(()) => Ok(Response::new(UnshareNexusReply { error: None })),
            Err(e) => Ok(Response::new(UnshareNexusReply {
                error: Some(e.into()),
            })),
        }
    }

    async fn add_nexus_child(
        &self,
        request: tonic::Request<AddNexusChildRequest>,
    ) -> Result<tonic::Response<AddNexusChildReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.add_nexus_child(&req, None).await {
            Ok(child) => Ok(Response::new(AddNexusChildReply {
                reply: Some(add_nexus_child_reply::Reply::Child(child.into())),
            })),
            Err(err) => Ok(Response::new(AddNexusChildReply {
                reply: Some(add_nexus_child_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn remove_nexus_child(
        &self,
        request: tonic::Request<RemoveNexusChildRequest>,
    ) -> Result<tonic::Response<RemoveNexusChildReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.remove_nexus_child(&req, None).await {
            Ok(()) => Ok(Response::new(RemoveNexusChildReply { error: None })),
            Err(e) => Ok(Response::new(RemoveNexusChildReply {
                error: Some(e.into()),
            })),
        }
    }

    async fn get_nexuses(
        &self,
        request: tonic::Request<GetNexusesRequest>,
    ) -> Result<tonic::Response<GetNexusesReply>, tonic::Status> {
        let req: GetNexusesRequest = request.into_inner();
        let filter: Filter = match req.filter {
            Some(filter) => Filter::try_from(filter)?,
            None => Filter::None,
        };
        match self.service.get(filter, None).await {
            Ok(nexuses) => Ok(Response::new(GetNexusesReply {
                reply: Some(get_nexuses_reply::Reply::Nexuses(nexuses.into())),
            })),
            Err(err) => Ok(Response::new(GetNexusesReply {
                reply: Some(get_nexuses_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn get_rebuild_history(
        &self,
        request: tonic::Request<RebuildHistoryRequest>,
    ) -> Result<tonic::Response<RebuildHistoryReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.get_rebuild_history(&req, None).await {
            Ok(history) => Ok(Response::new(RebuildHistoryReply {
                reply: Some(rebuild_history_reply::Reply::Record(history.try_into()?)),
            })),
            Err(err) => Ok(Response::new(RebuildHistoryReply {
                reply: Some(rebuild_history_reply::Reply::Error(err.into())),
            })),
        }
    }
}
