use crate::{
    misc::traits::ValidateRequestTypes,
    operations::watch::traits::WatchOperations,
    watch,
    watch::{
        get_watches_reply,
        watch_grpc_server::{WatchGrpc, WatchGrpcServer},
        GetWatchesReply, GetWatchesRequest, WatchReply,
    },
};
use std::sync::Arc;
use tonic::Response;

/// RPC Watch Server
#[derive(Clone)]
pub struct WatchServer {
    /// Service which executes the operations.
    service: Arc<dyn WatchOperations>,
}

impl WatchServer {
    /// returns a new WatchServer with the service implementing Watch operations
    pub fn new(service: Arc<dyn WatchOperations>) -> Self {
        Self { service }
    }
    /// coverts the WatchServer to its corresponding grpc server type
    pub fn into_grpc_server(self) -> WatchGrpcServer<WatchServer> {
        WatchGrpcServer::new(self)
    }
}

/// Implementation of the RPC methods.
#[tonic::async_trait]
impl WatchGrpc for WatchServer {
    async fn get_watches(
        &self,
        request: tonic::Request<GetWatchesRequest>,
    ) -> Result<tonic::Response<GetWatchesReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.get(&req, None).await {
            Ok(watches) => Ok(Response::new(GetWatchesReply {
                reply: Some(get_watches_reply::Reply::Watches(watches.into())),
            })),
            Err(err) => Ok(Response::new(GetWatchesReply {
                reply: Some(get_watches_reply::Reply::Error(err.into())),
            })),
        }
    }
    async fn delete_watch(
        &self,
        request: tonic::Request<watch::Watch>,
    ) -> Result<tonic::Response<WatchReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.destroy(&req, None).await {
            Ok(()) => Ok(Response::new(WatchReply { error: None })),
            Err(e) => Ok(Response::new(WatchReply {
                error: Some(e.into()),
            })),
        }
    }
    async fn create_watch(
        &self,
        request: tonic::Request<watch::Watch>,
    ) -> Result<tonic::Response<WatchReply>, tonic::Status> {
        let req = request.into_inner().validated()?;
        match self.service.create(&req, None).await {
            Ok(()) => Ok(Response::new(WatchReply { error: None })),
            Err(e) => Ok(Response::new(WatchReply {
                error: Some(e.into()),
            })),
        }
    }
}
