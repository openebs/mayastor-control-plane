use crate::{
    operations::{pool::traits::PoolOperations, Event},
    pool,
    pool::{
        create_pool_reply, get_pools_reply,
        pool_grpc_server::{PoolGrpc, PoolGrpcServer},
        CreatePoolReply, CreatePoolRequest, DestroyPoolReply, DestroyPoolRequest, GetPoolsReply,
        GetPoolsRequest,
    },
};
use std::sync::Arc;
use tonic::{Request, Response};

/// gRPC Pool Server
#[derive(Clone)]
pub struct PoolServer {
    /// Service which executes the operations.
    service: Arc<dyn PoolOperations>,
}

impl PoolServer {
    /// returns a new poolserver with the service implementing pool operations
    pub fn new(service: Arc<dyn PoolOperations>) -> Self {
        Self { service }
    }
    /// converts the poolserver to its corresponding grpc server type
    pub fn into_grpc_server(self) -> PoolGrpcServer<Self> {
        PoolGrpcServer::new(self)
    }
}

#[tonic::async_trait]
impl PoolGrpc for PoolServer {
    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<tonic::Response<pool::CreatePoolReply>, tonic::Status> {
        let req: CreatePoolRequest = request.into_inner();
        match self.service.create(&req, None).await {
            Ok(pool) => {
                let _ = pool.event().generate();
                Ok(Response::new(CreatePoolReply {
                    reply: Some(create_pool_reply::Reply::Pool(pool.into())),
                }))
            }
            Err(err) => Ok(Response::new(CreatePoolReply {
                reply: Some(create_pool_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> Result<tonic::Response<DestroyPoolReply>, tonic::Status> {
        let req = request.into_inner();
        match self.service.destroy(&req, None).await {
            Ok(()) => {
                let _ = req.event().generate();
                Ok(Response::new(DestroyPoolReply { error: None }))
            }
            Err(e) => Ok(Response::new(DestroyPoolReply {
                error: Some(e.into()),
            })),
        }
    }

    async fn get_pools(
        &self,
        request: Request<GetPoolsRequest>,
    ) -> Result<tonic::Response<pool::GetPoolsReply>, tonic::Status> {
        let req: GetPoolsRequest = request.into_inner();
        let filter = req.filter.map(Into::into).unwrap_or_default();
        match self.service.get(filter, None).await {
            Ok(pools) => Ok(Response::new(GetPoolsReply {
                reply: Some(get_pools_reply::Reply::Pools(pools.into())),
            })),
            Err(err) => Ok(Response::new(GetPoolsReply {
                reply: Some(get_pools_reply::Reply::Error(err.into())),
            })),
        }
    }
}
