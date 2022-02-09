use crate::{
    operations::pool::traits::PoolOperations,
    pool,
    pool::{
        create_pool_reply, get_pools_reply,
        pool_grpc_server::{PoolGrpc, PoolGrpcServer},
        CreatePoolReply, CreatePoolRequest, DestroyPoolReply, DestroyPoolRequest, GetPoolsReply,
        GetPoolsRequest,
    },
};
use common_lib::mbus_api::{ErrorChain, ReplyError};
use std::sync::Arc;
use tonic::{Request, Response};

/// RPC Pool Server
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
    /// coverts the poolserver to its corresponding grpc server type
    pub fn into_grpc_server(self) -> PoolGrpcServer<PoolServer> {
        PoolGrpcServer::new(self)
    }
}

// Implementation of the RPC methods.
#[tonic::async_trait]
impl PoolGrpc for PoolServer {
    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> Result<tonic::Response<DestroyPoolReply>, tonic::Status> {
        let req = request.into_inner();
        // Dispatch the destroy call to the registered service.
        let service = self.service.clone();
        tokio::spawn(async move {
            match service.destroy(&req, None).await {
                Ok(()) => Ok(Response::new(DestroyPoolReply { error: None })),
                Err(e) => Ok(Response::new(DestroyPoolReply {
                    error: Some(e.into()),
                })),
            }
        })
        .await
        .unwrap_or_else(|e| {
            Ok(Response::new(DestroyPoolReply {
                error: Some(ReplyError::tonic_reply_error(e.to_string(), e.full_string()).into()),
            }))
        })
    }

    async fn create_pool(
        &self,
        request: Request<CreatePoolRequest>,
    ) -> Result<tonic::Response<pool::CreatePoolReply>, tonic::Status> {
        let req: CreatePoolRequest = request.into_inner();
        let service = self.service.clone();
        tokio::spawn(async move {
            match service.create(&req, None).await {
                Ok(pool) => Ok(Response::new(CreatePoolReply {
                    reply: Some(create_pool_reply::Reply::Pool(pool.into())),
                })),
                Err(err) => Ok(Response::new(CreatePoolReply {
                    reply: Some(create_pool_reply::Reply::Error(err.into())),
                })),
            }
        })
        .await
        .unwrap_or_else(|e| {
            Ok(Response::new(CreatePoolReply {
                reply: Some(create_pool_reply::Reply::Error(
                    ReplyError::tonic_reply_error(e.to_string(), e.full_string()).into(),
                )),
            }))
        })
    }

    async fn get_pools(
        &self,
        request: Request<GetPoolsRequest>,
    ) -> Result<tonic::Response<pool::GetPoolsReply>, tonic::Status> {
        let req: GetPoolsRequest = request.into_inner();
        let filter = req.filter.map(Into::into).unwrap_or_default();
        let service = self.service.clone();
        tokio::spawn(async move {
            match service.get(filter, None).await {
                Ok(pools) => Ok(Response::new(GetPoolsReply {
                    reply: Some(get_pools_reply::Reply::Pools(pools.into())),
                })),
                Err(err) => Ok(Response::new(GetPoolsReply {
                    reply: Some(get_pools_reply::Reply::Error(err.into())),
                })),
            }
        })
        .await
        .unwrap_or_else(|e| {
            Ok(Response::new(GetPoolsReply {
                reply: Some(get_pools_reply::Reply::Error(
                    ReplyError::tonic_reply_error(e.to_string(), e.full_string()).into(),
                )),
            }))
        })
    }
}
