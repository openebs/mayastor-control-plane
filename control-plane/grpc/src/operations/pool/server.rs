use crate::{
    misc::traits::ValidateRequestTypes,
    operations::pool::traits::PoolOperations,
    pool::{
        self, clear_errors_reply, cordon_pool_reply, create_pool_reply, expand_pool_reply,
        get_pools_reply, label_pool_reply,
        pool_grpc_server::{PoolGrpc, PoolGrpcServer},
        unlabel_pool_reply, ClearErrorsReply, ClearErrorsRequest, CordonPoolReply,
        CordonPoolRequest, CreatePoolReply, CreatePoolRequest, DestroyPoolReply,
        DestroyPoolRequest, ExpandPoolReply, ExpandPoolRequest, GetPoolsReply, GetPoolsRequest,
        LabelPoolReply, LabelPoolRequest, UnlabelPoolReply, UnlabelPoolRequest,
    },
};
use std::sync::Arc;
use stor_port::types::v0::transport::Filter;
use tonic::{Request, Response, Status};

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
        let req = request.into_inner().validated()?;
        match self.service.create(&req, None).await {
            Ok(pool) => Ok(Response::new(CreatePoolReply {
                reply: Some(create_pool_reply::Reply::Pool(pool.into())),
                pool_diag: None,
            })),
            Err(err) => Ok(Response::new(CreatePoolReply {
                reply: Some(create_pool_reply::Reply::Error(err.error.into())),
                pool_diag: err.diag.map(Into::into),
            })),
        }
    }

    async fn destroy_pool(
        &self,
        request: Request<DestroyPoolRequest>,
    ) -> Result<tonic::Response<DestroyPoolReply>, tonic::Status> {
        let req = request.into_inner();
        match self.service.destroy(&req, None).await {
            Ok(Some(result)) => Ok(Response::new(DestroyPoolReply {
                reply: Some(pool::destroy_pool_reply::Reply::Result(result.into())),
            })),
            Ok(None) => Ok(Response::new(DestroyPoolReply { reply: None })),
            Err(e) => Ok(Response::new(DestroyPoolReply {
                reply: Some(pool::destroy_pool_reply::Reply::Error(e.into())),
            })),
        }
    }

    async fn get_pools(
        &self,
        request: Request<GetPoolsRequest>,
    ) -> Result<tonic::Response<pool::GetPoolsReply>, tonic::Status> {
        let req: GetPoolsRequest = request.into_inner();

        let filter = match req.filter {
            Some(filter) => match Filter::try_from(filter) {
                Ok(filter) => filter,
                Err(err) => {
                    return Ok(Response::new(GetPoolsReply {
                        reply: Some(get_pools_reply::Reply::Error(err.into())),
                    }))
                }
            },
            None => Filter::None,
        };

        match self.service.get(filter, None).await {
            Ok(pools) => Ok(Response::new(GetPoolsReply {
                reply: Some(get_pools_reply::Reply::Pools(pools.into())),
            })),
            Err(err) => Ok(Response::new(GetPoolsReply {
                reply: Some(get_pools_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn label_pool(
        &self,
        request: tonic::Request<LabelPoolRequest>,
    ) -> Result<tonic::Response<LabelPoolReply>, tonic::Status> {
        let req: LabelPoolRequest = request.into_inner();
        match self.service.label(&req, None).await {
            Ok(pool) => Ok(Response::new(LabelPoolReply {
                reply: Some(label_pool_reply::Reply::Pool(pool.into())),
            })),
            Err(err) => Ok(Response::new(LabelPoolReply {
                reply: Some(label_pool_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn unlabel_pool(
        &self,
        request: tonic::Request<UnlabelPoolRequest>,
    ) -> Result<tonic::Response<UnlabelPoolReply>, tonic::Status> {
        let req: UnlabelPoolRequest = request.into_inner();
        match self.service.unlabel(&req, None).await {
            Ok(pool) => Ok(Response::new(UnlabelPoolReply {
                reply: Some(unlabel_pool_reply::Reply::Pool(pool.into())),
            })),
            Err(err) => Ok(Response::new(UnlabelPoolReply {
                reply: Some(unlabel_pool_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn cordon_pool(
        &self,
        request: Request<CordonPoolRequest>,
    ) -> Result<Response<CordonPoolReply>, Status> {
        match self.service.cordon(request.into_inner().into()).await {
            Ok(node) => Ok(Response::new(CordonPoolReply {
                reply: Some(cordon_pool_reply::Reply::Pool(node.into())),
            })),
            Err(err) => Ok(Response::new(CordonPoolReply {
                reply: Some(cordon_pool_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn uncordon_pool(
        &self,
        request: Request<CordonPoolRequest>,
    ) -> Result<Response<CordonPoolReply>, Status> {
        match self.service.uncordon(request.into_inner().into()).await {
            Ok(node) => Ok(Response::new(CordonPoolReply {
                reply: Some(cordon_pool_reply::Reply::Pool(node.into())),
            })),
            Err(err) => Ok(Response::new(CordonPoolReply {
                reply: Some(cordon_pool_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn expand_pool(
        &self,
        request: Request<ExpandPoolRequest>,
    ) -> Result<tonic::Response<ExpandPoolReply>, tonic::Status> {
        let req = request.into_inner();
        match self.service.expand(&req).await {
            Ok(pool) => Ok(Response::new(ExpandPoolReply {
                reply: Some(expand_pool_reply::Reply::Pool(pool.into())),
            })),
            Err(err) => Ok(Response::new(ExpandPoolReply {
                reply: Some(expand_pool_reply::Reply::Error(err.into())),
            })),
        }
    }

    async fn clear_errors(
        &self,
        request: Request<ClearErrorsRequest>,
    ) -> Result<tonic::Response<ClearErrorsReply>, tonic::Status> {
        let request = request.into_inner();
        match self.service.clear_errors(&request.into()).await {
            Ok(pool) => Ok(Response::new(ClearErrorsReply {
                reply: Some(clear_errors_reply::Reply::Pool(pool.into())),
            })),
            Err(err) => Ok(Response::new(ClearErrorsReply {
                reply: Some(clear_errors_reply::Reply::Error(err.into())),
            })),
        }
    }
}
