use super::translation::{rpc_pool_to_agent, AgentToIoEngine};
use crate::controller::io_engine::{
    translation::IoEngineToAgent,
    types::{GetPoolHealthRequest, GetPoolHealthResponse, ProbePoolRequest, ProbePoolResponse},
};
use agents::errors::{GrpcRequest as GrpcRequestError, SvcError};
use rpc::v1::pool::ListPoolOptions;
use stor_port::{
    transport_api::ResourceKind,
    types::v0::transport::{CreatePool, DestroyPool, ExpandPool, ImportPool, PoolState},
};

use grpc::operations::pool::traits::ClearErrorsRequest;
use snafu::ResultExt;

#[async_trait::async_trait]
impl crate::controller::io_engine::PoolListApi for super::RpcClient {
    async fn list_pools(&self) -> Result<Vec<PoolState>, SvcError> {
        let rpc_pools = self
            .pool()
            .list_pools(ListPoolOptions::default())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "list_pools",
            })?;
        let rpc_pools = &rpc_pools.get_ref().pools;
        let pools = rpc_pools
            .iter()
            .map(|p| rpc_pool_to_agent(p, self.context.node()))
            .collect();
        Ok(pools)
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::PoolApi for super::RpcClient {
    #[tracing::instrument(name = "rpc::v1::pool::create", level = "debug", skip(self), err)]
    async fn create_pool(&self, request: &CreatePool) -> Result<PoolState, SvcError> {
        match self.pool().create_pool(request.to_rpc()).await {
            Ok(rpc_pool) => {
                let pool = rpc_pool_to_agent(&rpc_pool.into_inner(), &request.node);
                Ok(pool)
            }
            Err(error)
                if error.code() == tonic::Code::Internal
                    && error.message()
                        == format!(
                            "Failed to create a BDEV '{}'",
                            request.disks.first().cloned().unwrap_or_default()
                        ) =>
            {
                Err(SvcError::GrpcRequestError {
                    resource: ResourceKind::Pool,
                    request: "create_pool".to_string(),
                    source: tonic::Status::not_found(error.message()),
                })
            }
            Err(error) => Err(error).context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "create_pool",
            }),
        }
    }

    #[tracing::instrument(name = "rpc::v1::pool::destroy", level = "debug", skip(self), err)]
    async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError> {
        let _ = self
            .pool()
            .destroy_pool(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "destroy_pool",
            })?;
        Ok(())
    }

    #[tracing::instrument(name = "rpc::v1::pool::import", level = "debug", skip(self), err)]
    async fn import_pool(&self, request: &ImportPool) -> Result<PoolState, SvcError> {
        let rpc_pool =
            self.pool()
                .import_pool(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Pool,
                    request: "import_pool",
                })?;
        let pool = rpc_pool_to_agent(&rpc_pool.into_inner(), &request.node);
        Ok(pool)
    }

    #[tracing::instrument(name = "rpc::v1::pool::grow", level = "debug", skip(self), err)]
    async fn expand_pool(&self, request: &ExpandPool) -> Result<PoolState, SvcError> {
        match self.pool().grow_pool_v2(request.to_rpc()).await {
            Ok(rpc_pool) => Ok(rpc_pool_to_agent(
                &rpc_pool.into_inner(),
                self.context.node(),
            )),
            Err(error) if error.code() == tonic::Code::OutOfRange => {
                Err(SvcError::DiskBeyondMaxSize {
                    name: request.id.clone(),
                })
            }
            Err(error)
                if (error.code() == tonic::Code::FailedPrecondition
                    && error.metadata().contains_key("bdev_not_extended")) =>
            {
                Err(SvcError::DiskNotExtended {
                    name: request.id.clone(),
                })
            }
            Err(error)
                if (error.code() == tonic::Code::FailedPrecondition
                    && error.metadata().contains_key("bdev_rescan_failed")) =>
            {
                Err(SvcError::DiskRescanFailed {
                    name: request.id.clone(),
                })
            }
            Err(error) => Err(error).context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "expand_pool",
            }),
        }
    }

    async fn clear_errors(&self, request: &ClearErrorsRequest) -> Result<PoolState, SvcError> {
        let rpc_pool =
            self.pool()
                .clear_errors(request.to_rpc())
                .await
                .context(GrpcRequestError {
                    resource: ResourceKind::Pool,
                    request: "clear_errors",
                })?;
        let pool = rpc_pool_to_agent(&rpc_pool.into_inner(), self.context.node());
        Ok(pool)
    }

    async fn probe_pool(&self, request: &ProbePoolRequest) -> Result<ProbePoolResponse, SvcError> {
        let probed = self
            .pool()
            .probe_pool(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "probe_pool",
            })?;
        Ok(probed.into_inner().into())
    }

    async fn get_pool_health(
        &self,
        request: &GetPoolHealthRequest,
    ) -> Result<GetPoolHealthResponse, SvcError> {
        let response = self
            .pool()
            .get_pool_health(request.to_rpc())
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "get_pool_health",
            })?;
        Ok(response.into_inner().to_agent())
    }
}
