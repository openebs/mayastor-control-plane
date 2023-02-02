use common_lib::types::v0::transport::{CreatePool, DestroyPool, NodeId, PoolState};
use snafu::ResultExt;

use agents::{
    errors::{GrpcRequest as GrpcRequestError, SvcError},
    msg_translation::v1::{
        rpc_pool_to_agent as v1_rpc_pool_to_agent, AgentToIoEngine as v1_conversion,
    },
};
use common_lib::transport_api::ResourceKind;

use rpc::v1::pool::ListPoolOptions;

#[async_trait::async_trait]
impl crate::controller::io_engine::PoolListApi for super::RpcClient {
    async fn list_pools(&self, id: &NodeId) -> Result<Vec<PoolState>, SvcError> {
        let rpc_pools = self
            .pool()
            .list_pools(ListPoolOptions {
                name: None,
                pooltype: None,
            })
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "list_pools",
            })?;
        let rpc_pools = &rpc_pools.get_ref().pools;
        let pools = rpc_pools
            .iter()
            .map(|p| v1_rpc_pool_to_agent(p, id))
            .collect();
        Ok(pools)
    }
}

#[async_trait::async_trait]
impl crate::controller::io_engine::PoolApi for super::RpcClient {
    async fn create_pool(&self, request: &CreatePool) -> Result<PoolState, SvcError> {
        let rpc_pool = self
            .pool()
            .create_pool(v1_conversion::to_rpc(request))
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "create_pool",
            })?;
        let pool = v1_rpc_pool_to_agent(&rpc_pool.into_inner(), &request.node);
        Ok(pool)
    }

    async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError> {
        let _ = self
            .pool()
            .destroy_pool(v1_conversion::to_rpc(request))
            .await
            .context(GrpcRequestError {
                resource: ResourceKind::Pool,
                request: "destroy_pool",
            })?;
        Ok(())
    }
}
