use crate::core::{registry::Registry, specs::ResourceSpecsLocked, wrapper::GetterOps};
use common::errors::{PoolNotFound, ReplicaNotFound, SvcError};
use common_lib::{
    mbus_api::{
        message_bus::v0::{Pools, Replicas},
        ReplyError,
    },
    types::v0::{
        message_bus::{
            CreatePool, CreateReplica, DestroyPool, DestroyReplica, Filter, GetPools, GetReplicas,
            NodeId, Pool, PoolId, Replica, ShareReplica, UnshareReplica,
        },
        store::OperationMode,
    },
};
use grpc::{
    grpc_opts::Context,
    operations::{
        pool::traits::{CreatePoolInfo, DestroyPoolInfo, PoolOperations},
        replica::traits::{
            CreateReplicaInfo, DestroyReplicaInfo, ReplicaOperations, ShareReplicaInfo,
            UnshareReplicaInfo,
        },
    },
};
use snafu::OptionExt;

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

#[tonic::async_trait]
impl PoolOperations for Service {
    async fn create(
        &self,
        pool: &dyn CreatePoolInfo,
        _ctx: Option<Context>,
    ) -> Result<Pool, ReplyError> {
        let req = pool.into();
        let service = self.clone();
        let pool = tokio::spawn(async move { service.create_pool(&req).await }).await??;
        Ok(pool)
    }

    async fn destroy(
        &self,
        pool: &dyn DestroyPoolInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = pool.into();
        let service = self.clone();
        tokio::spawn(async move { service.destroy_pool(&req).await }).await??;
        Ok(())
    }

    async fn get(&self, filter: Filter, _ctx: Option<Context>) -> Result<Pools, ReplyError> {
        let req = GetPools { filter };
        let pools = self.get_pools(&req).await?;
        Ok(pools)
    }
}

#[tonic::async_trait]
impl ReplicaOperations for Service {
    async fn create(
        &self,
        req: &dyn CreateReplicaInfo,
        _ctx: Option<Context>,
    ) -> Result<Replica, ReplyError> {
        let create_replica = req.into();
        let service = self.clone();
        let replica =
            tokio::spawn(async move { service.create_replica(&create_replica).await }).await??;
        Ok(replica)
    }

    async fn get(&self, filter: Filter, _ctx: Option<Context>) -> Result<Replicas, ReplyError> {
        let req = GetReplicas { filter };
        let replicas = self.get_replicas(&req).await?;
        Ok(replicas)
    }

    async fn destroy(
        &self,
        req: &dyn DestroyReplicaInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let destroy_replica = req.into();
        let service = self.clone();
        tokio::spawn(async move { service.destroy_replica(&destroy_replica).await }).await??;
        Ok(())
    }

    async fn share(
        &self,
        req: &dyn ShareReplicaInfo,
        _ctx: Option<Context>,
    ) -> Result<String, ReplyError> {
        let share_replica = req.into();
        let service = self.clone();
        let response =
            tokio::spawn(async move { service.share_replica(&share_replica).await }).await??;
        Ok(response)
    }

    async fn unshare(
        &self,
        req: &dyn UnshareReplicaInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let unshare_replica = req.into();
        let service = self.clone();
        tokio::spawn(async move { service.unshare_replica(&unshare_replica).await }).await??;
        Ok(())
    }
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self { registry }
    }
    fn specs(&self) -> &ResourceSpecsLocked {
        self.registry.specs()
    }

    /// Get pools according to the filter
    #[tracing::instrument(level = "info", skip(self), err, fields(pool.uuid))]
    pub(super) async fn get_pools(&self, request: &GetPools) -> Result<Pools, SvcError> {
        let filter = request.filter.clone();
        match filter {
            Filter::None => self.node_pools(None, None).await,
            Filter::Node(node_id) => self.node_pools(Some(node_id), None).await,
            Filter::NodePool(node_id, pool_id) => {
                tracing::Span::current().record("pool.uuid", &pool_id.as_str());
                self.node_pools(Some(node_id), Some(pool_id)).await
            }
            Filter::Pool(pool_id) => {
                tracing::Span::current().record("pool.uuid", &pool_id.as_str());
                self.node_pools(None, Some(pool_id)).await
            }
            _ => Err(SvcError::InvalidFilter { filter }),
        }
    }

    /// Get pools from nodes.
    async fn node_pools(
        &self,
        node_id: Option<NodeId>,
        pool_id: Option<PoolId>,
    ) -> Result<Pools, SvcError> {
        let pools = match pool_id {
            Some(id) if node_id.is_none() => {
                vec![self.registry.get_pool(&id).await?]
            }
            Some(id) => {
                let pools = self.registry.get_node_opt_pools(node_id).await?;
                let pools: Vec<Pool> = pools.iter().filter(|p| p.id() == &id).cloned().collect();
                if pools.is_empty() {
                    return Err(SvcError::PoolNotFound { pool_id: id });
                }
                pools
            }
            None => self.registry.get_node_opt_pools(node_id).await?,
        };
        Ok(Pools(pools))
    }

    /// Get replicas according to the filter
    #[tracing::instrument(level = "info", skip(self), err)]
    pub(super) async fn get_replicas(&self, request: &GetReplicas) -> Result<Replicas, SvcError> {
        let filter = request.filter.clone();
        match filter {
            Filter::None => Ok(self.registry.get_replicas().await),
            Filter::Node(node_id) => self.registry.get_node_replicas(&node_id).await,
            Filter::NodePool(node_id, pool_id) => {
                let node = self.registry.get_node_wrapper(&node_id).await?;
                let pool_wrapper = node
                    .pool_wrapper(&pool_id)
                    .await
                    .context(PoolNotFound { pool_id })?;
                Ok(pool_wrapper.replicas())
            }
            Filter::Pool(pool_id) => {
                let pool_wrapper = self.registry.get_node_pool_wrapper(pool_id).await?;
                Ok(pool_wrapper.replicas())
            }
            Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
                let node = self.registry.get_node_wrapper(&node_id).await?;
                let pool_wrapper = node
                    .pool_wrapper(&pool_id)
                    .await
                    .context(PoolNotFound { pool_id })?;
                let replica = pool_wrapper
                    .replica(&replica_id)
                    .context(ReplicaNotFound { replica_id })?;
                Ok(vec![replica.clone()])
            }
            Filter::NodeReplica(node_id, replica_id) => {
                let node = self.registry.get_node_wrapper(&node_id).await?;
                let replica = node
                    .replica(&replica_id)
                    .await
                    .context(ReplicaNotFound { replica_id })?;
                Ok(vec![replica])
            }
            Filter::PoolReplica(pool_id, replica_id) => {
                let pool_wrapper = self.registry.get_node_pool_wrapper(pool_id).await?;
                let replica = pool_wrapper
                    .replica(&replica_id)
                    .context(ReplicaNotFound { replica_id })?;
                Ok(vec![replica.clone()])
            }
            Filter::Replica(replica_id) => {
                let replica = self.registry.get_replica(&replica_id).await?;
                Ok(vec![replica])
            }
            Filter::Volume(volume_id) => {
                let volume = self.registry.get_volume_state(&volume_id).await?;
                let replicas = self.registry.get_replicas().await.into_iter();
                let replicas = replicas
                    .filter(|r| {
                        if let Some(spec) = self.specs().get_replica(&r.uuid) {
                            let spec = spec.lock().clone();
                            spec.owners.owned_by(&volume.uuid)
                        } else {
                            false
                        }
                    })
                    .collect();
                Ok(replicas)
            }
            _ => Err(SvcError::InvalidFilter { filter }),
        }
        .map(Replicas)
    }

    /// Create pool
    #[tracing::instrument(level = "debug", skip(self), fields(pool.uuid = %request.id))]
    pub(super) async fn create_pool(&self, request: &CreatePool) -> Result<Pool, SvcError> {
        self.specs()
            .create_pool(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Destroy pool
    #[tracing::instrument(level = "info", skip(self), err, fields(pool.uuid = %request.id))]
    pub(super) async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError> {
        self.specs()
            .destroy_pool(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Create replica
    #[tracing::instrument(level = "info", skip(self), err)]
    pub(super) async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        self.specs()
            .create_replica(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Destroy replica
    #[tracing::instrument(level = "info", skip(self), err)]
    pub(super) async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError> {
        self.specs()
            .destroy_replica(&self.registry, request, false, OperationMode::Exclusive)
            .await
    }

    /// Share replica
    #[tracing::instrument(level = "info", skip(self), err)]
    pub(super) async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError> {
        self.specs()
            .share_replica(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Unshare replica
    #[tracing::instrument(level = "info", skip(self), err)]
    pub(super) async fn unshare_replica(&self, request: &UnshareReplica) -> Result<(), SvcError> {
        self.specs()
            .unshare_replica(&self.registry, request, OperationMode::Exclusive)
            .await?;
        Ok(())
    }
}
