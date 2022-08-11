use crate::core::{registry::Registry, specs::ResourceSpecsLocked, wrapper::GetterOps};
use common::errors::{PoolNotFound, ReplicaNotFound, SvcError};
use common_lib::{
    transport_api::{
        v0::{Pools, Replicas},
        ReplyError,
    },
    types::v0::{
        store::{pool::PoolSpec, replica::ReplicaSpec, OperationMode},
        transport::{
            CreatePool, CreateReplica, DestroyPool, DestroyReplica, Filter, GetPools, GetReplicas,
            NodeId, Pool, PoolId, PoolRef, Replica, ReplicaId, ShareReplica, UnshareReplica,
        },
    },
};
use grpc::{
    context::Context,
    operations::{
        pool::traits::{CreatePoolInfo, DestroyPoolInfo, PoolOperations},
        replica::traits::{
            CreateReplicaInfo, DestroyReplicaInfo, ReplicaOperations, ShareReplicaInfo,
            UnshareReplicaInfo,
        },
    },
};
use parking_lot::Mutex;
use snafu::OptionExt;
use std::sync::Arc;

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
        let pool = Context::spawn(async move { service.create_pool(&req).await }).await??;
        Ok(pool)
    }

    async fn destroy(
        &self,
        pool: &dyn DestroyPoolInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let req = pool.into();
        let service = self.clone();
        Context::spawn(async move { service.destroy_pool(&req).await }).await??;
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
            Context::spawn(async move { service.create_replica(&create_replica).await }).await??;
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
        Context::spawn(async move { service.destroy_replica(&destroy_replica).await }).await??;
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
            Context::spawn(async move { service.share_replica(&share_replica).await }).await??;
        Ok(response)
    }

    async fn unshare(
        &self,
        req: &dyn UnshareReplicaInfo,
        _ctx: Option<Context>,
    ) -> Result<(), ReplyError> {
        let unshare_replica = req.into();
        let service = self.clone();
        Context::spawn(async move { service.unshare_replica(&unshare_replica).await }).await??;
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
            Filter::NodePool(node_id, pool_ref) => {
                let pool_id: PoolId = match pool_ref.clone() {
                    PoolRef::PoolName(pool_name) => pool_name,
                    PoolRef::PoolUuid(pool_uuid) => pool_uuid.to_string().into(),
                };
                tracing::Span::current().record("pool.uuid", &pool_id.as_str());
                self.node_pools(Some(node_id), Some(pool_id)).await
            }
            Filter::Pool(pool_ref) => {
                let pool_id: PoolId = match pool_ref.clone() {
                    PoolRef::PoolName(pool_name) => pool_name,
                    PoolRef::PoolUuid(pool_uuid) => pool_uuid.to_string().into(),
                };
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
            Filter::NodePool(node_id, pool_ref) => {
                let node = self.registry.get_node_wrapper(&node_id).await?;
                let pool_id = match pool_ref.clone() {
                    PoolRef::PoolName(pool_name) => pool_name,
                    PoolRef::PoolUuid(pool_uuid) => pool_uuid.to_string().into(),
                };
                let pool_wrapper = node
                    .pool_wrapper(&pool_id)
                    .await
                    .context(PoolNotFound { pool_id })?;
                Ok(pool_wrapper.replicas())
            }
            Filter::Pool(pool_ref) => {
                let pool_wrapper = self
                    .registry
                    .get_node_pool_wrapper(match pool_ref.clone() {
                        PoolRef::PoolName(pool_name) => pool_name,
                        PoolRef::PoolUuid(pool_uuid) => pool_uuid.to_string().into(),
                    })
                    .await?;
                Ok(pool_wrapper.replicas())
            }
            Filter::NodePoolReplica(node_id, pool_ref, replica_id) => {
                let node = self.registry.get_node_wrapper(&node_id).await?;
                let pool_id = match pool_ref.clone() {
                    PoolRef::PoolName(pool_name) => pool_name,
                    PoolRef::PoolUuid(pool_uuid) => pool_uuid.to_string().into(),
                };
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
            Filter::PoolReplica(pool_ref, replica_id) => {
                let pool_wrapper = self
                    .registry
                    .get_node_pool_wrapper(match pool_ref.clone() {
                        PoolRef::PoolName(pool_name) => pool_name,
                        PoolRef::PoolUuid(pool_uuid) => pool_uuid.to_string().into(),
                    })
                    .await?;
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

    /// Get the protected VolumeSpec for the given volume `id`, if any exists
    pub(crate) fn locked_pool(&self, pool: &PoolId) -> Option<Arc<Mutex<PoolSpec>>> {
        self.specs().get_locked_pool(pool)
    }
    /// Get the protected VolumeSpec for the given volume `id`, if any exists
    pub(crate) fn locked_replica(&self, replica: &ReplicaId) -> Option<Arc<Mutex<ReplicaSpec>>> {
        self.specs().get_replica(replica)
    }

    /// Create pool
    #[tracing::instrument(level = "debug", skip(self), err, fields(pool.uuid = %request.id))]
    pub(super) async fn create_pool(&self, request: &CreatePool) -> Result<Pool, SvcError> {
        self.specs()
            .create_pool(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Destroy pool
    #[tracing::instrument(level = "info", skip(self), err, fields(pool.uuid = %request.id))]
    pub(super) async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError> {
        let pool_spec = self.locked_pool(&request.id);
        let pool_spec = pool_spec.as_ref();
        self.specs()
            .destroy_pool(pool_spec, &self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Create replica
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        self.specs()
            .create_replica(&self.registry, request, OperationMode::Exclusive)
            .await
    }

    /// Destroy replica
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError> {
        let replica = self.locked_replica(&request.uuid);
        self.specs()
            .destroy_replica(
                replica.as_ref(),
                &self.registry,
                request,
                false,
                OperationMode::Exclusive,
            )
            .await
    }

    /// Share replica
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError> {
        let replica = self.locked_replica(&request.uuid);
        self.specs()
            .share_replica(
                replica.as_ref(),
                &self.registry,
                request,
                OperationMode::Exclusive,
            )
            .await
    }

    /// Unshare replica
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn unshare_replica(&self, request: &UnshareReplica) -> Result<(), SvcError> {
        let replica = self.locked_replica(&request.uuid);
        self.specs()
            .unshare_replica(
                replica.as_ref(),
                &self.registry,
                request,
                OperationMode::Exclusive,
            )
            .await?;
        Ok(())
    }
}
