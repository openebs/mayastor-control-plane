use crate::controller::{
    registry::Registry,
    resources::{
        operations::{ResourceLabel, ResourceLifecycle, ResourceResize, ResourceSharing},
        operations_helper::{OperationSequenceGuard, ResourceSpecsLocked},
        OperationGuardArc, ResourceMutex,
    },
    wrapper::GetterOps,
};
use agents::errors::{PoolNotFound, ReplicaNotFound, SvcError};
use grpc::{
    context::Context,
    operations::{
        pool::traits::{
            CreatePoolInfo, DestroyPoolInfo, LabelPoolInfo, PoolOperations, UnlabelPoolInfo,
        },
        replica::traits::{
            CreateReplicaInfo, DestroyReplicaInfo, ReplicaOperations, ResizeReplicaInfo,
            ShareReplicaInfo, UnshareReplicaInfo,
        },
    },
};
use stor_port::{
    transport_api::{
        v0::{Pools, Replicas},
        ReplyError, ResourceKind,
    },
    types::v0::{
        store::{pool::PoolSpec, replica::ReplicaSpec},
        transport::{
            CreatePool, CreateReplica, DestroyPool, DestroyReplica, Filter, GetPools, GetReplicas,
            LabelPool, NodeId, Pool, PoolId, Replica, ResizeReplica, ShareReplica, UnlabelPool,
            UnshareReplica, VolumeId,
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

    async fn label(
        &self,
        pool: &dyn LabelPoolInfo,
        _ctx: Option<Context>,
    ) -> Result<Pool, ReplyError> {
        let req = pool.into();
        let service = self.clone();
        let pool = Context::spawn(async move { service.label(&req).await }).await??;
        Ok(pool)
    }

    /// Remove the specified label key from the pool.
    async fn unlabel(
        &self,
        pool: &dyn UnlabelPoolInfo,
        _ctx: Option<Context>,
    ) -> Result<Pool, ReplyError> {
        let req: UnlabelPool = pool.into();
        let service = self.clone();
        if req.label_key().is_empty() {
            return Err(SvcError::InvalidLabel {
                labels: req.label_key(),
                resource_kind: ResourceKind::Pool,
            }
            .into());
        }
        let pool = Context::spawn(async move { service.unlabel(&req).await }).await??;
        Ok(pool)
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

    async fn resize(
        &self,
        req: &dyn ResizeReplicaInfo,
        _ctx: Option<Context>,
    ) -> Result<Replica, ReplyError> {
        let resize_replica = req.into();
        let service = self.clone();
        let replica =
            Context::spawn(async move { service.resize_replica(&resize_replica).await }).await??;
        Ok(replica)
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
    #[tracing::instrument(level = "info", skip(self), err, fields(pool.id))]
    pub(super) async fn get_pools(&self, request: &GetPools) -> Result<Pools, SvcError> {
        let filter = request.filter.clone();
        match filter {
            Filter::None => self.node_pools(None, None).await,
            Filter::Node(node_id) => self.node_pools(Some(node_id), None).await,
            Filter::NodePool(node_id, pool_id) => {
                tracing::Span::current().record("pool.id", pool_id.as_str());
                self.node_pools(Some(node_id), Some(pool_id)).await
            }
            Filter::Pool(pool_id) => {
                tracing::Span::current().record("pool.id", pool_id.as_str());
                self.node_pools(None, Some(pool_id)).await
            }
            Filter::Volume(volume_id) => self.volume_pools(Some(volume_id)).await,
            _ => Err(SvcError::InvalidFilter { filter }),
        }
    }

    /// Get pools associated with the volume.
    async fn volume_pools(&self, volume_id: Option<VolumeId>) -> Result<Pools, SvcError> {
        let pools = match volume_id {
            Some(vid) => self.registry.get_volume_opts_pools(vid).await?,
            None => self.registry.get_node_opt_pools(None).await?,
        };
        Ok(Pools(pools))
    }

    /// Get pools from nodes.
    async fn node_pools(
        &self,
        node_id: Option<NodeId>,
        pool_id: Option<PoolId>,
    ) -> Result<Pools, SvcError> {
        let pools = match pool_id {
            Some(id) if node_id.is_none() => {
                vec![self.registry.ctrl_pool(&id).await?]
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
            Filter::None => Ok(self.registry.replicas().await),
            Filter::Node(node_id) => self.registry.node_replicas(&node_id).await,
            Filter::NodePool(node_id, pool_id) => {
                let node = self.registry.node_wrapper(&node_id).await?;
                let pool_wrapper = node
                    .pool_wrapper(&pool_id)
                    .await
                    .context(PoolNotFound { pool_id })?;
                Ok(pool_wrapper.replicas().clone())
            }
            Filter::Pool(pool_id) => {
                let pool_wrapper = self.registry.pool_wrapper(&pool_id).await?;
                Ok(pool_wrapper.replicas().clone())
            }
            Filter::NodePoolReplica(node_id, pool_id, replica_id) => {
                let node = self.registry.node_wrapper(&node_id).await?;
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
                let node = self.registry.node_wrapper(&node_id).await?;
                let replica = node
                    .replica(&replica_id)
                    .await
                    .context(ReplicaNotFound { replica_id })?;
                Ok(vec![replica])
            }
            Filter::PoolReplica(pool_id, replica_id) => {
                let pool_wrapper = self.registry.pool_wrapper(&pool_id).await?;
                let replica = pool_wrapper
                    .replica(&replica_id)
                    .context(ReplicaNotFound { replica_id })?;
                Ok(vec![replica.clone()])
            }
            Filter::Replica(replica_id) => {
                let replica = self.registry.replica(&replica_id).await?;
                Ok(vec![replica])
            }
            Filter::Volume(volume_id) => {
                let volume = self.registry.volume_state(&volume_id).await?;
                let replicas = self.registry.replicas().await.into_iter();
                let replicas = replicas
                    .filter(|r| {
                        if let Some(spec) = self.specs().replica_rsc(&r.uuid) {
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

    /// Get the resourced PoolSpec for the given pool `id`, if any exists.
    pub(crate) fn locked_pool(&self, pool: &PoolId) -> Option<ResourceMutex<PoolSpec>> {
        self.specs().pool_rsc(pool)
    }
    /// Get the guarded PoolSpec for the given pool `id`, if any exists.
    pub(crate) async fn pool_opt(
        &self,
        pool: &PoolId,
    ) -> Result<Option<OperationGuardArc<PoolSpec>>, SvcError> {
        Ok(match self.locked_pool(pool) {
            None => None,
            Some(pool) => Some(pool.operation_guard_wait().await?),
        })
    }

    /// Create a pool using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(pool.id = %request.id))]
    pub(super) async fn create_pool(&self, request: &CreatePool) -> Result<Pool, SvcError> {
        OperationGuardArc::<PoolSpec>::create(&self.registry, request).await
    }

    /// Destroy a pool using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(pool.id = %request.id))]
    pub(super) async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError> {
        let mut pool = self.pool_opt(&request.id).await?;
        pool.destroy(&self.registry, request).await
    }

    /// Create a replica using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        OperationGuardArc::<ReplicaSpec>::create(&self.registry, request).await
    }

    /// Destroy a replica using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError> {
        let mut replica = self.specs().replica_opt(&request.uuid).await?;
        replica.as_mut().destroy(&self.registry, request).await
    }

    /// Share a replica using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError> {
        let mut replica = self.specs().replica_opt(&request.uuid).await?;
        replica.as_mut().share(&self.registry, request).await
    }

    /// Unshare a replica using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn unshare_replica(&self, request: &UnshareReplica) -> Result<(), SvcError> {
        let mut replica = self.specs().replica_opt(&request.uuid).await?;
        replica.as_mut().unshare(&self.registry, request).await?;
        Ok(())
    }

    /// Resize a replica using the given parameters.
    #[tracing::instrument(level = "info", skip(self), err, fields(replica.uuid = %request.uuid))]
    pub(super) async fn resize_replica(
        &self,
        request: &ResizeReplica,
    ) -> Result<Replica, SvcError> {
        let mut replica = self.specs().replica(&request.uuid).await?;
        replica.resize(&self.registry, request).await
    }

    /// Label the specified pool.
    #[tracing::instrument(level = "info", skip(self), err, fields(pool.id = %request.pool_id))]
    async fn label(&self, request: &LabelPool) -> Result<Pool, SvcError> {
        let mut guarded_pool = self.specs().guarded_pool(&request.pool_id).await?;
        let spec = guarded_pool
            .label(&self.registry, request.labels(), request.overwrite())
            .await?;
        let state = self.registry.ctrl_pool_state(&request.pool_id).await.ok();
        Ok(Pool::new(spec, state))
    }

    /// Remove the specified label from  the specified pool.
    #[tracing::instrument(level = "info", skip(self), err, fields(pool.id = %request.pool_id))]
    async fn unlabel(&self, request: &UnlabelPool) -> Result<Pool, SvcError> {
        let mut guarded_pool = self.specs().guarded_pool(&request.pool_id).await?;
        let spec = guarded_pool
            .unlabel(&self.registry, request.label_key())
            .await?;
        let state = self.registry.ctrl_pool_state(&request.pool_id).await.ok();
        Ok(Pool::new(spec, state))
    }
}
