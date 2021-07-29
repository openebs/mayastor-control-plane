use crate::core::{registry::Registry, wrapper::GetterOps};
use common::errors::{PoolNotFound, ReplicaNotFound, SvcError};
use common_lib::{
    mbus_api::message_bus::v0::{Pools, Replicas},
    types::v0::message_bus::{
        CreatePool, CreateReplica, DestroyPool, DestroyReplica, Filter, GetPools, GetReplicas,
        NodeId, Pool, PoolId, Replica, ShareReplica, UnshareReplica,
    },
};
use snafu::OptionExt;

#[derive(Debug, Clone)]
pub(super) struct Service {
    registry: Registry,
}

impl Service {
    pub(super) fn new(registry: Registry) -> Self {
        Self { registry }
    }

    /// Get pools according to the filter
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn get_pools(&self, request: &GetPools) -> Result<Pools, SvcError> {
        let filter = request.filter.clone();
        match filter {
            Filter::None => self.node_pools(None, None).await,
            Filter::Node(node_id) => self.node_pools(Some(node_id), None).await,
            Filter::NodePool(node_id, pool_id) => {
                self.node_pools(Some(node_id), Some(pool_id)).await
            }
            Filter::Pool(pool_id) => self.node_pools(None, Some(pool_id)).await,
            _ => Err(SvcError::InvalidFilter { filter }),
        }
    }

    /// Get pools from nodes.
    async fn node_pools(
        &self,
        node_id: Option<NodeId>,
        pool_id: Option<PoolId>,
    ) -> Result<Pools, SvcError> {
        let pools = self.registry.get_node_opt_pools(node_id).await?;
        match pool_id {
            Some(id) => {
                let p: Vec<Pool> = pools.iter().filter(|p| p.id == id).cloned().collect();
                if p.is_empty() {
                    Err(SvcError::PoolNotFound { pool_id: id })
                } else {
                    Ok(Pools(p))
                }
            }
            None => Ok(Pools(pools)),
        }
    }

    /// Get replicas according to the filter
    #[tracing::instrument(level = "debug", err)]
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
                        if let Some(spec) = self.registry.specs.get_replica(&r.uuid) {
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
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_pool(&self, request: &CreatePool) -> Result<Pool, SvcError> {
        self.registry
            .specs
            .create_pool(&self.registry, request)
            .await
    }

    /// Destroy pool
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_pool(&self, request: &DestroyPool) -> Result<(), SvcError> {
        self.registry
            .specs
            .destroy_pool(&self.registry, request)
            .await
    }

    /// Create replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn create_replica(
        &self,
        request: &CreateReplica,
    ) -> Result<Replica, SvcError> {
        self.registry
            .specs
            .create_replica(&self.registry, request)
            .await
    }

    /// Destroy replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn destroy_replica(&self, request: &DestroyReplica) -> Result<(), SvcError> {
        self.registry
            .specs
            .destroy_replica(&self.registry, request, true)
            .await
    }

    /// Share replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn share_replica(&self, request: &ShareReplica) -> Result<String, SvcError> {
        self.registry
            .specs
            .share_replica(&self.registry, request)
            .await
    }

    /// Unshare replica
    #[tracing::instrument(level = "debug", err)]
    pub(super) async fn unshare_replica(&self, request: &UnshareReplica) -> Result<(), SvcError> {
        self.registry
            .specs
            .unshare_replica(&self.registry, request)
            .await?;
        Ok(())
    }
}
